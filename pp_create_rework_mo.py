import conn_db as conn
import uuid
import com_ws as cw


def xstr(s):
    return '' if s is None else str(s).strip()


# ------------------重工工单查询成品库存-------------------------------------
def get_rework_inventory(mo_query):
    print("测试:", mo_query)
    # return False
    lot_id = mo_query.get('lot_id', '')
    location = mo_query.get('mo_location', '')

    ret_dict = {'ERR_DESC': '', 'ITEM_LIST': []}
    input = {'ITEM': {'WERKS': '1200', 'ZWAFER_LOT': lot_id, 'LGORT': location}}

    action = cw.WS().send(input, 'MM108')
    if not action['status']:
        ret_dict['ERR_DESC'] = action['desc']
        return ret_dict

    output = action['data']

    item_node = output.get('ITEM')
    if not item_node:
        ret_dict['ERR_DESC'] = f'SAP返回节点数据异常,{output}'
        return ret_dict

    if isinstance(item_node, list):
        for item in item_node:
            get_wafer_obj(item, ret_dict)
    else:
        item = item_node
        get_wafer_obj(item, ret_dict)

    return ret_dict


def get_wafer_obj(item, ret_dict):
    if not isinstance(item, dict):
        ret_dict['ERR_DESC'] = f'RETURN节点错误'
        return ret_dict

    if item.get('STATUS') == 'E':
        ret_dict['ERR_DESC'] = f'查询不到数据'
        return ret_dict

    wafer_node = item.get('ITEM_WF')
    print("1:", wafer_node)
    if not wafer_node:
        return ret_dict

    if isinstance(wafer_node, list):
        for wafer in wafer_node:
            wafer_obj = {}
            wafer_obj['SAP_BOX_ID'] = item.get('ZOUT_BOX', '')
            wafer_obj['SAP_LOT_ID'] = item.get('CHARG', '')
            wafer_obj['PRODUCT_ID'] = item.get('MATNR', '')
            wafer_obj['CUST_LOT_ID'] = wafer.get('ZWAFER_LOT', '')
            wafer_obj['CUST_WAFER_ID'] = wafer.get('ZWAFER_ID', '')
            wafer_obj['QTY'] = wafer.get('ZDIE_QTY_RM', '')
            wafer_obj['LGORT'] = wafer.get('LGORT', '')  # 库存点
            ret_dict['ITEM_LIST'].append(wafer_obj)
    else:
        wafer = wafer_node
        wafer_obj = {}
        wafer_obj['SAP_BOX_ID'] = item.get('ZOUT_BOX', '')
        wafer_obj['SAP_LOT_ID'] = item.get('CHARG', '')
        wafer_obj['PRODUCT_ID'] = item.get('MATNR', '')
        wafer_obj['CUST_LOT_ID'] = wafer.get('ZWAFER_LOT', '')
        wafer_obj['CUST_WAFER_ID'] = wafer.get('ZWAFER_ID', '')
        wafer_obj['QTY'] = wafer.get('ZDIE_QTY_RM', '')
        wafer_obj['LGORT'] = wafer.get('LGORT', '')  # 库存点
        ret_dict['ITEM_LIST'].append(wafer_obj)


# 创建特殊订单:重工
def create_rework_mo(mo_data):
    print(mo_data)
    ret_dict = {'STATUS': False, 'ERR_DESC': ''}
    header = mo_data['header']
    items = mo_data['items']
    order_type = header.get('moType', '')
    con = conn.HanaConn()

    header['upload_id'] = get_rand_id(8)

    # 插入订单头表
    sql = f'''  INSERT INTO ZM_CDM_PO_HEADER(CUST_CODE,PO_TYPE,USER_NAME,UPLOAD_ID,FLAG,UPLOAD_DATE,ID) values('{header['custCode']}','{order_type}','{header['userName']}',
    '{header['upload_id']}','1',now(),ZM_CDM_PO_HEADER_SEQ.NEXTVAL) '''
    if not con.exec_n(sql):
        ret_dict['ERR_DESC'] = '订单头表插入失败'
        return ret_dict

    # 插入订单明细表
    for item in items:
        lot_id = str(item.get('CUST_LOT_ID', ''))
        wafer_id = str(item.get('CUST_WAFER_ID', ''))
        product_id = item.get('PRODUCT_ID', '')
        if len(wafer_id) == 1:
            wafer_id = '0' + wafer_id
        wafer_dies = int(item.get('QTY', 0))

        # 相同片累加
        sql = f"select WAFER_SN from ZM_CDM_PO_ITEM where UPLOAD_ID = '{header['upload_id']}' and LOT_ID = '{lot_id}' and LOT_WAFER_ID = '{wafer_id}' "
        results = con.query(sql)
        if results:
            wafer_same_sn = xstr(results[0][0])
            sql = f"update ZM_CDM_PO_ITEM set PASSBIN_COUNT = PASSBIN_COUNT + {wafer_dies} where WAFER_SN = '{wafer_same_sn}' "
            if not con.exec_n(sql):
                ret_dict['ERR_DESC'] = '订单明细表更新失败'
                return ret_dict
            continue

        # 首次插入
        sql = f"select WAFER_SN,WAFER_TIMES,FLAG3 from ZM_CDM_PO_ITEM where LOT_ID = '{lot_id}' and LOT_WAFER_ID = '{wafer_id}' and SUBSTRING(SAP_PRODUCT_PN,LENGTH(SAP_PRODUCT_PN)-7) = '{product_id[-8:]}' ORDER BY CREATE_DATE desc"
        results = con.query(sql)
        if not results:
            ret_dict['ERR_DESC'] = '无法查询到原订单数据'
            return ret_dict

        wafer_last_sn = xstr(results[0][0])
        wafer_last_times = xstr(results[0][1])
        wafer_flag3 = xstr(results[0][1]) + '+'
        inv_remark = ""

        # 插入新记录
        sql = f'''insert into ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,PO_TYPE,PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,
        CUST_FAB_DEVICE_1,CUST_FAB_DEVICE_2,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,
        MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,
        ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,CDM_ID,CDM_ITEM,SO_ID,SO_ITEM,FLAG,
        FLAG2,FLAG3,CREATE_DATE,CREATE_BY,UPLOAD_ID,WAFER_SN,WAFER_TIMES,REMARK3)  
        select CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,'{order_type}',PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,CUST_FAB_DEVICE_1,
        CUST_FAB_DEVICE_2,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,{wafer_dies},0,MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,
        ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,
        ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,CDM_ID,CDM_ITEM,SO_ID,SO_ITEM,FLAG,'0','{wafer_flag3}',NOW(),CREATE_BY,'{header['upload_id']}',
        zm_cdm_wafer_sn_seq_new.nextval,'{wafer_last_times}+','{wafer_last_sn}'  from ZM_CDM_PO_ITEM where wafer_sn = '{wafer_last_sn}'  '''

        if not con.exec_n(sql):
            ret_dict['ERR_DESC'] = '订单明细表插入失败'
            return ret_dict

    con.db.commit()
    ret_dict['STATUS'] = True
    return ret_dict


# 创建特殊订单:散袋
def create_sandai_mo(mo_data):
    print(mo_data)
    ret_dict = {'STATUS': False, 'ERR_DESC': ''}
    header = mo_data['header']
    items = mo_data['items']
    order_type = header.get('moType', '')
    con = conn.HanaConn()

    header['upload_id'] = get_rand_id(8)

    # 插入订单头表
    sql = f'''  INSERT INTO ZM_CDM_PO_HEADER(CUST_CODE,PO_TYPE,USER_NAME,UPLOAD_ID,FLAG,UPLOAD_DATE,ID) values('{header['custCode']}','{order_type}','{header['userName']}',
    '{header['upload_id']}','1',now(),ZM_CDM_PO_HEADER_SEQ.NEXTVAL) '''
    if not con.exec_n(sql):
        ret_dict['ERR_DESC'] = '订单头表插入失败'
        return ret_dict

    # 插入订单明细表
    for item in items:
        lot_id = str(item.get('CUST_LOT_ID', ''))
        wafer_id = str(item.get('CUST_WAFER_ID', ''))
        product_id = item.get('PRODUCT_ID', '')
        if len(wafer_id) == 1:
            wafer_id = '0' + wafer_id
        wafer_dies = int(item.get('QTY', 0))

        # 相同片累加
        sql = f"select WAFER_SN from ZM_CDM_PO_ITEM where UPLOAD_ID = '{header['upload_id']}' and LOT_ID = '{lot_id}' and LOT_WAFER_ID = '{wafer_id}' "
        results = con.query(sql)
        if results:
            wafer_same_sn = xstr(results[0][0])
            sql = f"update ZM_CDM_PO_ITEM set PASSBIN_COUNT = PASSBIN_COUNT + {wafer_dies} where WAFER_SN = '{wafer_same_sn}' "
            if not con.exec_n(sql):
                ret_dict['ERR_DESC'] = '订单明细表更新失败'
                return ret_dict
            continue

        # 首次插入
        sql = f"select WAFER_SN,WAFER_TIMES,FLAG3 from ZM_CDM_PO_ITEM where LOT_ID = '{lot_id}' and LOT_WAFER_ID = '{wafer_id}' and SUBSTRING(SAP_PRODUCT_PN,LENGTH(SAP_PRODUCT_PN)-7) = '{product_id[-8:]}' ORDER BY CREATE_DATE desc"
        results = con.query(sql)
        if not results:
            ret_dict['ERR_DESC'] = '无法查询到原订单数据'
            return ret_dict

        wafer_last_sn = xstr(results[0][0])
        wafer_last_times = xstr(results[0][1])

        # 插入新记录
        sql = f'''insert into ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,PO_TYPE,PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,
        CUST_FAB_DEVICE_1,CUST_FAB_DEVICE_2,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,
        MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,
        ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,CDM_ID,CDM_ITEM,SO_ID,SO_ITEM,FLAG,
        FLAG2,FLAG3,CREATE_DATE,CREATE_BY,UPLOAD_ID,WAFER_SN,WAFER_TIMES,REMARK3)  
        select CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,'{order_type}',PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,CUST_FAB_DEVICE_1,
        CUST_FAB_DEVICE_2,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,{wafer_dies},0,MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,
        ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,
        ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,CDM_ID,CDM_ITEM,SO_ID,SO_ITEM,FLAG,'0','0',NOW(),CREATE_BY,'{header['upload_id']}',
        zm_cdm_wafer_sn_seq_new.nextval,'{wafer_last_times}+','{wafer_last_sn}'  from ZM_CDM_PO_ITEM where wafer_sn = '{wafer_last_sn}'  '''

        if not con.exec_n(sql):
            ret_dict['ERR_DESC'] = '订单明细表插入失败'
            return ret_dict

    con.db.commit()
    ret_dict['STATUS'] = True
    return ret_dict


# 获取随机id
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


if __name__ == "__main__":
    # app.run(host='0.0.0.0', debug=True, port=5078, threaded=True)
    get_rework_inventory({'lot_id': '672G26', 'mo_location': '3005'})

    