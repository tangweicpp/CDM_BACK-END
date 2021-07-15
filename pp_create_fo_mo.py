import conn_db as conn
import com_ws as cw
import uuid
import json
import time
import mm_mat_info as mmi
from flask import abort, make_response


def xstr(s):
    return '' if s is None else str(s).strip()


# 查询FO对应的硅基库存
def get_fo_inventory(mo_query):
    print(mo_query)
    ret_dict = {'ERR_DESC': '', 'ITEM_LIST': [], 'HEADER': {}}
    csp_product_id = mo_query['cspProductID'].strip()

    csp_data = get_csp_data_by_po(csp_product_id)
    if csp_data['ERR_MSG']:
        ret_dict['ERR_DESC'] = csp_data['ERR_MSG']
        return ret_dict

    ret_dict['HEADER']['PRODUCT_ID'] = csp_data['PRODUCT_ID']
    ret_dict['HEADER']['SAP_PRODUCT_ID'] = csp_data['SAP_PRODUCT_ID']
    ret_dict['HEADER']['SO_ID'] = csp_data['SO_ID']
    ret_dict['HEADER']['SO_ITEM'] = csp_data['SO_ITEM']
    ret_dict['HEADER']['SO_QTY'] = csp_data['SO_QTY']
    ret_dict['HEADER']['PO_ID'] = csp_data['PO_ID']

    so_id = ret_dict['HEADER']['SO_ID']
    product_name = ret_dict['HEADER']['PRODUCT_ID']
    if not product_name:
        ret_dict['ERR_DESC'] = 'FO成品物料号不可为空'
        return ret_dict

    # 查询出对应的硅基料号
    product_name = get_fo_si_product_name(product_name)
    if not product_name:
        ret_dict['ERR_DESC'] = '查询不到FO成品的BOM或BOM里找不到对应的硅基物料号'
        return ret_dict

    location = mo_query.get('mo_location', '')

    input = {'ITEM': {'WERKS': '1200', 'LGORT': location}}

    if product_name:
        input['ITEM']['MATNR'] = product_name

    print(json.dumps(input))

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
            get_csp_wafer_obj(item, ret_dict, so_id)
    else:
        item = item_node
        get_csp_wafer_obj(item, ret_dict, so_id)

    return ret_dict


# 查询硅基库存-DC
def get_fo_dc_inventory(mo_query):
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()
    ret_dict = {'ERR_DESC': '', 'ITEM_LIST': [], 'HEADER': {}}
    location = mo_query.get('mo_location', '')
    zwafer_lot = mo_query.get('SILotID', '')

    # sql
    sql = f"SELECT '',CHARG,MATNR,ZWAFER_LOT,ZWAFER_ID,ZGROSS_DIE_QTY,LGORT FROM ZKTMM0001 WHERE LGORT = '{location}' AND ZWAFER_LOT = '{zwafer_lot}' order by ZWAFER_ID "
    print(sql)
    results = con_dw.query(sql)
    if not results:
        ret_dict['ERR_DESC'] = "查询不到硅基库存"
        return ret_dict

    for row in results:
        wafer_obj = {}

        wafer_obj['SAP_BOX_ID'] = xstr(row[0])
        wafer_obj['SAP_LOT_ID'] = xstr(row[1])
        wafer_obj['PRODUCT_ID'] = xstr(row[2])
        wafer_obj['CUST_LOT_ID'] = xstr(row[3])
        wafer_obj['CUST_WAFER_ID'] = xstr(row[4])
        wafer_obj['QTY'] = xstr(row[5])
        wafer_obj['LGORT'] = xstr(row[6])

        # if "-" in wafer_obj['CUST_WAFER_ID']:
        #     csp_wafer_id = wafer_obj['CUST_WAFER_ID'][:-2]
        # else:
        #     csp_wafer_id = wafer_obj['CUST_WAFER_ID']

        csp_wafer_id = wafer_obj['CUST_WAFER_ID']

        # 排除已经建立的WAFER
        sql = f"select * from zm_cdm_po_item where lot_wafer_id = '{csp_wafer_id}' and flag = '1' and PASSBIN_COUNT <> 1 "

        results2 = con.query(sql)
        if not results2:
            ret_dict['ITEM_LIST'].append(wafer_obj)

    return ret_dict


def get_fo_si_product_name(product_name):
    con = conn.HanaConnDW()
    child_product_list = []
    child_flag_list = ['Z013']

    if not product_name:
        return ''

    if str(tuple(child_flag_list))[-2:-1] == ',':
        product_type = str(tuple(child_flag_list)).replace(',', '')
    else:
        product_type = str(tuple(child_flag_list))

    sql = f''' SELECT a.matnr, b.IDNRK,c.ZZPROCESS FROM VM_SAP_V_MAT a
        INNER JOIN VM_SAP_V_ITEM b on a.STLNR  = b.STLNR 
        INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = b.IDNRK 
        INNER JOIN VM_SAP_MAT_INFO d ON d.MATNR = a.MATNR 
        WHERE d.ZZCNLH = '{product_name}' AND c.MTART in {product_type} AND c.ZZPROCESS in ('TSV','TSV-FO')
    '''

    print(sql)
    results = con.query(sql)
    if not results:
        return ''

    child_product = xstr(results[0][1])
    return child_product


def get_cust_device(product_name):
    mat = mmi.get_mat_master_data(product_no=product_name)
    if not mat:
        abort(make_response({"ERR_MSG": f"查询不到该成品料号:{product_name}"}))
    
    # 保持小写机种名
    if mat[0]['ZZKHXH'] == 'WIL6000':
        mat[0]['ZZKHXH'] = 'Wil6000'


    return mat[0]['ZZKHXH'], mat[0]['ZZHTXH'], mat[0]['ZZFABXH'], mat[0]['ZZJYGD']


# 创建CSP工单的订单
def create_fo_mo(mo_data):
    print(mo_data)
    ret_dict = {'STATUS': False, 'ERR_MSG': ''}
    header = mo_data['header']
    items = mo_data['items']
    order_type = header.get('moType', '')
    po_id = header.get('cspPOID', '')
    so_id = header.get('cspSOID', '')
    so_item = header.get('cspSOItem', '')
    product_name = header.get('cspProductID', '')
    sap_product_name = header.get('cspSapProductID', '')
    cust_device, ht_device, fab_device, gross_dies = get_cust_device(
        product_name)

    con = conn.HanaConn()
    header['upload_id'] = get_rand_id(8)

    # 插入订单头表
    sql = f'''  INSERT INTO ZM_CDM_PO_HEADER(CUST_CODE,PO_TYPE,USER_NAME,UPLOAD_ID,FLAG,UPLOAD_DATE,ID) values('{header['custCode']}','{order_type}','{header['userName']}',
    '{header['upload_id']}','1',now(),ZM_CDM_PO_HEADER_SEQ.NEXTVAL) '''
    if not con.exec_n(sql):
        print('订单头表插入失败')
        con.db.rollback()
        ret_dict['ERR_MSG'] = '订单头表插入失败'
        return ret_dict

    # 插入订单明细表
    for item in items:
        wafer_id = str(item.get('CUST_WAFER_ID', ''))
        wafer_dies = int(item.get('QTY', 0))

        # 相同片累加
        sql = f"select WAFER_SN from ZM_CDM_PO_ITEM where UPLOAD_ID = '{header['upload_id']}' and LOT_WAFER_ID = '{wafer_id}' "
        results = con.query(sql)
        if results:
            wafer_same_sn = xstr(results[0][0])
            sql = f"update ZM_CDM_PO_ITEM set PASSBIN_COUNT = PASSBIN_COUNT + {wafer_dies} where WAFER_SN = '{wafer_same_sn}' "
            if not con.exec_n(sql):
                con.db.rollback()
                ret_dict['ERR_MSG'] = '订单明细表更新失败'
                print('订单明细表更新失败')
                return ret_dict
            continue

        # 首次插入
        sql = f"select WAFER_SN,WAFER_TIMES from ZM_CDM_PO_ITEM where LOT_WAFER_ID = '{wafer_id}' ORDER BY CREATE_DATE desc"
        print(sql)
        results = con.query(sql)
        if not results:
            ret_dict['ERR_MSG'] = '无法查询到原订单数据'
            print('无法查询到原订单数据')
            return ret_dict

        wafer_last_sn = xstr(results[0][0])
        wafer_last_times = xstr(results[0][1])
        # if "-" in wafer_id:
        #     csp_wafer_id = wafer_id[:-2]
        # else:
        csp_wafer_id = wafer_id

        # 插入新记录
        sql = f'''insert into ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,PO_TYPE,PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,
        CUST_FAB_DEVICE_1,CUST_FAB_DEVICE_2,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,
        MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,
        ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,CDM_ID,CDM_ITEM,SO_ID,SO_ITEM,FLAG,
        FLAG2,FLAG3,CREATE_DATE,CREATE_BY,UPLOAD_ID,WAFER_SN,WAFER_TIMES,REMARK3)  
        select CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,'{po_id}','{order_type}',PO_DATE,BONDED,'{cust_device}','{fab_device}',CUST_FAB_DEVICE_1,
        CUST_FAB_DEVICE_2,'{ht_device}','{product_name}','{sap_product_name}',LOT_ID,WAFER_ID,'{csp_wafer_id}',{gross_dies},0,'',ADD_0,ADD_1,ADD_2,ADD_3,
        ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,
        ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,'','','{so_id}','{so_item}',FLAG,'0','1',NOW(),CREATE_BY,'{header['upload_id']}',
        zm_cdm_wafer_sn_seq_new.nextval,'{wafer_last_times}+','{wafer_last_sn}'  from ZM_CDM_PO_ITEM where wafer_sn = '{wafer_last_sn}'  '''

        print(sql)
        if not con.exec_n(sql):
            con.db.rollback()
            ret_dict['ERR_MSG'] = '订单明细表插入失败'
            return ret_dict

    con.db.commit()
    ret_dict['STATUS'] = True
    return ret_dict


# 创建硅基=>DC工单的订单
def create_fo_dc_mo(mo_data):
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()
    print(mo_data)

    ret_dict = {'STATUS': False, 'ERR_MSG': ''}
    header = mo_data['header']
    items = mo_data['items']
    order_type = "YP10"

    product_name = header.get('SIDCPrdID', '')
    if product_name[-3:-2] != "W":
        ret_dict['ERR_MSG'] = f"成品料号{product_name}倒数第三位不是W=>不是DC料号"
        return ret_dict

    sql = f''' SELECT DISTINCT ZZKHXH,ZZHTXH,ZZJYGD,MATNR FROM VM_SAP_MAT_INFO WHERE ZZCNLH = '{product_name}' '''
    results = con_dw.query(sql)
    if not results:
        ret_dict['ERR_MSG'] = f"查询不到成品料号{product_name}的物料主数据"
        return ret_dict

    cust_device = xstr(results[0][0])
    ht_device = xstr(results[0][1])
    gross_dies = int(results[0][2])
    sap_product_name = xstr(results[0][3])

    header['upload_id'] = get_rand_id(8)

    # 插入订单头表
    sql = f'''  INSERT INTO ZM_CDM_PO_HEADER(CUST_CODE,PO_TYPE,USER_NAME,UPLOAD_ID,FLAG,UPLOAD_DATE,ID) values('{header['custCode']}','{order_type}','{header['userName']}',
    '{header['upload_id']}','1',now(),ZM_CDM_PO_HEADER_SEQ.NEXTVAL) '''
    if not con.exec_n(sql):
        print('订单头表插入失败')
        con.db.rollback()
        ret_dict['ERR_MSG'] = '订单头表插入失败'
        return ret_dict

    # 插入订单明细表
    for item in items:
        wafer_id = str(item.get('CUST_WAFER_ID', ''))
        wafer_dies = int(item.get('QTY', 0))

        # 相同片累加
        sql = f"select WAFER_SN from ZM_CDM_PO_ITEM where UPLOAD_ID = '{header['upload_id']}' and LOT_WAFER_ID = '{wafer_id}' "
        results = con.query(sql)
        if results:
            wafer_same_sn = xstr(results[0][0])
            sql = f"update ZM_CDM_PO_ITEM set PASSBIN_COUNT = PASSBIN_COUNT + {wafer_dies} where WAFER_SN = '{wafer_same_sn}' "
            if not con.exec_n(sql):
                con.db.rollback()
                ret_dict['ERR_MSG'] = '订单明细表更新失败'
                print('订单明细表更新失败')
                return ret_dict
            continue

        # 首次插入
        sql = f"select WAFER_SN,WAFER_TIMES from ZM_CDM_PO_ITEM where LOT_WAFER_ID = '{wafer_id}' ORDER BY CREATE_DATE desc"
        print(sql)
        results = con.query(sql)
        if not results:
            ret_dict['ERR_MSG'] = '无法查询到原订单数据'
            print('无法查询到原订单数据')
            return ret_dict

        wafer_last_sn = xstr(results[0][0])
        wafer_last_times = xstr(results[0][1])
        # if "-" in wafer_id:
        #     csp_wafer_id = wafer_id[:-2]
        # else:
        csp_wafer_id = wafer_id

        # 插入新记录
        sql = f'''insert into ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,PO_TYPE,PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,
        CUST_FAB_DEVICE_1,CUST_FAB_DEVICE_2,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,
        MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,
        ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,CDM_ID,CDM_ITEM,SO_ID,SO_ITEM,FLAG,
        FLAG2,FLAG3,CREATE_DATE,CREATE_BY,UPLOAD_ID,WAFER_SN,WAFER_TIMES,REMARK3)  
        select CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,'','{order_type}',PO_DATE,BONDED,'{cust_device}','',CUST_FAB_DEVICE_1,
        CUST_FAB_DEVICE_2,'{ht_device}','{product_name}','{sap_product_name}',LOT_ID,WAFER_ID,'{csp_wafer_id}',{gross_dies},0,'',ADD_0,ADD_1,ADD_2,ADD_3,
        ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,
        ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,'','','','',FLAG,'0','1',NOW(),CREATE_BY,'{header['upload_id']}',
        zm_cdm_wafer_sn_seq_new.nextval,'{wafer_last_times}+','{wafer_last_sn}'  from ZM_CDM_PO_ITEM where wafer_sn = '{wafer_last_sn}'  '''

        if not con.exec_n(sql):
            con.db.rollback()
            ret_dict['ERR_MSG'] = '订单明细表插入失败'
            return ret_dict

    con.db.commit()
    ret_dict['STATUS'] = True
    return ret_dict


def get_csp_wafer_obj(item, ret_dict, so_id):
    con = conn.HanaConn()
    sql = f"SELECT LOT_WAFER_ID FROM ZM_CDM_PO_ITEM zcpi WHERE SO_ID = '{so_id}'"
    results = con.query(sql)
    wafer_id_list = []
    if results:
        for rs in results:
            wafer_id_list.append(rs[0])

    if not isinstance(item, dict):
        ret_dict['ERR_DESC'] = f'RETURN节点错误'
        return ret_dict

    if item.get('STATUS') == 'E':
        ret_dict['ERR_DESC'] = f'查询不到数据'
        return ret_dict

    wafer_node = item.get('ITEM_WF')
    if not wafer_node:
        return ret_dict

    if isinstance(wafer_node, list):
        for wafer in wafer_node:
            wafer_obj = {}
            if fiter_wafer_id(wafer.get('ZWAFER_ID', '')):
                print('ceshi:1')
                continue
            print('ceshi :2')

            wafer_obj['SAP_BOX_ID'] = item.get('ZOUT_BOX', '')
            wafer_obj['SAP_LOT_ID'] = item.get('CHARG', '')
            wafer_obj['PRODUCT_ID'] = item.get('MATNR', '')
            wafer_obj['CUST_LOT_ID'] = wafer.get('ZWAFER_LOT', '')
            wafer_obj['CUST_WAFER_ID'] = wafer.get('ZWAFER_ID', '')
            wafer_obj['QTY'] = wafer.get('ZGROSS_DIE_QTY', '')
            wafer_obj['LGORT'] = wafer.get('LGORT', '')  # 库存点

            lot_wafer_id = str(wafer_obj['CUST_LOT_ID']) + \
                str(wafer_obj['CUST_WAFER_ID'])

            if not lot_wafer_id in wafer_id_list:
                ret_dict['ITEM_LIST'].append(wafer_obj)
    else:
        wafer = wafer_node
        wafer_obj = {}
        if fiter_wafer_id(wafer.get('ZWAFER_ID', '')):
            return ret_dict
        wafer_obj['SAP_BOX_ID'] = item.get('ZOUT_BOX', '')
        wafer_obj['SAP_LOT_ID'] = item.get('CHARG', '')
        wafer_obj['PRODUCT_ID'] = item.get('MATNR', '')
        wafer_obj['CUST_LOT_ID'] = wafer.get('ZWAFER_LOT', '')
        wafer_obj['CUST_WAFER_ID'] = wafer.get('ZWAFER_ID', '')
        wafer_obj['QTY'] = wafer.get('ZGROSS_DIE_QTY', '')
        wafer_obj['LGORT'] = wafer.get('LGORT', '')  # 库存点

        lot_wafer_id = str(wafer_obj['CUST_LOT_ID']) + \
            str(wafer_obj['CUST_WAFER_ID'])

        if not lot_wafer_id in wafer_id_list:
            ret_dict['ITEM_LIST'].append(wafer_obj)


# 过滤错误
def fiter_wafer_id(lot_wafer_id):
    if '-' in lot_wafer_id:
        lot_wafer_id = lot_wafer_id[:-2]

    con = conn.HanaConn()
    sql = f"select * from zm_cdm_po_item where lot_wafer_id = '{lot_wafer_id}'   AND  flag = '1' AND PASSBIN_COUNT <> 1 "
    print(sql)
    results = con.query(sql)
    if results:
        return True
    else:
        return False


# FO CSP
def get_csp_data_by_po(csp_product_id):
    res = {'ERR_MSG': '', 'PRODUCT_ID': '',
           'SAP_PRODUCT_ID': '', 'SO_ID': '', 'SO_ITEM': '', 'SO_QTY': ''}
    con = conn.HanaConn()

    # 查询
    sql = f"""SELECT b.PRD_ID,b.SAP_PRD_ID,a.SO_NO,b.SO_ITEM_SN,b.QTY,a.PO_NO FROM ZM_CDM_SO_HEADER a
        INNER JOIN ZM_CDM_SO_ITEM b ON a.SO_SN = b.SO_SN 
        WHERE b.PRD_ID = '{csp_product_id}' ORDER BY a.SO_NO desc
    """

    # # 查询对应SO的信息
    # sql = f'''SELECT b.PRD_ID,b.SAP_PRD_ID,a.SO_NO,b.SO_ITEM_SN,sum(b.QTY) FROM ZM_CDM_SO_HEADER a
    #     INNER JOIN ZM_CDM_SO_ITEM b ON a.SO_SN = b.SO_SN
    #     WHERE a.PO_NO = '{po_id}'
    #     GROUP BY b.PRD_ID,b.SAP_PRD_ID,a.SO_NO,b.SO_ITEM_SN'''

    # print(sql)
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = '查询不到该CSP成品料号的销售订单'
        return res

    # if len(results) > 1:
    #     res['ERR_MSG'] = 'CSP 成品料号的SO ITEM不可大于1'
    #     return res

    res['PRODUCT_ID'] = xstr(results[0][0])
    res['SAP_PRODUCT_ID'] = xstr(results[0][1])
    res['SO_ID'] = xstr(results[0][2])
    res['SO_ITEM'] = xstr(results[0][3])
    res['SO_QTY'] = results[0][4]
    res['PO_ID'] = xstr(results[0][5])

    # 查询当前SO剩余量
    sql = f"SELECT sum(PASSBIN_COUNT+FAILBIN_COUNT) FROM ZM_CDM_PO_ITEM WHERE SO_ID = '{res['SO_ID']}'  AND PRODUCT_PN = '{res['PRODUCT_ID']}' "
    print(sql)
    results = con.query(sql)
    if not results:
        return res

    csp_po_qty = results[0][0]
    csp_po_qty = 0 if not csp_po_qty else csp_po_qty
    res['SO_QTY'] = res['SO_QTY'] - csp_po_qty
    return res


# 获取随机id
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


# -------------------------------------------------------------新规则硅基ID-----------------------------------------------------------------

# 获取当天最大的ID
def get_SI_Lot_Seq(zzbase_str):
    con = conn.HanaConn()
    sql = f"SELECT IFNULL(MAX(ZZSEQ)+1,1)  FROM ZM_CDM_COMMON_SEQ WHERE ZZTYPE = 'SI_WAFER_ID' AND ZZBASE = '{zzbase_str}' "
    results = con.query(sql)
    if results:
        return results[0][0]
    else:
        return None


#  获取硅基ID
def get_SI_ID(lot_qty):
    res = {"ERR_MSG": "", "LOT_ITEMS": []}
    mo_date = time.strftime('%y%m%d')
    mo_year = mo_date[:2]
    mo_mon = ("123456789ABC")[int(mo_date[2:4])-1]
    mo_day = ("123456789ABCDEFGHJKLMNPQRSTUVWX")[int(mo_date[4:6])-1]
    base_str = mo_year + mo_mon + mo_day
    base_seq = get_SI_Lot_Seq(base_str)
    # print(base_str)
    for i in range(lot_qty):
        lot_obj = {}
        lot_seq_num = base_seq + i
        lot_seq_key = base_str + decToCh(lot_seq_num)
        # lot_seq_check_sum = get_sum_str(lot_seq_key + "A0")
        lot_seq_str = lot_seq_key
        # print(lot_seq_str)
        lot_obj['ZZTYPE'] = "SI_WAFER_ID"
        lot_obj['ZZBASE'] = base_str
        lot_obj['ZZKEY'] = lot_seq_str
        lot_obj['ZZSEQ'] = lot_seq_num
        res['LOT_ITEMS'].append(lot_obj)

    return res


def decToCh(znum):
    if znum < 100:
        return ("00"+str(znum))[-2:]

    znumstr = str(znum)
    return ("ABCDEFGHJKLMNPQRSTUVWXY")[int(znumstr[:2])-10] + str(znum)[-1:]


def get_sum_str(id):
    sum = 0
    # 默认把最后两码改为A0计算checksum
    defaultId = id[:-2] + "A0"

    for i in range(len(defaultId)):
        numericalValue = ord(defaultId[i]) - 32
        offset = 3 * (len(defaultId)-1-i)
        numericalValue = numericalValue << offset
        sum = sum + numericalValue

    reminder = sum % 59
    decimalise = 59-reminder
    if decimalise == 59:
        decimalise = 0

    fA = chr(ord('A') + decimalise // 8)
    fB = str(0 + decimalise % 8)
    f_ch = fA+fB
    # print(f_ch)
    return f_ch


# GD55 FO LOT DC流水
def get_GD55_Lot_dc_seq():
    con = conn.HanaConn()
    zzbase_str = ""
    sql = f"SELECT IFNULL(MAX(ZZSEQ)+1,1)  FROM ZM_CDM_COMMON_SEQ WHERE ZZTYPE = 'GD55_FO_LOT_DC_SEQ' AND ZZBASE = '{zzbase_str}' "
    results = con.query(sql)
    if results:
        return results[0][0]
    else:
        return None


if __name__ == "__main__":
    data = {'header': {'moType': 'YP13', 'custCode': '', 'productName': '', 'sapProductName': '', 'cspSapProductID': '', 'userName': '07885', 'type': '1', 'qty': '', 'lotID': '', 'moLocation': '3007', 'grossDies': '', 'sapCustCode': '', 'custDevice': '', 'htDevice': '', 'cspProductID': '', 'SILotID': '213N01',
                       'SIDCPrdID': '19YBJ17808CWFO1', 'cspSOQty': 0, 'cspSelectedQty': 8144, 'cspSOID': '', 'cspSOItem': '', 'repReason': '', 'moType2': ''}, 'items': [{'CUST_LOT_ID': '213N01', 'CUST_WAFER_ID': '213N01-10C4', 'LGORT': '3007', 'PRODUCT_ID': '000000000030101300', 'QTY': '8144', 'SAP_BOX_ID': '', 'SAP_LOT_ID': '2002333627'}]}
    create_fo_dc_mo(data)
