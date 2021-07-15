import json
import conn_db as conn
import uuid
import datetime
import com_ws as cw


def xstr(s):
    return '' if s is None else str(s).strip()


# 创建AA的销售订单
def create_aa_so(po_data):
    res = save_po_data_76(po_data, 0)
    return res


def get_curr_date(flag):
    return datetime.datetime.now().strftime('%Y%m%d') if flag == 1 else datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


def post_map_request(lot_id):
    sql = f"insert into erptemp.dbo.AA_BC_HISTORY(LOTID,flag,上传日期) values('{lot_id}','0',getdate()) "
    conn.MssConn().exec_c(sql)


def save_po_data_76(po_data, map_flag):
    res = {'ERR_MSG': '', 'RES_DATA': []}
    con = conn.HanaConn()
    header = po_data.get('header')
    items = po_data.get('items')
    upload_id = get_rand_id(8)

    # 保存PO
    sql = f'''INSERT into ZM_CDM_PO_HEADER(BONDED_TYPE,CUST_CODE,DELAY_DAYS,FILE_NAME,FILE_PATH,
        MAIL_TIP,OFFER_SHEET,PO_LEVEL,PO_TYPE,TRAD_CUST_CODE,USER_NAME,UPLOAD_ID,FLAG,UPLOAD_DATE,ID)
        values('{header.get('bonded_type','')}','{header.get('cust_code','')}','{header.get('delay_days','')}',
        '{header.get('file_name','')}','{header.get('file_path','')}','{header.get('mail_tip','')}',
        '{header.get('offer_sheet','')}','{header.get('po_level','')}','{header.get('po_type','')}',
        '{header.get('cust_code','')}','{header.get('user_name','')}',
        '{upload_id}','1',now(),ZM_CDM_PO_HEADER_SEQ.NEXTVAL) '''

    if not con.exec_n(sql):
        res['ERR_MSG'] = '订单头表(ZM_CDM_PO_HEADER)保存错误'
        return res

    for item in items:
        res_obj = {}
        item['wafer_sn'] = get_rand_id(8)

        # BC
        if item.get('lot_id'):
            res_obj['LOT_ID'] = item['lot_id']
            item['lot_id'] = item['lot_id'][2:]
            item['add_8'] = 'BC'
            item['wafer_qty'] = item.get('add_0', '')

            # 判断是否存在上传记录
            sql = f"SELECT * FROM ZM_CDM_PO_ITEM zcpi WHERE LOT_ID = '{item['lot_id']}' AND FLAG = '1' AND ADD_8 = 'BC' "
            print(sql)
            results = con.query(sql)
            if results:
                res['ERR_MSG'] = f"这笔LOT：{item['lot_id']}已存在,不可再次上传!"
                return res

            if map_flag:
                # 插入虚拟wafer信息
                insert_wafer_data(item.get('fab_device', ''),
                                  item['lot_id'], item['wafer_qty'], upload_id)
            else:
                post_map_request(item['lot_id'])

        # FEDS
        if item.get('customer_device') and (not item.get('po_id')):
            item['add_8'] = 'FEDS'
            res_obj['CUST_DEVICE'] = item['customer_device']

            # 之前的成品机种对应预测失效
            sql = f"UPDATE ZM_CDM_PO_ITEM  SET FLAG = '0' WHERE CUSTOMER_DEVICE = '{item['customer_device']}'  and ADD_8 = 'FEDS' "
            print(sql)
            if not con.exec_n(sql):
                res['ERR_MSG'] = f"客户机种:{item['customer_device']}FEDS更新失败,请联系IT!"
                return res

        # PO
        if item.get('po_id'):
            item['add_8'] = 'PO'
            if not item.get('customer_device'):
                res['ERR_MSG'] = f"PO的客户机种不可为空"
                return res

        else:
            header['po_type'] = ''

        if item.get('product_pn'):
            item['product_pn'] = item['product_pn'].split('||')[0]

        # 插入订单记录
        sql = f'''INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,PO_TYPE,PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,CUST_FAB_DEVICE_1,HT_PN,PRODUCT_PN,
            SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,
            ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,
            FLAG,FLAG2,FLAG3,CREATE_DATE,CREATE_BY,WAFER_TIMES,UPLOAD_ID,WAFER_SN,WAFER_HOLD)
             values('{header.get('cust_code','')}','{item.get('sap_cust_code','200118')}','{header.get('cust_code','')}','{item.get('po_id','')}','{header.get('po_type','')}',
            '{item.get('po_date','')}','{item.get('bonded','')}','{item.get('customer_device','')}','{item.get('fab_device','')}','{item.get('process','')}','{item.get('ht_pn','')}','{item.get('product_pn','')}',
            '{item.get('sap_product_pn')}','{item.get('lot_id','')}','{item.get('wafer_id','')}','{item.get('lot_wafer_id','')}',
            '{item.get('mark_code','')}', '{item.get('add_0','')}','{item.get('add_1','')}','{item.get('add_2','')}','{item.get('add_3','')}',
            '{item.get('add_4','')}','{item.get('add_5','')}','{item.get('add_6','')}','{item.get('add_7','')}','{item.get('add_8','')}','{item.get('add_9','')}',
            '{item.get('add_10','')}','{item.get('add_11','')}','{item.get('add_12','')}','{item.get('add_13','')}','{item.get('add_14','')}','{item.get('add_15','')}',
            '{item.get('add_16','')}','{item.get('add_17','')}','{item.get('add_18','')}','{item.get('add_19','')}','{item.get('add_20','')}','{item.get('add_21','')}',
            '{item.get('add_22','')}','{item.get('add_23','')}','{item.get('add_24','')}','{item.get('add_25','')}','{item.get('add_26','')}','{item.get('add_27','')}',
            '{item.get('add_28','')}','{item.get('add_29','')}','{item.get('add_30','')}','1','0','0',now(),'{header.get('user_name','')}','{item.get('upload_times','')}',
            '{upload_id}','{item.get('wafer_sn','')}','{item.get('hold_flag','')}')  '''

        if not con.exec_n(sql):
            res['ERR_MSG'] = '订单ITEM表(ZM_CDM_PO_ITEM)保存错误'
            return res

        # 需要创建大PO
        customer_device = item.get('customer_device', '')
        passbin_count = 9999999
        po_id = item.get('po_id', '')
        if customer_device and passbin_count and po_id:
            sales_orders = get_sales_orders(
                con, upload_id, passbin_count, header)

            if sales_orders:
                if not create_sales_orders(con, sales_orders, res):
                    return res
        else:
            res_obj['MESSAGE'] = "上传成功"
            res['RES_DATA'].append(res_obj)

    con.db.commit()
    return res


# 查询FAB机种组名
def get_wafer_gross_die(fab_device):
    con = conn.HanaConnDW()
    sql = f"SELECT DISTINCT KEY1 FROM ZM_CONFIG_TYPE_LIST WHERE CONFIG_TYPE = '2' AND KEY2 = '{fab_device}' "
    results = con.query(sql)
    fab_device_g = xstr(results[0][0]).upper(
    ) if results else fab_device.upper()

    sql = f"SELECT DISTINCT ZZJYGD FROM MARA m WHERE ZZFABXH = '{fab_device_g}' "
    results = con.query(sql)
    if results:
        gross_die = int(xstr(results[0][0]))
    else:
        gross_die = 0

    return gross_die


# 76没有map,插入wafer数据
def insert_wafer_data(fab_device, lot_id, wafer_qty, upload_id):
    con = conn.HanaConn()
    gross_die = get_wafer_gross_die(fab_device)
    if wafer_qty:
        wafer_qty = int(wafer_qty)
        wafer_id_flag = 1 if wafer_qty == 25 else 0
        for i in range(wafer_qty):
            wafer_id = ('000' + str(i+1))[-2:]
            lot_wafer_id = lot_id + wafer_id
            wafer_sn = get_rand_id(8)

            sql = f''' INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE,CUSTOMER_DEVICE,LOT_ID,WAFER_ID,LOT_WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,ADD_8,FLAG,FLAG2,FLAG3,CREATE_DATE,CREATE_BY,UPLOAD_ID,WAFER_SN,ID)  
                VALUES('US010','','{lot_id}','{wafer_id}','{lot_wafer_id}',{gross_die},0,'MAP','1','0','0',NOW(),'SYSTEM', '{upload_id}', '{wafer_sn}', {wafer_id_flag})
            '''
            con.exec_c(sql)


def get_sales_orders(con, upload_id, po_qty, header):
    # 查询是否有开立条件
    sales_orders = {'SO_DATA': []}
    sql = f"SELECT DISTINCT PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID ='{upload_id}' "
    results1 = con.query(sql)
    if not results1:
        return None

    for row1 in results1:
        sales_order = {'HEADER': {}, 'ITEM': []}

        # SO_HEADER
        sales_order['HEADER']['ACTION'] = 'N'            # 动作标识(默认新增)
        sales_order['HEADER']['AUART'] = xstr(row1[0])   # 订单类型
        sales_order['HEADER']['KUNNR'] = xstr(row1[1])   # 真实客户
        sales_order['HEADER']['KUNRE'] = sales_order['HEADER']['KUNNR']  # 交易客户
        sales_order['HEADER']['BSTKD'] = xstr(row1[3])   # 客户PO
        sales_order['HEADER']['HEADER_ATTR'] = [
            {'NAME': 'HEADER_1', 'VALUE': "DEFAULT"}]    # HEADER属性

        sql = f"SELECT SO_SN FROM ZM_CDM_SO_HEADER WHERE PO_NO='{sales_order['HEADER']['BSTKD']}' AND PO_TYPE='{sales_order['HEADER']['AUART']}' AND SO_NO IS NOT NULL "
        results = con.query(sql)
        if results:
            sales_order['HEADER']['ACTION'] = 'C'
            sales_order['HEADER']['HEAD_NO'] = results[0][0]
        else:
            sales_order['HEADER']['HEAD_NO'] = get_rand_id(8)
            sql_n = f'''INSERT INTO ZM_CDM_SO_HEADER(PO_NO,PO_TYPE,SO_SN,SO_CREATE_BY,SO_CREATE_DATE,CUST_CODE,FLAG,PO_UPLOAD_ID)
                        values('{sales_order['HEADER']['BSTKD']}','{sales_order['HEADER']['AUART']}','{sales_order['HEADER']['HEAD_NO']}',
                        '{header['user_name']}',NOW(),'US010','1','{upload_id}') '''
            if not con.exec_n(sql_n):
                return None

        sql2 = f'''SELECT SAP_PRODUCT_PN,sum(PASSBIN_COUNT+FAILBIN_COUNT),CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN ,count(1)
        FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID  = '{upload_id}' AND PO_ID = '{sales_order['HEADER']['BSTKD']}'
        GROUP BY SAP_PRODUCT_PN,CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN '''

        results2 = con.query(sql2)
        for row2 in results2:
            product_item = {}
            product_item['ACTION'] = 'N'                    # 动作标识
            product_item['ITEM_NO'] = get_rand_id(6)        # CRM行号
            product_item['BSTDK'] = get_curr_date(1)        # PO date
            product_item['BNAME'] = '12345'  # CDM创建人帐号
            product_item['MATNR'] = xstr(row2[0])           # 物料号
            product_item['KWMENG'] = po_qty         # 数量
            product_item['ZCUST_DEVICE'] = xstr(row2[2])    # 客户机种
            product_item['ZFAB_DEVICE'] = xstr(row2[3])     # FAB机种
            product_item['INCO1'] = ''                      # 贸易条款1
            product_item['INCO2'] = ''                      # 贸易条款2

            sql2_n = f'''INSERT INTO ZM_CDM_SO_ITEM(SO_SN,CDM_ITEM_SN,PRD_ID,QTY,CREATE_BY,CREATE_DATE,FLAG,SAP_PRD_ID,REMARK1)
            values('{sales_order['HEADER']['HEAD_NO']}','{product_item['ITEM_NO']}','{row2[4] }','{product_item['KWMENG']}',
            '{product_item['BNAME']}',now(),'0','{product_item['MATNR']}','{product_item['ZCUST_DEVICE']}')
            '''
            if not con.exec_n(sql2_n):
                return None

            sales_order['ITEM'].append(product_item)

        sales_orders['SO_DATA'].append(sales_order)
    return sales_orders


def create_sales_orders(con, input, res):
    res['REQ_DATA'] = input
    action = cw.WS().send(input, 'SD017')
    if not action['status']:
        res['ERR_MSG'] = action['desc']
        return False

    output = action['data']
    return_node = output.get('RETURN')
    if not return_node:
        res['ERR_MSG'] = f'SAP接口返回字段错误:{output}'
        return False

    if isinstance(return_node, list):
        for item in return_node:
            if not 'VBELN' in item:
                res['RES_DATA'].append(item)
                return False

            update_sales_orders(con, item)
            res['RES_DATA'].append(item)

    else:
        item = return_node
        if not 'VBELN' in item:
            res['RES_DATA'].append(item)
            return False

        update_sales_orders(con, item)
        res['RES_DATA'].append(item)

    return True


# 更新CDM
def update_sales_orders(con, item):
    cdm_no = item.get('HEAD_NO', '')     # CDM no
    cdm_item_no = item.get('ITEM_NO', '')  # CDM item no
    so_no = item.get('VBELN', '')        # so no
    so_item_no = item.get('POSNR', '')  # so item no

    sql = f"SELECT PO_NO FROM ZM_CDM_SO_HEADER WHERE SO_SN = '{cdm_no}' "
    item['PO_NO'] = xstr(con.query(sql)[0][0])

    if cdm_item_no:
        sql = f"SELECT PRD_ID,SAP_PRD_ID FROM ZM_CDM_SO_ITEM zcsi WHERE SO_SN = '{cdm_no}' AND CDM_ITEM_SN = '{cdm_item_no}' "
        item['PRODUCT_ID'] = xstr(con.query(sql)[0][0])
        item['SAP_PRODUCT_ID'] = xstr(con.query(sql)[0][1])
    else:
        item['PRODUCT_ID'] = ''
        item['SAP_PRODUCT_ID'] = ''

    if so_no and so_item_no:
        sql = f"update ZM_CDM_SO_HEADER set SO_NO='{so_no}',flag='1' where so_sn = '{cdm_no}'  "
        con.exec_c(sql)

        sql = f'''update ZM_CDM_SO_ITEM set SO_ITEM_SN = '{so_item_no}', flag='1'  WHERE SO_SN  = '{cdm_no}'
         and CDM_ITEM_SN = '{cdm_item_no}' '''
        con.exec_c(sql)

        sql = f"update ZM_CDM_SO_SUB_ITEM set flag = '1' WHERE ITEM_SN = '{cdm_item_no}' "
        con.exec_c(sql)

        sql = f'''update ZM_CDM_PO_ITEM set flag = '1',SO_ID = '{so_no}', SO_ITEM = '{so_item_no}',
        CDM_ID = '{cdm_no}',CDM_ITEM='{cdm_item_no}'
        where PO_ID = '{item['PO_NO']}' and SO_ID is null and SO_ITEM is null  '''
        con.exec_c(sql)


def test():
    res = {}
    con = conn.HanaConn()
    upload_id = '29d227a0'
    sales_orders = get_sales_orders(upload_id, 50000)
    print(json.dumps(sales_orders))
    if sales_orders:
        if not create_sales_orders(con, sales_orders, res):
            print("创建成功")


if __name__ == "__main__":
    data = {'header': {'bonded_type': 'Y', 'cust_code': 'AA', 'delay_days': '', 'err_desc': '', 'file_name': 'd24a59销售订单创建 V1.0.xlsx', 'file_path': '/root/CDM_DEV/cdm_1.1_flask/uploads/po/ZOR3/AA/销售订单创建 V1(6).0(6).xlsx', 'mail_tip': '', 'need_delay': 'false', 'need_mail_tip': 'false', 'offer_sheet': '', 'po_level': 'primary', 'po_type': 'ZOR3', 'template_sn': 'd285ab', 'template_type': 'LOT|WAFER|DIES|CUSTPN|FABPN|PO', 'user_name': '07885'}, 'items': [
        {'customer_device': 'AR0330CM1C21SHKA0-CP', 'fab_device': '', 'passbin_count': '28980', 'po_id': '14097654223234', 'product_pn': '19Y76015M000CF'}]}
    create_76_po(data)
