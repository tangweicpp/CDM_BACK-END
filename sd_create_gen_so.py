import datetime
import json
import uuid
import requests
from flask import make_response
from flask import abort
import conn_db as conn
import sd_parse_special_po_xl as spsp
from collections import Counter
from mm_mat_info import get_mat_master_data_csp
from web_api_client import get_data_from_web_api


fo_flag = ''


def xstr(s):
    return '' if s is None else str(s).strip()


def create_gen_so(po_data):
    print(po_data)

    res = {'ERR_MSG': ''}

    con = conn.HanaConn()

    po_header = po_data['header']
    po_items = po_data['items']

    # 初始化数据
    init_po_data(po_header, po_items)
    # 检查数据
    check_po_data(po_header, po_items)

    # 保存数据
    save_po_data(con, po_header, po_items)

    # 创建晶圆采购单
    save_wafer_po_data(con, po_header)

    # 创建SO
    if 'FO' in po_header['template_desc']:
        so_data = get_sales_order_fo(
            con, po_header['upload_id'], po_header['cust_code'])

    else:
        so_data = get_sales_order(
            con, po_header['upload_id'], po_header['cust_code'])

    if so_data['SO_DATA']:
        create_sales_order(con, so_data, po_header['upload_id'])

    # 创建后台任务
    create_po_back_task(con, po_header)

    # 获取订单创建结果
    res['RES_DATA'], po_flag = get_po_status(con, po_header['upload_id'])

    # 数据提交
    if po_flag:
        con.db.commit()
    else:
        con.db.rollback()

    return res


# 获取FAB机种名
def get_fab_device_name(fab_g_name):
    con = conn.HanaConnDW()
    sql = f"SELECT KEY2 FROM ZM_CONFIG_TYPE_LIST WHERE CONFIG_TYPE = '2' AND KEY1 = '{fab_g_name}' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    else:
        return fab_g_name


# 创建晶圆采购单
def save_wafer_po_data(con, po_header):
    if po_header.get('create_bank_wo') == 'true':
        create_wafer_po_data(con, po_header['upload_id'])


def create_wafer_po_data(con, upload_id):
    po_data = {'formList': []}

    # 表头
    sql = f"SELECT DISTINCT CUST_CODE,BONDED ,CREATE_BY FROM ZM_CDM_PO_ITEM zcpi WHERE UPLOAD_ID = '{upload_id}' "
    results = con.query(sql)
    if results:
        cust_code = xstr(results[0][0])
        bonded = xstr(results[0][1])
        creater = xstr(results[0][2])
    else:
        return False

    # 明细
    # fab层级
    sql = f"SELECT DISTINCT FAB_DEVICE FROM ZM_CDM_PO_ITEM zcpi WHERE UPLOAD_ID = '{upload_id}' "
    results = con.query(sql)
    if results:
        for row in results:
            fab_node = {}
            fab_node['creater'] = creater
            fab_node['custCode'] = cust_code
            fab_node['fabDevice'] = get_fab_device_name(xstr(row[0]))
            fab_node['isBonded'] = bonded
            fab_node['remark'] = '订单同步推送'
            fab_node['purchase_source'] = 1
            fab_node['request_type'] = "ZJ2" if bonded == 'Y' else 'ZJ4'
            fab_node['lotList'] = []
            po_data['formList'].append(fab_node)

            # lot层级
            sql = f"SELECT LOT_ID,ADD_1,COUNT(1) FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}' AND FAB_DEVICE = '{xstr(row[0])}' GROUP BY LOT_ID,ADD_1   "
            results2 = con.query(sql)
            if results2:
                for row2 in results2:
                    lot_node = {}

                    lot_node['qty'] = row2[2]
                    lot_node['remark'] = ''
                    lot_node['waferList'] = []

                    if cust_code in ('US008', '70', 'HK099', 'HK006', 'SH296', 'DA69', 'AT51', 'SZ280', 'BJ218'):
                        fab_lot = xstr(row2[1])
                        lot_node['lot'] = fab_lot if fab_lot else xstr(row2[0])
                        sql = f"SELECT REPLACE(LOT_WAFER_ID,'{xstr(row2[0])}','{lot_node['lot']}') FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}' AND FAB_DEVICE = '{xstr(row[0])}' AND LOT_ID = '{xstr(row2[0])}'  "

                    else:

                        lot_node['lot'] = xstr(row2[0])
                        if row2[1]:
                            sql = f"SELECT LOT_WAFER_ID FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}' AND FAB_DEVICE = '{xstr(row[0])}' AND LOT_ID = '{lot_node['lot']}' AND ADD_1 = '{row2[1]}' "
                        else:
                            sql = f"SELECT LOT_WAFER_ID FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}' AND FAB_DEVICE = '{xstr(row[0])}' AND LOT_ID = '{lot_node['lot']}' "

                    fab_node['lotList'].append(lot_node)

                    # Wafer层级
                    results3 = con.query(sql)
                    if results3:
                        for row3 in results3:
                            wafer_node = {}
                            wafer_node['waferID'] = xstr(row3[0])
                            lot_node['waferList'].append(wafer_node)

    if not po_data['formList']:
        return False

    print("晶圆采购单:", po_data)

    po_res = send_po_wafer_request(po_data)
    if po_res.get('ERR_MSG'):
        err_msg = {'ERR_MSG': "晶圆采购单创建异常:  " + po_res['ERR_MSG']}
        abort(make_response(err_msg))

    update_po_wafer_data(con, upload_id, po_res, po_data)

    return True


def update_po_wafer_data(con, upload_id, po_res, po_data):
    # 更新订单表
    sql = f"UPDATE ZM_CDM_PO_ITEM SET REMARK2 = '{po_res.get('PO_ID','')}',REMARK3= '{po_res.get('ERR_MSG','')[:100]}' WHERE UPLOAD_ID = '{upload_id}' "
    if con.exec_n(sql):
        print("更新成功")

     


def send_po_wafer_request(req):
    res = {'PO_ID': '', 'ERR_MSG': ''}
    req_data = json.dumps(req)
    print("***********发送请求*************")
    print(req_data)
    url = "http://10.160.1.128:9005/cos/wo/tw"
    headers = {"Content-Type": "application/json"}

    res_data = ''
    # 异步请求
    try:
        res_data = requests.post(
            url, data=req_data, headers=headers, timeout=(1, 5)).text

        print("*************返回响应*************")
        print(res_data)
        res_dict = json.loads(res_data)

    except requests.exceptions.RequestException as e:
        print("!!!接口异常=>", e)
        res['ERR_MSG'] = "!!!接口异常=>" + e
    else:
        if res_dict.get('msg') == 'success':
            res['PO_ID'] = res_dict.get('data', [])[0].get(
                'poNo', '') if res_dict.get('data') else '保税订单,等待报关'
        else:
            res['ERR_MSG'] = res_dict.get('msg')

    return res


# 初始化数据
def init_po_data(po_header, po_items):
    # US026 WI
    if po_header.get('cust_code') in ('US026', 'SG005'):
        file_path = po_header.get('file_path')
        res = spsp.parse_SG005_WI(file_path)
        if not res:
            err_msg = {'ERR_MSG': 'WI上传任务创建失败'}
            abort(make_response(err_msg))

    po_header['err_desc'] = ''

    for po_item in po_items:
        po_item['mat_sql'] = ''


# 检查数据
def check_po_data(po_header, po_items):
    lot_wafer_id_list = []
    product_pn_list = []
    wafer_pn_list = []
    po_list = []
    con_dw = conn.HanaConnDW()

    for item in po_items:
        # ZJ41打标码周记检查
        if po_header['cust_code'] in ('ZJ41') and item.get('add_4'):
            if item['add_4'][1:2] in ("I", "O", "Z", "i", "l", "o", "z", "0", "1", "2"):
                abort(make_response(
                    {"ERR_MSG": f"ZJ41打标码周记{item['add_4']}第二位字符{item['add_4'][1:2]}异常,第二个字符不能有I,O,Z,i,l,o,z,0,1,2这些字符"}))

        product_pn = item['product_pn']
        po_id = item['po_id']

        # US008 BK校验
        if po_header['cust_code'] in ('US008'):
            sql = f"SELECT ZZPROCESS FROM MARA m WHERE ZZCNLH = '{product_pn}' "
            results = con_dw.query(sql)
            if results:
                process = xstr(results[0][0])
                if 'FC' in process and item.get('add_6'):
                    if not item.get('khzy5'):
                        abort(make_response(
                            {"ERR_MSG": f"US008 FC机种必须维护BK信息, 请联系NPI维护"}))

                    if not item.get('add_6'):
                        abort(make_response(
                            {"ERR_MSG": f"US008 FC机种订单必须要有BK信息, 请内勤确认"}))

                    if not item['add_6'] in item['khzy5']:
                        abort(make_response(
                            {"ERR_MSG": f"US008 FC机种BK信息错误,客户PO-{item['add_6']},NPI维护-{item['khzy5']}, 请联系NPI维护"}))

        product_pn_list.append(product_pn)
        po_list.append(po_id)

        for wafer in item['wafer_list']:
            lot_wafer_id_list.append(wafer['lot_wafer_id'])
            wafer_pn_list.append(wafer['lot_wafer_id']+'_' + product_pn)

    if not po_header.get('fcChecked'):
        check_po_wafer_id(lot_wafer_id_list,
                          product_pn_list, wafer_pn_list, po_list)


# 检查wafer数据
def check_po_wafer_id(lot_wafer_id_list, product_pn_list, wafer_pn_list, po_list):
    d = dict(Counter(wafer_pn_list))
    repeat_wafer_id = [key for key, value in d.items()if value > 1]
    if repeat_wafer_id:
        err_msg = {"ERR_MSG": f"同料号WAFER ID重复:{repeat_wafer_id}"}
        abort(make_response(err_msg))

    con = conn.HanaConn()
    con_or = conn.OracleConn()
    if str(tuple(lot_wafer_id_list))[-2:-1] == ',':
        str_wafer_id_total = str(tuple(lot_wafer_id_list)).replace(',', '')
    else:
        str_wafer_id_total = str(tuple(lot_wafer_id_list))

    if str(tuple(po_list))[-2:-1] == ',':
        str_po_id_total = str(tuple(po_list)).replace(',', '')
    else:
        str_po_id_total = str(tuple(po_list))

    if str(tuple(product_pn_list))[-2:-1] == ',':
        product_pn_total = str(tuple(product_pn_list)).replace(',', '')
    else:
        product_pn_total = str(tuple(product_pn_list))

    # sql = f"SELECT LOT_WAFER_ID,PRODUCT_PN  FROM ZM_CDM_PO_ITEM WHERE LOT_WAFER_ID IN {str_wafer_id_total} AND PRODUCT_PN IN {product_pn_total}  and flag = '1' and flag2='0' "
    sql = f"SELECT LOT_WAFER_ID,PRODUCT_PN  FROM ZM_CDM_PO_ITEM WHERE LOT_WAFER_ID IN {str_wafer_id_total} AND PRODUCT_PN IN {product_pn_total}  and flag = '1' and HT_PN <> 'XTT02002FC' and HT_PN <> 'XHW50001FC' "
    print(sql)
    results = con.query(sql)
    if results:
        abort(make_response(
            {"ERR_MSG":  f"系统里已经存在相同料号{results[0][1]} 的wafer订单记录,不可再次上传该片：{results[0][0]}"}))


# 保存数据
def save_po_data(con, po_header, po_items):
    # 片号更新
    wafer_update_list = []

    # 表头
    po_header['upload_id'] = get_rand_id(8)
    sql = f'''INSERT into ZM_CDM_PO_HEADER(BONDED_TYPE,CUST_CODE,DELAY_DAYS,FILE_NAME,FILE_PATH,
    MAIL_TIP,OFFER_SHEET,PO_LEVEL,PO_TYPE,TRAD_CUST_CODE,USER_NAME,UPLOAD_ID,FLAG,UPLOAD_DATE,ID)
    values('{po_header['bonded_type']}','{po_header['cust_code']}','{po_header['delay_days']}',
    '{po_header['file_name']}','{po_header['file_path']}','{po_header['mail_tip']}',
    '{po_header['offer_sheet']}','{po_header['po_level']}','{po_header['po_type']}',
    '{po_header['cust_code']}','{po_header['user_name']}',
    '{po_header['upload_id']}','1',now(),ZM_CDM_PO_HEADER_SEQ.NEXTVAL) '''

    if not con.exec_n(sql):
        res = {}
        con.db.rollback()
        res['ERR_MSG'] = '订单头表(ZM_CDM_PO_HEADER)保存错误'
        res['ERR_SQL'] = sql
        abort(make_response(res))

    # 明细行
    for item in po_items:
        wafer_list = item['wafer_list']

        for wafer in wafer_list:
            wafer_data = get_wafer_data(con, wafer, po_header, item)

            # 插入晶圆片号更新
            if po_header['cust_code'] in ('US008', '70', 'HK099', 'HK006'):
                wafer_lot = wafer_data['add_1']
            else:
                wafer_lot = wafer_data['lot_id']

            if not wafer_lot in wafer_update_list:
                wafer_update_list.append(wafer_lot)
                sql = f"""INSERT INTO ZM_CDM_WAFER_ID_UPDATE_TASK(WAFER_LOT,FLAG,CREATE_DATE,CREATE_BY,REMARK1,UPLOAD_ID)
                    values('{wafer_lot}','0',now(),'{wafer_data['create_by']}','','{wafer_data['upload_id']}')
                """
                con.exec_n(sql)

            # 插入明细表
            if po_header.get('fcChecked'):
                if not ("FC" in wafer_data['product_pn'] or "FT" in wafer_data['product_pn'] or "UXMPW201115" in wafer_data['customer_device']):
                    abort(make_response({"ERR_MSG": "非FC/FT的料号不可跳过重复片检查"}))
                # elif wafer_data['po_type'] != "ZOR1":
                #     abort(make_response({"ERR_MSG": "非样品阶段的料号不可跳过重复片检查"}))
                else:
                    flag3 = "FC_FT_NO_REP_CHECK:" + get_rand_id(8)
            else:
                flag3 = "1"

            sql = f'''INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,PO_TYPE,PO_DATE,BONDED,CUSTOMER_DEVICE,FAB_DEVICE,HT_PN,PRODUCT_PN,
            SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,MARK_CODE,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,
            ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,ADD_23,ADD_24,ADD_25,ADD_26,ADD_27,ADD_28,ADD_29,ADD_30,
            FLAG,FLAG2,FLAG3,CREATE_DATE,CREATE_BY,WAFER_TIMES,UPLOAD_ID,WAFER_SN,WAFER_HOLD,ID,WAFER_PCS_PRICE,WAFER_DIE_PRICE,ADDRESS_CODE,BASE_SO,REMARK1,
            STATUS,PO_H,CUST_FAB_DEVICE_1,CUST_FAB_DEVICE_2,REMAKR5)
            values('{wafer_data['cust_code']}','{wafer_data['sap_cust_code']}','{wafer_data['trad_cust_code']}','{wafer_data['po_id']}','{wafer_data['po_type']}',
            '{wafer_data['po_date']}','{wafer_data['bonded']}','{wafer_data['customer_device']}','{wafer_data['fab_device']}','{wafer_data['ht_pn']}','{wafer_data['product_pn']}',
            '{wafer_data['sap_product_pn']}','{wafer_data['lot_id']}','{wafer_data['wafer_id']}','{wafer_data['lot_wafer_id']}','{wafer_data['passbin_count']}',
            '{wafer_data['failbin_count']}','{wafer_data['mark_code']}', '{wafer_data['add_0']}','{wafer_data['add_1']}','{wafer_data['add_2']}','{wafer_data['add_3']}',
            '{wafer_data['add_4']}','{wafer_data['add_5']}','{wafer_data['add_6']}','{wafer_data['add_7']}','{wafer_data['add_8']}','{wafer_data['add_9']}',
            '{wafer_data['add_10']}','{wafer_data['add_11']}','{wafer_data['add_12']}','{wafer_data['add_13']}','{wafer_data['add_14']}','{wafer_data['add_15']}',
            '{wafer_data['add_16']}','{wafer_data['add_17']}','{wafer_data['add_18']}','{wafer_data['add_19']}','{wafer_data['add_20']}','{wafer_data['add_21']}',
            '{wafer_data['add_22']}','{wafer_data['add_23']}','{wafer_data['add_24']}','{wafer_data['add_25']}','{wafer_data['add_26']}','{wafer_data['add_27']}',
            '{wafer_data['add_28']}','{wafer_data['add_29']}','{wafer_data['add_30']}',
            '1','0','{flag3}',now(),'{wafer_data['create_by']}','{wafer_data['upload_times']}',
            '{wafer_data['upload_id']}',zm_cdm_wafer_sn_seq_new.nextval,'{wafer_data['hold_flag']}',{wafer_data['id']},'{wafer_data['wafer_pcs_price']}','{wafer_data['wafer_die_price']}',
            '{wafer_data['address_code']}','{wafer_data['base_so']}','{wafer_data['po_item']}','成功', '{wafer_data['mark_code']}','{wafer_data['po_customer_device']}','{wafer_data['po_fab_device']}','{wafer_data['remark_5']}')  '''
            print(sql)
            if not con.exec_n(sql):
                con.db.rollback()
                res = {}
                res['ERR_MSG'] = '订单ITEM表(ZM_CDM_PO_ITEM)保存错误'
                res['ERR_SQL'] = sql

                abort(make_response(res))


# 获取SAP客户
def get_sap_cust_code(cust_code):
    con = conn.HanaConnDW()
    sql = f"SELECT DISTINCT PARTNER FROM VM_SAP_PO_CUSTOMER WHERE ZZYKHH = '{cust_code}' "
    results = con.query(sql)
    if results:
        sap_cust_code = xstr(results[0][0])
        return sap_cust_code

    return ''


# 获取上段打标码
def get_last_mark_code(lot_wafer_id):
    con = conn.HanaConn()
    query_wafer_id = lot_wafer_id.replace('+', '')
    sql = f" SELECT DISTINCT MARK_CODE,ADD_8 FROM ZM_CDM_PO_ITEM zcpi WHERE REPLACE(LOT_WAFER_ID,'+','') = '{query_wafer_id}' AND MARK_CODE IS NOT NULL AND mark_code <> '' "
    results = con.query(sql)
    if len(results) == 1:
        return xstr(results[0][0]), xstr(results[0][1])
    else:
        return '', ''


# 获取wafer数据
def get_wafer_data(con, wafer, po_header, item):
    wafer_data = {}

    if item.get('po_type'):
        if item['po_type'] == '样品订单':
            wafer_data['po_type'] = "ZOR1"
        elif item['po_type'] == '小批量订单':
            wafer_data['po_type'] = "ZOR2"
        elif item['po_type'] == '量产订单':
            wafer_data['po_type'] = "ZOR3"
        elif item['po_type'] == '免费订单':
            wafer_data['po_type'] = "ZOR4"
        elif item['po_type'] == 'RMA收费订单':
            wafer_data['po_type'] = "ZOR5"
        elif item['po_type'] == 'RMA免费订单':
            wafer_data['po_type'] = "ZOR6"
        else:
            wafer_data['po_type'] = item['po_type']

    else:
        wafer_data['po_type'] = po_header.get('po_type', '')

    wafer_data['po_date'] = po_header.get('po_date', get_curr_date(0))
    wafer_data['upload_id'] = po_header.get('upload_id', '')
    wafer_data['bonded'] = po_header.get('bonded_type', '')
    wafer_data['create_by'] = po_header.get('user_name', '')
    wafer_data['cust_code'] = po_header['cust_code']
    wafer_data['sap_cust_code'] = po_header['sap_cust_code']
    wafer_data['trad_cust_code'] = po_header['sap_cust_code'] if po_header['trad_cust_code'] == po_header['cust_code'] else get_sap_cust_code(
        po_header['trad_cust_code'])
    wafer_data['po_id'] = item.get('po_id', '')
    wafer_data['po_item'] = item.get('po_item', '')
    wafer_data['customer_device'] = item.get('customer_device', '')
    wafer_data['po_customer_device'] = item.get('po_customer_device', '')
    wafer_data['fab_device'] = item.get('fab_device', '')
    wafer_data['po_fab_device'] = item.get('po_fab_device', '')

    if not wafer_data['po_fab_device']:
        wafer_data['po_fab_device'] = wafer_data['fab_device']

    if not wafer_data['po_customer_device']:
        wafer_data['po_customer_device'] = wafer_data['customer_device']

    wafer_data['product_pn'] = item.get('product_pn', '')
    wafer_data['sap_product_pn'] = item.get('sap_product_pn', '')
    wafer_data['ht_pn'] = item.get('ht_pn', '')
    wafer_data['passbin_count'] = item.get('passbin_count', 0)
    wafer_data['failbin_count'] = item.get('failbin_count', 0)
    wafer_data['mark_code'] = item.get('mark_code', '')
    wafer_data['id'] = 0 if item.get('real_wafer_id', '') == 'N' else 1
    wafer_data['add_0'] = item.get('add_0', '')
    wafer_data['add_1'] = item.get('add_1', '')
    wafer_data['add_2'] = item.get('add_2', '')
    wafer_data['add_3'] = item.get('add_3', '')
    wafer_data['add_4'] = item.get('add_4', '')
    wafer_data['add_5'] = item.get('add_5', '')
    wafer_data['add_6'] = item.get('add_6', '')
    wafer_data['add_7'] = item.get('add_7', '')
    wafer_data['add_8'] = item.get('add_8', '')
    wafer_data['add_9'] = item.get('add_9', '')
    wafer_data['add_10'] = item.get('add_10', '')
    wafer_data['add_11'] = item.get('add_11', '')
    wafer_data['add_12'] = item.get('add_12', '')
    wafer_data['add_13'] = item.get('add_13', '')
    wafer_data['add_14'] = item.get('add_14', '')
    wafer_data['add_15'] = item.get('add_15', '')
    wafer_data['add_16'] = item.get('add_16', '')
    wafer_data['add_17'] = item.get('add_17', '')
    wafer_data['add_18'] = item.get('add_18', '')
    wafer_data['add_19'] = item.get('add_19', '')
    wafer_data['add_20'] = item.get('add_20', '')
    wafer_data['add_21'] = item.get('add_21', '')
    wafer_data['add_22'] = item.get('add_22', '')
    wafer_data['add_23'] = item.get('add_23', '')
    wafer_data['add_24'] = item.get('add_24', '')
    wafer_data['add_25'] = item.get('add_25', '')
    wafer_data['add_26'] = item.get('add_26', '')
    wafer_data['add_27'] = item.get('add_27', '')
    wafer_data['add_28'] = item.get('add_28', '')
    wafer_data['add_29'] = item.get('add_29', '')
    wafer_data['add_30'] = item.get('add_30', '')
    wafer_data['remark_5'] = item.get('child_pn', '')

    wafer_data['lot_id'] = item.get('lot_id', '')
    wafer_data['po_qty'] = int(float(item.get('po_qty', 0)))
    if wafer_data['po_qty']:
        wafer_data['add_30'] = wafer_data['po_qty']

    wafer_data['wafer_pcs_price'] = item.get('wafer_pcs_price', '')
    wafer_data['wafer_die_price'] = item.get('wafer_die_price', '')
    wafer_data['address_code'] = item.get('address_code', '')
    wafer_data['wafer_id'] = wafer['wafer_id']
    wafer_data['lot_wafer_id'] = wafer['lot_wafer_id']
    wafer_data['wafer_sn'] = get_rand_id(8)
    wafer_data['upload_times'] = get_upload_times(
        con, wafer_data['lot_wafer_id'])
    wafer_data['hold_flag'] = 'Y' if wafer['hold_flag'] else 'N'
    wafer_data['base_so'] = item.get('base_so', '')

    if not wafer_data['trad_cust_code']:
        res = {}
        res['ERR_MSG'] = '交易客户代码不存在'
        con.db.rollback()
        abort(make_response(res))

    # GC二级代码
    # if po_header.get('r_cust_code', '') == 'HK109' and len(item.get('add_0', '')) != 3 and len(item.get('add_0', '')) != 5 and item.get('customer_device', '') != 'GC607-2.5':
    if po_header.get('r_cust_code', '') == 'HK109' and len(item.get('add_0', '')) != 3 and len(item.get('add_0', '')) != 5:
        sec_code = item.get('khzy4', '').split(';')

        if len(sec_code) <= 1:
            res = {}
            res['ERR_MSG'] = '二级代码未维护,请联系NPI维护专用字段4'
            con.db.rollback()
            abort(make_response(res))

        if not item.get('add_0'):
            res = {}
            res['ERR_MSG'] = '订单上没有二级代码,请确认'
            con.db.rollback()
            abort(make_response(res))

        wafer_data['add_0'] = item.get(
            'add_0', '') + sec_code[1]

    # sh07地址代码
    if po_header.get('r_cust_code') == 'SH07':
        # 片号检查
        if wafer_data['id'] == 0:
            res = {}
            res['ERR_MSG'] = 'SH07片号未维护,不允许上传'
            con.db.rollback()
            abort(make_response(res))

        # 查表
        address_code = conn.HanaConn().query(
            f"SELECT VALUE FROM ZM_CDM_KEY_LOOK_UP zcklu WHERE KEY = '{item['product_pn']}'  ")
        if address_code:
            wafer_data['add_7'] = xstr(address_code[0][0])

    # 打标码,GC WO
    if po_header.get('r_cust_code') == 'HK109':
        if not wafer_data['mark_code'] or not wafer_data['add_8']:
            wafer_data['mark_code'], wafer_data['add_8'] = get_last_mark_code(
                wafer_data['lot_wafer_id'])

    if '转NORMAL' in po_header['template_desc'] and po_header['cust_code'] in ('BJ49'):
        if not wafer_data['mark_code'] or not wafer_data['add_8']:
            wafer_data['mark_code'], wafer_data['add_8'] = get_last_mark_code(
                wafer_data['lot_wafer_id'])

    # 37失效旧WO
    if po_header['cust_code'] in ('US337'):
        disable_us337_old_wafer(con, wafer_data['lot_wafer_id'])

    return wafer_data


# 37旧数据失效
def disable_us337_old_wafer(con, lot_wafer_id):
    lot_wafer_id = lot_wafer_id.replace('+', '')
    sql = f"SELECT WAFER_SN FROM ZM_CDM_PO_ITEM zcpi WHERE replace(LOT_WAFER_ID,'+','')  = '{lot_wafer_id}' AND (PO_ID = '' or po_id is null) "
    results = con.query(sql)
    if results:
        wafer_sn = xstr(results[0][0])
        # 失效
        sql = f"update ZM_CDM_PO_ITEM set flag='0',UPDATE_DATE=NOW() where wafer_sn ='{wafer_sn}'  "
        con.exec_n(sql)


# 获取第几次上传
def get_upload_times(con, lot_wafer_id):
    lot_wafer_id = lot_wafer_id.replace('+', '')
    sql = f"SELECT WAFER_TIMES FROM ZM_CDM_PO_ITEM WHERE REPLACE(LOT_WAFER_ID,'+','') = '{lot_wafer_id}' AND FLAG ='1' order by create_date desc  "
    results = con.query(sql)
    if not results:
        return ''

    return xstr(results[0][0]) + "+"


# 获取随机数
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


# 获取订单日期
def get_curr_date(flag):
    return datetime.datetime.now().strftime('%Y%m%d') if flag == 1 else datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# 创建销售订单
def get_sales_order(con, upload_id, cust_code):
    so_data_list = {'SO_DATA': []}

    # header
    sql = f"""SELECT PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY,String_agg(WAFER_SN ,''',''') FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}' AND BASE_SO = 'Y'
            GROUP BY PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY  """

    results = con.query(sql)
    for row in results:
        so_data = {'HEADER': {}, 'ITEM': []}

        po_type = xstr(row[0])
        sap_cust_code = xstr(row[1])
        trad_cust_code = xstr(row[2])
        po_id = xstr(row[3])
        creater = xstr(row[4])
        wafer_sn_list = xstr(row[5])

        header = {}
        so_data['HEADER'] = header
        header['AUART'] = po_type
        header['KUNNR'] = sap_cust_code
        header['KUNRE'] = trad_cust_code
        header['BSTKD'] = po_id
        header['CREATER'] = creater
        header['UPLOAD_ID'] = upload_id
        header['ACTION'], header['HEAD_NO'] = get_so_action(con, header)

        # items
        sql = f"""SELECT SAP_PRODUCT_PN,sum(PASSBIN_COUNT+FAILBIN_COUNT),CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN,WAFER_PCS_PRICE,WAFER_DIE_PRICE,ADDRESS_CODE,count(1),REMARK1,String_agg(WAFER_SN ,''','''),PO_DATE
            FROM ZM_CDM_PO_ITEM WHERE WAFER_SN IN ('{wafer_sn_list}')
            GROUP BY SAP_PRODUCT_PN,CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN,WAFER_PCS_PRICE,WAFER_DIE_PRICE,ADDRESS_CODE,REMARK1,PO_DATE """

        results2 = con.query(sql)
        for row in results2:
            sap_product_pn = xstr(row[0])
            gross_dies = row[1]
            cust_device = xstr(row[2])
            fab_device = xstr(row[3])
            product_pn = xstr(row[4])
            wafer_pcs_price = xstr(row[5])
            wafer_die_price = xstr(row[6])
            address_code = xstr(row[7])
            wafer_pcs = row[8]
            po_item = xstr(row[9])
            wafer_sn_list_2 = xstr(row[10])
            po_date = xstr(row[11])
            if len(po_date) != 8:
                abort(make_response({"ERR_MSG": "接单日期错误"}))

            item = {}
            item['ACTION'] = 'N'
            item['ITEM_NO'] = get_rand_id(6)
            item['BSTDK'] = po_date
            item['BNAME'] = header['CREATER']
            item['MATNR'] = sap_product_pn
            item['KWMENG'] = gross_dies
            item['ZCUST_DEVICE'] = cust_device
            item['ZFAB_DEVICE'] = fab_device
            item['POSEX'] = po_item
            item['INCO1'] = ''
            item['INCO2'] = ''
            item['ZZDZDM'] = address_code

            if wafer_pcs_price and not cust_code in ('US008', '70', 'HK099', 'HK006', 'SH296'):
                item['NETPR'] = float(wafer_pcs_price) * \
                    int(wafer_pcs)

            if wafer_die_price and not cust_code in ('US008', '70', 'HK099', 'HK006', 'SH296'):
                item['NETPR'] = int(
                    wafer_die_price) * int(item['KWMENG'])

            # if wafer_pcs_price:
            #     item['NETPR'] = float(wafer_pcs_price) * \
            #         int(wafer_pcs)

            # if wafer_die_price:
            #     item['NETPR'] = int(
            #         wafer_die_price) * int(item['KWMENG'])

            sql = f'''INSERT INTO ZM_CDM_SO_ITEM(SO_SN,CDM_ITEM_SN,PRD_ID,QTY,CREATE_BY,CREATE_DATE,FLAG,SAP_PRD_ID)
                values('{header['HEAD_NO']}','{item['ITEM_NO']}','{product_pn}','{item['KWMENG']}',
                '{item['BNAME']}',now(),'0','{item['MATNR']}')
                '''
            if not con.exec_n(sql):
                con.db.rollback()
                abort(make_response({'ERR_MSG': 'SO_ITEM插入失败'}))

            # -----------------------------waferlist-------------------------------
            item['WAFER_LIST'] = []
            sql = f""" SELECT LOT_ID,WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,WAFER_SN,WAFER_HOLD,LOT_WAFER_ID FROM ZM_CDM_PO_ITEM
                WHERE WAFER_SN IN ('{wafer_sn_list_2}') ORDER BY LOT_ID,WAFER_ID """

            results3 = con.query(sql)
            for row3 in results3:
                lot_id = xstr(row3[0])
                wafer_id = xstr(row3[1])
                lot_wafer_id = xstr(row3[6])
                wafer_sn = xstr(row3[4])
                wafer_good_dies = row3[2]
                wafer_ng_dies = row3[3]
                wafer_gross_dies = wafer_good_dies + wafer_ng_dies
                wafer_hold = xstr(row3[5])
                wafer_release_date = get_curr_date(
                    1) if wafer_hold == 'Y' else ''

                wafer = {}
                wafer['ACTION'] = 'N'
                wafer['ZFAB_DEVICE'] = fab_device
                wafer['ZCUST_DEVICE'] = cust_device
                wafer['ZCUST_LOT'] = lot_id
                wafer['ZCUST_WAFER_ID'] = lot_wafer_id
                wafer['ZGOODDIE_QTY'] = wafer_good_dies
                wafer['ZBADDIE_QTY'] = wafer_ng_dies
                wafer['ZGROSSDIE_QTY'] = wafer_gross_dies
                wafer['HOLDLOT'] = wafer_hold
                wafer['REDATE'] = wafer_release_date

                sql = f"""INSERT INTO ZM_CDM_SO_SUB_ITEM(ITEM_SN,WAFER_SN,CUST_LOT_ID,CUST_WAFER_ID,CUST_LOTWAFER_ID,GOOD_DIES,NG_DIES,FLAG,REMARK1,REMARK2)
                values('{item['ITEM_NO']}','{wafer_sn}','{lot_id}','{wafer_id}','{lot_wafer_id}',
                '{wafer_good_dies}','{wafer_ng_dies}','0','{wafer_hold}','{wafer_release_date}')
                """
                if not con.exec_n(sql):
                    con.db.rollback()
                    abort(make_response({'ERR_MSG': 'SO WAFER_LIST插入失败'}))

                item['WAFER_LIST'].append(wafer)

            so_data['ITEM'].append(item)

        so_data_list['SO_DATA'].append(so_data)

    return so_data_list


# 创建FO销售订单
def get_sales_order_fo(con, upload_id, cust_code):
    global fo_flag
    fo_flag = upload_id
    so_data_list = {'SO_DATA': []}

    # header
    sql = f"SELECT DISTINCT PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY,add_30,ADD_3,ADD_29 FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID ='{upload_id}'  "
    results = con.query(sql)
    for row in results:
        so_data = {'HEADER': {}, 'ITEM': []}

        po_type = xstr(row[0])
        sap_cust_code = xstr(row[1])
        trad_cust_code = xstr(row[2])
        po_id = xstr(row[3])
        creater = xstr(row[4])
        po_qty = xstr(row[5])
        cust_device = xstr(row[7]) if xstr(row[7]) else xstr(row[6])

        header = {}
        so_data['HEADER'] = header
        header['AUART'] = po_type
        header['KUNNR'] = sap_cust_code
        header['KUNRE'] = trad_cust_code
        header['BSTKD'] = po_id
        header['CREATER'] = creater
        header['UPLOAD_ID'] = upload_id
        header['ACTION'], header['HEAD_NO'] = get_so_action(con, header)

        # items
        mat_data = get_mat_master_data_csp(customer_device=cust_device)
        product_pn = mat_data[0]['ZZCNLH']
        product_item = {}
        product_item['ACTION'] = 'N'                        # 动作标识
        product_item['ITEM_NO'] = get_rand_id(6)            # CRM行号
        product_item['BSTDK'] = get_curr_date(1)            # PO date
        product_item['BNAME'] = creater                     # CDM创建人帐号
        product_item['MATNR'] = mat_data[0]['MATNR']               # 物料号
        product_item['KWMENG'] = po_qty                # 数量
        product_item['ZCUST_DEVICE'] = mat_data[0]['ZZKHXH']       # 客户机种
        product_item['ZFAB_DEVICE'] = mat_data[0]['ZZFABXH']       # FAB机种
        product_item['WAFER_LIST'] = []

        sql2_n = f'''INSERT INTO ZM_CDM_SO_ITEM(SO_SN,CDM_ITEM_SN,PRD_ID,QTY,CREATE_BY,CREATE_DATE,FLAG,SAP_PRD_ID,REMARK1)
        values('{header['HEAD_NO']}','{product_item['ITEM_NO']}','{product_pn}','{product_item['KWMENG']}',
        '{product_item['BNAME']}',now(),'0','{product_item['MATNR']}','{product_item['ZCUST_DEVICE']}')
        '''
        if not con.exec_n(sql2_n):
            abort(make_response({'ERR_MSG': 'SO_ITEM插入失败'}))

        so_data['ITEM'].append(product_item)
        so_data_list['SO_DATA'].append(so_data)

    return so_data_list


# 获取ACTION
def get_so_action(con, header):
    sql = f"SELECT SO_SN FROM ZM_CDM_SO_HEADER WHERE PO_NO='{header['BSTKD']}' AND PO_TYPE='{header['AUART']}' AND CUST_CODE = '{header['KUNNR']}' AND SO_NO IS NOT NULL"
    results = con.query(sql)
    if results:
        action = "C"
        header_no = xstr(results[0][0])

    else:
        action = "N"
        header_no = get_rand_id(8)

        # 新建SO表头记录
        sql = f'''INSERT INTO ZM_CDM_SO_HEADER(PO_NO,PO_TYPE,SO_SN,SO_CREATE_BY,SO_CREATE_DATE,CUST_CODE,FLAG,PO_UPLOAD_ID)
            values('{header['BSTKD']}','{header['AUART']}','{header_no}','{header['CREATER']}',NOW(),'{header['KUNNR']}','0','{header['UPLOAD_ID']}') '''

        if not con.exec_n(sql):
            abort(make_response({'ERR_MSG': 'SO_HEADER插入失败'}))

    return action, header_no


# 发送SO
def create_sales_order(con, so_data, upload_id):
    res = get_data_from_web_api("SD017", so_data)
    print(res)
    if res['ERR_MSG']:
        update_po_data(con, 3, res['ERR_MSG'], upload_id)
    else:
        update_po_data(con, 1, res['RES_DATA_D'], upload_id)


# 更新订单数据
def update_po_data(con, flag, res_data, upload_id):
    if flag == 3:
        # 请求异常
        sql = f"UPDATE ZM_CDM_PO_HEADER SET FLAG='0' WHERE UPLOAD_ID = '{upload_id}' "
        con.exec_n(sql)
        sql = f"UPDATE ZM_CDM_PO_ITEM SET STATUS='{res_data}',FLAG=wafer_sn WHERE UPLOAD_ID = '{upload_id}' "
        con.exec_n(sql)

    if flag == 1:
        # 正常返回
        return_node = res_data['RETURN']
        if isinstance(return_node, list):
            for item in return_node:
                if not set_po_status(con, item):
                    clear_po_status(con, upload_id)

        elif isinstance(return_node, dict):
            item = return_node
            if not set_po_status(con, item):
                clear_po_status(con, upload_id)


# 清空订单状态
def clear_po_status(con, upload_id):
    # 请求异常
    sql = f"UPDATE ZM_CDM_PO_HEADER SET FLAG='0' WHERE UPLOAD_ID = '{upload_id}' "
    con.exec_n(sql)
    sql = f"UPDATE ZM_CDM_PO_ITEM SET STATUS='失败',FLAG=wafer_sn WHERE UPLOAD_ID = '{upload_id}' AND STATUS='成功' "
    con.exec_n(sql)


# 更新状态
def set_po_status(con, item):
    global fo_flag
    head_no = item.get('HEAD_NO', '')
    item_no = item.get('ITEM_NO', '')
    so_no = item.get('VBELN', '')
    so_item = item.get('POSNR', '')
    msg = item.get('MESSAGE', '')

    if msg == '成功':
        sql = f"update ZM_CDM_SO_HEADER set SO_NO='{so_no}',flag='1' where so_sn = '{head_no}'  "
        con.exec_n(sql)

        sql = f'''update ZM_CDM_SO_ITEM set SO_ITEM_SN = '{so_item}', flag='1'  WHERE SO_SN  = '{head_no}'
        and CDM_ITEM_SN = '{item_no}' '''
        con.exec_n(sql)

        sql = f"update ZM_CDM_SO_SUB_ITEM set flag = '1' WHERE ITEM_SN = '{item_no}' "
        con.exec_n(sql)

        if fo_flag:
            sql = f'''update ZM_CDM_PO_ITEM set flag='1', SO_ID='{so_no}', SO_ITEM='{so_item}',
                CDM_ID='{head_no}',CDM_ITEM='{item_no}',STATUS='{msg}'  WHERE UPLOAD_ID = '{fo_flag}' '''
            con.exec_n(sql)

        else:
            sql = f'''update ZM_CDM_PO_ITEM set flag='1', SO_ID='{so_no}', SO_ITEM='{so_item}',
                CDM_ID='{head_no}',CDM_ITEM='{item_no}',STATUS='{msg}'  WHERE wafer_sn in
                (select wafer_sn from ZM_CDM_SO_SUB_ITEM WHERE ITEM_SN = '{item_no}' ) '''
            con.exec_n(sql)

    else:
        sql = f"update ZM_CDM_SO_HEADER set flag='0' where so_sn = '{head_no}'  "
        con.exec_n(sql)

        sql = f'''update ZM_CDM_SO_ITEM set flag='0'  WHERE SO_SN  = '{head_no}' and CDM_ITEM_SN = '{item_no}' '''
        con.exec_n(sql)

        sql = f"update ZM_CDM_SO_SUB_ITEM set flag = '0' WHERE ITEM_SN = '{item_no}' "
        con.exec_n(sql)

        if fo_flag:
            sql = f'''update ZM_CDM_PO_ITEM set flag=WAFER_SN, STATUS='{msg}' WHERE UPLOAD_ID = '{fo_flag}' '''
            con.exec_n(sql)

        else:
            sql = f'''update ZM_CDM_PO_ITEM set flag=WAFER_SN,STATUS='{msg}' WHERE wafer_sn in
            (select wafer_sn from ZM_CDM_SO_SUB_ITEM WHERE ITEM_SN = '{item_no}' ) '''
            con.exec_n(sql)

        return False

    return True


# 获取订单状态
def get_po_status(con, upload_id):
    global fo_flag
    po_res = []
    po_flag = True
    sql = f"SELECT DISTINCT PO_ID,PRODUCT_PN,SAP_PRODUCT_PN,CUSTOMER_DEVICE,SO_ID,SO_ITEM,STATUS,BASE_SO,UPLOAD_ID,REMARK2 FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID='{upload_id}' "
    results = con.query(sql)
    if results:
        for row in results:
            item = {}
            item['PO_NO'] = xstr(row[0])
            item['PRODUCT_ID'] = xstr(row[1])
            item['SAP_PRODUCT_ID'] = xstr(row[2])
            item['CUST_DEVICE'] = xstr(row[3])
            item['VBELN'] = xstr(row[4])
            item['POSNR'] = xstr(row[5])
            item['MESSAGE'] = xstr(row[6])

            if item['MESSAGE'] != "成功":
                po_flag = False

            item['BASE_SO'] = xstr(row[7])
            item['UPLOAD_ID'] = xstr(row[8])
            item['WAFER_PO'] = xstr(row[9])
            po_res.append(item)

    # FO CLEAR 销售订单
    if fo_flag:
        sql = f"UPDATE ZM_CDM_PO_ITEM set so_id = '', so_item='' where UPLOAD_ID='{upload_id}' "
        results = con.query(sql)
        fo_flag = ''

    return po_res, po_flag


# 创建后台任务
def create_po_back_task(con, po_header):
    # 有效数据
    sql = f"SELECT * FROM ZM_CDM_PO_ITEM zcpi WHERE FLAG ='1' AND UPLOAD_ID = '{po_header['upload_id']}' "
    results = con.query(sql)
    if not results:
        return False

    # 发送邮件
    po_header_str = json.dumps(po_header, ensure_ascii=False)
    sql = f"insert into ZM_CDM_PO_UPLOAD_TASK(TASK_DATA,TASK_TYPE,TASK_FLAG,UPLOAD_ID,DESC,CREATE_DATE,CREATE_BY) values('{po_header_str}','MAIL_SENDER','0','{po_header['upload_id']}','订单邮件抛送',NOW(),'{po_header['user_name']}')"
    if not con.exec_n(sql):
        err_msg = {'ERR_MSG': '订单上传邮件抛送任务创建失败'}
        abort(make_response(err_msg))

    # 发送打标码请求
    sql = f"insert into ZM_CDM_PO_UPLOAD_TASK(TASK_DATA,TASK_TYPE,TASK_FLAG,UPLOAD_ID,DESC,CREATE_DATE,CREATE_BY) values('','MARK_CODE','0','{po_header['upload_id']}','打标码更新',NOW(),'{po_header['user_name']}')"
    if not con.exec_n(sql):
        err_msg = {'ERR_MSG': '打标码任务创建失败'}
        abort(make_response(err_msg))

    return True


if __name__ == "__main__":
    create_gen_so({'header': {'bonded_type': 'Y', 'common_checked': 'false', 'create_bank_wo': 'true', 'cust_code': 'TS81', 'delay_days': '', 'err_desc': '', 'file_name': '2732dd20210312003.xls', 'file_path': '/opt/CDM_BACK-END/docs/TMI202107140118H(0(0(0(0).xls', 'mail_tip': '', 'need_delay': 'false', 'need_mail_tip': 'false', 'offer_sheet': '', 'po_date': '20210714', 'po_level': 'primary', 'po_type': 'ZOR3', 'process': '', 'sap_cust_code': '101916', 'template_desc': 'BUMPING订单', 'template_sn': '276e7c', 'template_type': 'LOT|WAFER|DIES|CUSTPN|FABPN|PO', 'user_name': '07885', 'trad_cust_code': 'TS81', 'fcChecked': False}, 'items': [{'base_so': 'Y', 'child_pn': '000000000042209284', 'customer_device': 'STI8070A', 'dies_from_po': False, 'fab_device': 'ST0190_2_2A', 'failbin_count': 0, 'ht_pn': 'XTS81004B', 'khzy1': '', 'khzy2': '', 'khzy3': '', 'khzy4': '', 'khzy5': '', 'lcbz': 'N', 'lot_id': 'CN6451.1TEST', 'mat_hold': '', 'passbin_count': 8699, 'po_customer_device': 'STI8070A', 'po_fab_device': 'ST0190_2_2A', 'po_id': 'TMI202107140118H', 'po_type': '样品订单', 'product_pn': '18XTS81004B0BL', 'product_pn_list': ['18XTS81004B0BL'], 'sap_product_pn': '32116783', 'valid': True, 'wafer_dies': 8699, 'wafer_id_list': ['01'], 'wafer_id_str': '1#', 'wafer_list': [{'hold_flag': False, 'lot_id': 'CN6451.1TEST', 'lot_wafer_id': 'CN6451.1TEST01', 'real_wafer_id': 'Y', 'wafer_id': '01'}], 'wafer_pn': '', 'wafer_qty': 1, 'warn_desc': 'ok', 'trad_cust_code': 'TS81'}, {'base_so': 'Y', 'child_pn': '000000000042209284', 'customer_device': 'STI8070B', 'dies_from_po': False, 'fab_device': 'ST0190_2_2A', 'failbin_count': 0, 'ht_pn': 'XTS81005B', 'khzy1': '', 'khzy2': '', 'khzy3': '', 'khzy4': '', 'khzy5': '', 'lcbz': 'N', 'lot_id': 'CN6451.1TEST', 'mat_hold': '', 'passbin_count': 8699, 'po_customer_device': 'STI8070B', 'po_fab_device': 'ST0190_2_2A', 'po_id': 'TMI202107140118H', 'po_type': '样品订单', 'product_pn': '18XTS81005B0BL', 'product_pn_list': ['18XTS81005B0BL'], 'sap_product_pn': '32116785', 'valid': True, 'wafer_dies': 8699, 'wafer_id_list': ['02'], 'wafer_id_str': '2#', 'wafer_list': [{'hold_flag': False, 'lot_id': 'CN6451.1TEST', 'lot_wafer_id': 'CN6451.1TEST02', 'real_wafer_id': 'Y', 'wafer_id': '02'}], 'wafer_pn': '', 'wafer_qty': 1, 'warn_desc': 'ok', 'trad_cust_code': 'TS81'}]})
