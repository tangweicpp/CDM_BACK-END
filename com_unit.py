'''
@File    :   com_unit.py
@Time    :   2020/11/07 09:53:09
@Author  :   Tony Tang
@Version :   1.0
@Contact :   wei.tang_ks@ht-tech.com
@License :   (C)Copyright 2020-2025, Htks
@Desc    :   None
'''
from numpy.core.fromnumeric import reshape
import conn_db as conn
import requests
import com_ws as cw
import json
import uuid
import datetime
import os
import re


import trans_sql_to_xl as ttx
from requests.auth import HTTPBasicAuth
import set_mark_code as smc
from flask import abort
from flask import make_response
import pandas as pd
from mm_mat_info import get_mat_master_data
from mm_mat_info import get_mat_data
from web_api_client import get_data_from_web_api


SO_URL_KSSD017 = 'http://192.168.98.245:50000/RESTAdapter/SD/KSSD017'


def xstr(s):
    return '' if s is None else str(s).strip()


def auth(username, password):
    con = conn.HanaConn()

    res = {'ERR_MSG': ''}

    # 用户名校验
    sql = f"SELECT USER_PASSWD FROM ZM_CDM_USER_INFO  WHERE USER_ID = '{username}' AND FLAG = 'Y' "
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "用户名不存在"
        return res

    # 密码校验
    r_passwd = xstr(results[0][0])
    if r_passwd != password:
        res['ERR_MSG'] = "密码不正确,如忘记密码请咨询IT获取"
        return res

    # 密码更新
    if username == password:
        res['ERR_MSG'] = "密码不可和用户名一致,请修改密码并登录"
        return res

    if username == '07885':
        res['USER_NAME'] = "系统管理员"
    else:
        res['USER_NAME'] = get_user_real_name(user_id=username)

    return res


# 修改密码
def modify_user_passwd(username, old_passwd, new_passwd):
    con = conn.HanaConn()

    res = {'ERR_MSG': ''}

    # 用户名校验
    sql = f"SELECT USER_PASSWD FROM ZM_CDM_USER_INFO  WHERE USER_ID = '{username}' AND FLAG = 'Y' "

    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "用户名不存在"
        return res

    # 校验原密码
    r_passwd = xstr(results[0][0])
    if old_passwd != r_passwd:
        print(old_passwd, r_passwd)
        res['ERR_MSG'] = "原密码不正确,请输入正确的原密码, 或咨询IT"
        return res

    # 修改
    sql = f"UPDATE ZM_CDM_USER_INFO SET USER_PASSWD = '{new_passwd}',CREATE_DATE=NOW() WHERE USER_ID = '{username}' AND FLAG = 'Y'"
    con.exec_c(sql)

    return res


def get_user_real_name(user_id):
    if user_id == '07885':
        return "系统管理员"
    con = conn.MysqlConn()
    sql = f"select NAME from HRM_EMPLOYEE_INFO where EMPLOYEE_NUMBER = '{user_id}' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    else:
        return ""


# 获取订单日期
def get_curr_date(flag):
    return datetime.datetime.now().strftime('%Y%m%d') if flag == 1 else datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_lbl_value_list():
    res = {'ERR_MSG': '', 'RES_DATA': {
        'CUST_CODE_LIST': [], 'SO_TYPE_LIST': [], 'MO_TYPE_LIST': [], 'MO_NAME_LIST': [], 'MO_PRIORITY_LIST': [], 'PO_FIELD_LIST': [], 'LGORT_LIST': [], 'TRAD_CUST_LIST': [], 'XB_LGORT_LIST': []}}
    con = conn.HanaConn()
    sql = "SELECT TYPE,LABEL,VALUE,DESCRIPE,ENABLE FROM ZM_CDM_LABEL_VALUE_LOOKUP ORDER BY ID"
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到客户配置信息"
        return res

    for row in results:
        item = {}
        item['type'] = xstr(row[0])
        item['label'] = xstr(row[1])
        item['value'] = xstr(row[2])
        item['descripe'] = xstr(row[3])
        item['disable'] = True if xstr(row[4]) == 'N' else False

        if item['type'] == 'SO_TYPE':
            res['RES_DATA']['SO_TYPE_LIST'].append(item)
        elif item['type'] == 'MO_TYPE':
            res['RES_DATA']['MO_TYPE_LIST'].append(item)
        elif item['type'] == 'CUST_CODE':
            res['RES_DATA']['CUST_CODE_LIST'].append(item)
        elif item['type'] == 'MO_NAME':
            res['RES_DATA']['MO_NAME_LIST'].append(item)
        elif item['type'] == 'MO_PRIORITY':
            res['RES_DATA']['MO_PRIORITY_LIST'].append(item)
        elif item['type'] == 'PO_FIELDS':
            res['RES_DATA']['PO_FIELD_LIST'].append(item)
        elif item['type'] == 'LGORT':
            res['RES_DATA']['LGORT_LIST'].append(item)
            if item['value'][:1] == '4':
                res['RES_DATA']['XB_LGORT_LIST'].append(item)

        elif item['type'] == 'TRAD_CUST_LIST':
            res['RES_DATA']['TRAD_CUST_LIST'].append(item)

    res['RES_DATA']['CURR_DATE'] = get_curr_date(1)
    return res


def get_cust_code_list():
    con = conn.HanaConn()
    cust_code_list = []

    sql = "SELECT DISTINCT CUST_CODE FROM ZM_CDM_PO_TEMPLATE_LIST ORDER BY CUST_CODE"
    results = con.query(sql)
    for row in results:
        result = {}
        result['value'] = xstr(row[0])
        result['label'] = xstr(row[0])

        cust_code_list.append(result)
    return cust_code_list


def get_so_type_list():
    con = conn.HanaConn()
    mo_type_list = []

    sql = "SELECT TYPE_LABEL,TYPE_VALUE,TYPE_DESC,TYPE_FLAG,TYPE_FLAG2,TYPE_FLAG3,TYPE_FLAG4 FROM ZM_CDM_SO_TYPE where TYPE_FLAG = 'Y' ORDER BY TYPE_VALUE "
    results = con.query(sql)
    for row in results:
        result = {}
        result['label'] = xstr(row[0])
        result['value'] = xstr(row[1])
        result['desc'] = xstr(row[2])
        result['disabled'] = True if xstr(row[4]) == 'Y' else False
        result['flag3'] = xstr(row[5])
        result['flag4'] = xstr(row[6])

        mo_type_list.append(result)
    return mo_type_list


def get_mo_type_list():
    con = conn.HanaConn()
    mo_type_list = []

    sql = "SELECT TYPE_LABEL,TYPE_VALUE,TYPE_DESC,TYPE_FLAG,TYPE_FLAG2,TYPE_FLAG3,TYPE_FLAG4 FROM ZM_CDM_MO_TYPE where TYPE_FLAG = 'Y' ORDER BY TYPE_VALUE "
    results = con.query(sql)
    for row in results:
        result = {}
        result['label'] = xstr(row[0])  # 工单类型名
        result['value'] = xstr(row[1])  # SAP工单类型值
        result['desc'] = xstr(row[2])  # 工单类型描述
        result['disabled'] = True if xstr(row[4]) == 'Y' else False  # 是否禁用
        result['flag3'] = xstr(row[5])  # 与订单相关
        result['flag4'] = xstr(row[6])  # 跳过晶圆库存检查

        mo_type_list.append(result)
    return mo_type_list


def get_mo_prefix_list():
    con = conn.HanaConn()
    mo_prefix_list = []

    sql = "SELECT key_name,key_desc FROM ZM_CDM_MO_PREFIX WHERE flag = '1' ORDER BY key_name "
    results = con.query(sql)
    for row in results:
        result = {}
        result['label'] = xstr(row[0])
        result['value'] = xstr(row[0])
        result['desc'] = xstr(row[1])

        mo_prefix_list.append(result)
    return mo_prefix_list


def get_po_template(cust_code):
    if not cust_code:
        return []

    con = conn.HanaConn()
    cust_po_template_list = []
    sql = f"SELECT CUST_CODE,TEMPLATE_FILE,TEMPLATE_PIC ,KEY_LIST,FILE_LEVEL,FILE_URL,ACCEPT,FILE_DESC,TEMPLATE_SN FROM ZM_CDM_PO_TEMPLATE_LIST WHERE CUST_CODE  = '{cust_code}' and flag = '1' order by CREATE_DATE"
    results = con.query(sql)

    for row in results:
        result = {}
        result['template_file_name'] = xstr(row[1])
        result['template_img_url'] = xstr(row[2])
        result['template_file_key_list'] = xstr(row[3])
        result['template_file_level'] = xstr(row[4])
        result['template_file_url'] = xstr(row[5])
        result['template_file_accept_type'] = xstr(row[6])
        result['template_desc'] = xstr(row[7])
        result['template_sn'] = xstr(row[8])
        result['template_show_progress_flag'] = False
        result['template_show_filelist_flag'] = False
        result['template_load_progress'] = 0

        cust_po_template_list.append(result)
    return cust_po_template_list


# SO上传前料号多选一case
def get_product_data(cust_code, sap_product_name):
    product_data = {}
    sap_product_name = sap_product_name.split("||")[1]
    con = conn.HanaConnDW()

    sql = f"SELECT DISTINCT PARTNER FROM VM_SAP_PO_CUSTOMER WHERE ZZYKHH = '{cust_code}'"
    sap_cust_code = xstr(con.query(sql)[0][0]).lstrip('0')

    sql = f'''SELECT DISTINCT ZZKHXH,ZZFABXH,ZZHTXH,ZZJYGD,MATNR,ZZBASESOMO FROM VM_SAP_MAT_INFO WHERE MATNR like '%{sap_product_name}' '''
    results = con.query(sql)
    print('料号变化:', sql)

    if results:
        product_data['customer_device'] = xstr(results[0][0])
        product_data['fab_device'] = xstr(results[0][1])
        product_data['ht_pn'] = xstr(results[0][2])
        product_data['wafer_dies'] = int(xstr(results[0][3]))
        product_data['sap_product_pn'] = xstr(results[0][4]).lstrip('0')
        product_data['wafer_pn'] = ''
        product_data['sap_cust_code'] = sap_cust_code
        product_data['trad_cust_code'] = cust_code
        product_data['base_so'] = xstr(results[0][5])

    return product_data


def query_po_data2(po_query):
    json_data = []

    sql = f'''
        select a.customershortname,b.MPN_DESC,a.lotid,a.substrateid, CASE WHEN c.WAFERID IS null THEN 'N' ELSE 'Y' end,b.WAFER_VISUAL_INSPECT from mappingdatatest a
        INNER JOIN CUSTOMEROITBL_TEST b ON a.FILENAME = to_char(b.id)
        left JOIN IB_WAFERLIST c ON a.SUBSTRATEID  = c.WAFERID  AND a.LOTID  = c.WAFERLOT
        where 1 = 1  '''

    sql = sql + \
        f" and a.lotid = '{po_query['cust_lot_id']}' " if po_query['cust_lot_id'] else sql

    sql = sql + \
        f" and b.wafer_visual_inspect = '{po_query['upload_id']}' " if po_query['upload_id'] else sql

    sql = sql + ' order by substrateid'

    results = conn.OracleConn.query(sql)
    for row in results:
        result = {}
        result['cust_code'] = xstr(row[0])
        result['cust_pn'] = xstr(row[1])
        result['lot_id'] = xstr(row[2])
        result['wafer_id'] = xstr(row[3])
        result['is_worked'] = xstr(row[4])
        result['upload_id'] = xstr(row[5])

        json_data.append(result)
    return json_data


# 查询订单数据
def query_po_data(po_query):
    print(po_query)
    res = {"ERR_MSG": "", "RES_DATA": []}
    con = conn.HanaConn()

    sql = f'''SELECT CUST_CODE,CUSTOMER_DEVICE,PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,MO_ID,SO_ID,SO_ITEM,WAFER_SN,UPLOAD_ID,PO_ID,(PASSBIN_COUNT+FAILBIN_COUNT) as GROSS_DIE,
            CREATE_DATE,CREATE_BY,CDM_ID,CDM_ITEM,WAFER_HOLD,MARK_CODE,ADD_0,ADD_1,FAB_DEVICE,SAP_PRODUCT_PN,ADDRESS_CODE,PO_TYPE,ID
            FROM ZM_CDM_PO_ITEM where FLAG = '1' and 1 = 1  '''

    sql = sql + \
        f" and CUST_CODE = '{po_query['query_value']}' " if po_query['query_type'] == 'M0' else sql

    sql = sql + \
        f" and CUSTOMER_DEVICE = '{po_query['query_value']}' " if po_query['query_type'] == 'M1' else sql

    sql = sql + \
        f" and HT_PN = '{po_query['query_value']}' " if po_query['query_type'] == 'M2' else sql

    sql = sql + \
        f" and PRODUCT_PN = '{po_query['query_value']}' " if po_query['query_type'] == 'M3' else sql

    sql = sql + \
        f" and PO_ID = '{po_query['query_value']}' " if po_query['query_type'] == 'M4' else sql

    sql = sql + \
        f" and (LOT_ID = '{po_query['query_value']}' OR ADD_1 = '{po_query['query_value']}' OR ADD_8='{po_query['query_value']}')  " if po_query['query_type'] == 'M5' else sql

    sql = sql + \
        f" and LOT_WAFER_ID = '{po_query['query_value']}' " if po_query['query_type'] == 'M6' else sql

    sql = sql + \
        f" and UPLOAD_ID = '{po_query['query_value']}' " if po_query['query_type'] == 'M7' else sql

    sql = sql + \
        f" and CREATE_DATE >= '{po_query['query_start_date']}' and CREATE_DATE <= '{po_query['query_end_date']}' " if po_query[
            'query_start_date'] and po_query['query_end_date'] else sql

    sql = sql + ' order by LOT_ID,WAFER_ID,CREATE_DATE desc '
    print(sql)
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到订单数据"
        res['ERR_SQL'] = sql
        return res

    row_len = len(results)

    if row_len > 250:
        row_len = 250

    for i in range(row_len):
        row_data = {}
        row_data['CUST_CODE'] = xstr(results[i][0])
        row_data['CUSTOMER_DEVICE'] = xstr(results[i][1])
        row_data['PRODUCT_PN'] = xstr(results[i][2])
        row_data['LOT_ID'] = xstr(results[i][3])
        row_data['WAFER_ID'] = xstr(results[i][4])
        row_data['LOT_WAFER_ID'] = xstr(results[i][5])
        row_data['MO_ID'] = xstr(results[i][6])
        row_data['SO_ID'] = xstr(results[i][7])
        row_data['SO_ITEM'] = xstr(results[i][8])
        row_data['WAFER_SN'] = xstr(results[i][9])
        row_data['UPLOAD_ID'] = xstr(results[i][10])
        row_data['PO_ID'] = xstr(results[i][11])
        row_data['GROSS_DIE'] = xstr(results[i][12])
        row_data['CREATE_DATE'] = xstr(results[i][13])
        row_data['CREATE_BY'] = xstr(results[i][14])
        row_data['CDM_ID'] = xstr(results[i][15])
        row_data['CDM_ITEM'] = xstr(results[i][16])
        row_data['WAFER_HOLD'] = xstr(results[i][17])
        row_data['MARK_CODE'] = xstr(results[i][18])
        row_data['ADD_0'] = xstr(results[i][19])
        row_data['ADD_1'] = xstr(results[i][20])
        row_data['FAB_DEVICE'] = xstr(results[i][21])
        row_data['SAP_PRODUCT_PN'] = xstr(results[i][22])
        row_data['ADDRESS_CODE'] = xstr(results[i][23])
        row_data['PO_TYPE'] = xstr(results[i][24])
        row_data['REAL_WAFER_ID'] = results[i][25]
        if row_data['REAL_WAFER_ID'] == 1:
            row_data['REAL_WAFER_ID'] = row_data['WAFER_ID']
        else:
            row_data['REAL_WAFER_ID'] = ""

        if row_data['PO_TYPE'] == 'ZOR1':
            row_data['PO_TYPE'] = "样品订单"
        elif row_data['PO_TYPE'] == 'ZOR2':
            row_data['PO_TYPE'] = '小批量订单'
        elif row_data['PO_TYPE'] == 'ZOR3':
            row_data['PO_TYPE'] = '量产订单'
        elif row_data['PO_TYPE'] == 'ZOR4':
            row_data['PO_TYPE'] = '免费订单'
        elif row_data['PO_TYPE'] == 'ZOR5':
            row_data['PO_TYPE'] = 'RMA收费订单'
        elif row_data['PO_TYPE'] == 'ZOR6':
            row_data['PO_TYPE'] = 'RMA免费订单'
        elif row_data['PO_TYPE'] == 'OLD_DATA':
            row_data['PO_TYPE'] = '老系统订单'
        elif row_data['PO_TYPE'] == 'YP10':
            row_data['PO_TYPE'] = 'DC工单'
        elif row_data['PO_TYPE'] == 'YP11':
            row_data['PO_TYPE'] = '玻璃工单'
        elif row_data['PO_TYPE'] == 'YP12':
            row_data['PO_TYPE'] = '硅基工单'
        elif row_data['PO_TYPE'] == 'YP13':
            row_data['PO_TYPE'] = 'FO_CSP工单'

        res["RES_DATA"].append(row_data)

    return res


# 导出订单数据
def export_po_data(po_query):
    res = {"ERR_MSG": "", "RES_DATA": []}
    con = conn.HanaConn()

    sql = f'''SELECT CUST_CODE,CUSTOMER_DEVICE,PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,MO_ID,SO_ID,SO_ITEM,WAFER_SN,UPLOAD_ID,PO_ID,(PASSBIN_COUNT+FAILBIN_COUNT) as GROSS_DIE,
            CREATE_DATE,CREATE_BY,CDM_ID,CDM_ITEM,WAFER_HOLD,MARK_CODE,ADD_0,ADD_1,FAB_DEVICE,SAP_PRODUCT_PN,ADDRESS_CODE,PO_TYPE,ID
            FROM ZM_CDM_PO_ITEM where FLAG = '1' and 1 = 1  '''

    sql = sql + \
        f" and CUST_CODE = '{po_query['query_value']}' " if po_query['query_type'] == 'M0' else sql

    sql = sql + \
        f" and CUSTOMER_DEVICE = '{po_query['query_value']}' " if po_query['query_type'] == 'M1' else sql

    sql = sql + \
        f" and HT_PN = '{po_query['query_value']}' " if po_query['query_type'] == 'M2' else sql

    sql = sql + \
        f" and PRODUCT_PN = '{po_query['query_value']}' " if po_query['query_type'] == 'M3' else sql

    sql = sql + \
        f" and PO_ID = '{po_query['query_value']}' " if po_query['query_type'] == 'M4' else sql

    sql = sql + \
        f" and LOT_ID = '{po_query['query_value']}' " if po_query['query_type'] == 'M5' else sql

    sql = sql + \
        f" and LOT_WAFER_ID = '{po_query['query_value']}' " if po_query['query_type'] == 'M6' else sql

    sql = sql + \
        f" and UPLOAD_ID = '{po_query['query_value']}' " if po_query['query_type'] == 'M7' else sql

    sql = sql + \
        f" and CREATE_DATE >= '{po_query['query_start_date']}' and CREATE_DATE <= '{po_query['query_end_date']}' " if po_query[
            'query_start_date'] and po_query['query_end_date'] else sql

    sql = sql + ' order by LOT_ID,WAFER_ID,CREATE_DATE desc '
    # print(sql)
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到订单数据"
        res['ERR_SQL'] = sql
        return res

    row_len = len(results)
    if row_len > 500:
        res['ERR_MSG'] = "数据量大于500行,无法导出,请缩小导出范围"
        res['ERR_SQL'] = sql
        return res

    file_id = ttx.trans_sql(sql)
    res['HEADER_DATA'] = file_id

    return res


# 修改订单数据
def update_po_data(po_update):
    res = {"ERR_MSG": "", "ERR_SQL": ""}
    con = conn.HanaConn()

    items = po_update.get('items')
    for item in items:
        res = update_data(con, item)
        if res['ERR_MSG']:
            return res

    con.db.commit()
    return res


# 修改订单
def update_data(con, item):
    res = {"ERR_MSG": "", "ERR_SQL": ""}
    print("订单修改", item)
    create_by = item['CREATE_BY'].strip()
    wafer_sn = item['WAFER_SN'].strip()
    mark_code = item['MARK_CODE'].strip()
    wafer_hold = item['WAFER_HOLD'].strip()
    add_0 = item['ADD_0'].strip()
    add_1 = item['ADD_1'].strip()
    address_code = item['ADDRESS_CODE'].strip()
    mo_id = item['MO_ID'].strip()
    wafer_id = item['LOT_WAFER_ID'].strip()

    # 修改出货地址
    if address_code:
        sql = f"UPDATE ZM_CDM_PO_ITEM SET ADDRESS_CODE='{address_code}',UPDATE_BY='{create_by}',UPDATE_DATE=NOW() WHERE WAFER_SN = '{wafer_sn}'"
        if not con.exec_n(sql):
            res['ERR_MSG'] = "订单表更新失败"
            res['ERR_SQL'] = sql
            return res

        if mo_id:
            # 更新工单部分
            sql = f"""UPDATE ZD_LOOKUP_EX SET VALUE = '{address_code}' WHERE id IN (SELECT DISTINCT SHOP_ORDER_BO||','||ID FROM VIEW_CUSTOMER_ITEM
                WHERE SHOP_ORDER = '{mo_id}'
                AND ITEM_NAME = '{wafer_id}') AND SUBID = 'COMP_CODE'
            """
            print(sql)
            if not con.exec_n(sql):
                res['ERR_MSG'] = "工单表更新出货地址失败"
                res['ERR_SQL'] = sql
                return res

            # 判断当前工单剩余片的出货地址是否也改掉
            sql = f"""SELECT DISTINCT VALUE FROM ZD_LOOKUP_EX WHERE id IN (SELECT DISTINCT SHOP_ORDER_BO||','||ID FROM VIEW_CUSTOMER_ITEM
                    WHERE SHOP_ORDER = '{mo_id}') AND SUBID = 'COMP_CODE'
            """
            results2 = con.query(sql)
            if len(results2) == 1:
                sql = f"""UPDATE ZD_LOOKUP_EX SET VALUE = '{address_code}' WHERE id IN (SELECT DISTINCT SHOP_ORDER_BO FROM VIEW_CUSTOMER_ITEM
                WHERE SHOP_ORDER = '{mo_id}') AND subid = 'JOBID'
                """
                print(sql)
                con.exec_n(sql)

    return res


# 删除订单数据
def delete_po_data(po_del):
    print(po_del)
    res = {"ERR_MSG": "", "ERR_SQL": ""}
    items = po_del.get('items')
    header = po_del.get('header')
    for item in items:
        res = delete_data(item, header)
        if res['ERR_MSG']:
            return res

    return res


# 删除订单数据明细
def delete_data(item, header):
    res = {"ERR_MSG": "", "ERR_SQL": ""}
    upload_id = item.get('UPLOAD_ID', '')
    po_id = item.get('PO_ID', '')
    so_sn = item.get('CDM_ID', '')
    so_item_sn = item.get('CDM_ITEM', '')
    lot_id = item.get('LOT_ID', '')
    wafer_id = item.get('WAFER_ID', '')
    wafer_sn = item.get('WAFER_SN', '')
    so_id = item.get('SO_ID', '')
    so_item = item.get('SO_ITEM', '')

    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    # 删除记录
    sql = f"insert into ZM_CDM_PO_ITEM_DELETE select * from zm_cdm_po_item where WAFER_SN = '{wafer_sn}' "
    con.exec_n(sql)

    # 删除原因
    if header:
        sql = f"update zm_cdm_po_item_delete set update_date=NOW(),UPDATE_BY='{header['userName']}删除',STATUS='{header['delReason']}'  where WAFER_SN = '{wafer_sn}'  "
        con.exec_n(sql)

    # 删除PO部分
    sql = f"delete from ZM_CDM_PO_ITEM where WAFER_SN = '{wafer_sn}'"
    if not con.exec_n(sql):
        res['ERR_MSG'] = "订单明细删除错误"
        res['ERR_SQL'] = sql
        return res

    con_dw.exec_n(sql)

    sql = f"SELECT count(1) FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}'"
    if con.query(sql)[0][0] == 0:
        sql = f"delete from ZM_CDM_PO_HEADER where UPLOAD_ID = '{upload_id}' "
        if not con.exec_n(sql):
            res['ERR_MSG'] = "订单表头删除错误"
            res['ERR_SQL'] = sql
            return res

    sql = f"SELECT sum(PASSBIN_COUNT+FAILBIN_COUNT) FROM ZM_CDM_PO_ITEM WHERE CDM_ITEM = '{so_item_sn}'"
    item_qty = con.query(sql)[0][0]

    # 删除SO部分
    # wafer
    sql = f"delete from ZM_CDM_SO_SUB_ITEM where WAFER_SN = '{wafer_sn}'"
    if not con.exec_n(sql):
        res['ERR_MSG'] = "SO WAFERLIST删除错误"
        res['ERR_SQL'] = sql
        return res

    # item
    sql = f"SELECT count(1) FROM ZM_CDM_SO_SUB_ITEM WHERE ITEM_SN = '{so_item_sn}'"
    if con.query(sql)[0][0] == 0:
        sql = f"select * from ZM_CDM_SO_ITEM where CDM_ITEM_SN = '{so_item_sn}' "
        if con.query(sql):
            sql = f"delete from ZM_CDM_SO_ITEM where CDM_ITEM_SN = '{so_item_sn}' "
            if not con.exec_n(sql):
                res['ERR_MSG'] = "CDM SO ITEM删除错误"
                res['ERR_SQL'] = sql
                return res

            if so_id:
                del_dict = {'SO_DATA': [{'HEADER': {'ACTION': 'C', 'BSTKD': po_id, 'HEAD_NO': so_sn}, 'ITEM': [
                    {'ACTION': 'D', 'ITEM_NO': so_item_sn}]}]}

                err_msg = delete_sap_so_data(del_dict)
                if err_msg:
                    res['ERR_MSG'] = err_msg
                    return res

    else:
        if so_id:
            del_dict = {'SO_DATA': [{'HEADER': {'ACTION': 'C', 'BSTKD': po_id, 'HEAD_NO': so_sn}, 'ITEM': [
                {'ACTION': 'C', 'ITEM_NO': so_item_sn, 'KWMENG': item_qty, 'WAFER_LIST': [{'ACTION': 'D', 'ZCUST_LOT': lot_id, 'ZCUST_WAFER_ID': wafer_id}]}]}]}
            # err_msg = delete_sap_so_data(del_dict)
            # if err_msg:
            #     res['ERR_MSG'] = err_msg
            #     return res

    # header
    # sql = f"select count(1) from ZM_CDM_SO_ITEM WHERE SO_SN = '{so_sn}'"
    sql = f"select count(1) from ZM_CDM_PO_ITEM WHERE CDM_ID = '{so_sn}'"
    if con.query(sql)[0][0] == 0:
        sql = f"delete from ZM_CDM_SO_HEADER where SO_SN = '{so_sn}' "
        if not con.exec_n(sql):
            res['ERR_MSG'] = "CDM SO头删除错误"
            res['ERR_SQL'] = sql
            return res

        sql = f"delete from ZM_CDM_SO_ITEM where SO_SN = '{so_sn}' "
        if not con.exec_n(sql):
            res['ERR_MSG'] = "CDM SO ITEM删除错误"
            res['ERR_SQL'] = sql
            return res

        if so_id:
            del_dict = {'SO_DATA': [
                {'HEADER': {'ACTION': 'D', 'BSTKD': po_id, 'HEAD_NO': so_sn}}]}

            err_msg = delete_sap_so_data(del_dict)
            if err_msg:
                res['ERR_MSG'] = err_msg
                return res

    con.db.commit()
    con_dw.db.commit()
    return res


# 发送请求给SAP删除销售订单
def delete_sap_so_data(del_dict):
    res = cw.WS().send(del_dict, 'SD017')
    if res.get('desc'):
        return res.get('desc')

    sap_res_node = res.get('data', {}).get('RETURN', {})
    if isinstance(sap_res_node, dict):
        if sap_res_node.get('MESSAGE') != '成功':
            return sap_res_node.get('MESSAGE')

    return ''


def delete_data_back(flag_, del_id):
    if flag_ == '0':
        sql = f"delete from mappingdatatest where lotid = '{del_id}' "
        conn.OracleConn.exec(sql)
        sql = f"delete from CustomerOItbl_test where source_batch_id = '{del_id}' "
        conn.OracleConn.exec(sql)
        sql = f"delete from TBL_PO_UPLOAD_WAFER_INFO where LOT_ID = '{del_id}' "
        conn.OracleConn.exec(sql)
        sql = f"delete from [ERPBASE].[dbo].[tblmappingData] where lotid = '{del_id}' "
        conn.MssConn.exec(sql)
        sql = f"delete from [ERPBASE].[dbo].[tblCustomerOI] where source_batch_id = '{del_id}' "
        conn.MssConn.exec(sql)
    elif flag_ == '1':
        sql = f"delete from CustomerOItbl_test where to_char(id) in (select filename from mappingdatatest where substrateid = '{del_id}') "
        conn.OracleConn.exec(sql)
        sql = f"delete from mappingdatatest where substrateid = '{del_id}' "
        conn.OracleConn.exec(sql)
        sql = f"delete from [ERPBASE].[dbo].[tblCustomerOI] where CONVERT(char(20),id) in (select filename from [ERPBASE].[dbo].[tblmappingData] where substrateid = '{del_id}') "
        conn.MssConn.exec(sql)
        sql = f"delete from [ERPBASE].[dbo].[tblmappingData] where substrateid = '{del_id}' "
        conn.MssConn.exec(sql)
    elif flag_ == '2':
        sql = f"delete from mappingdatatest where filename in (select to_char(id) from CustomerOItbl_test where wafer_visual_inspect = '{del_id}') "
        conn.OracleConn.exec(sql)
        sql = f"delete from CustomerOItbl_test where wafer_visual_inspect = '{del_id}' "
        conn.OracleConn.exec(sql)
        sql = f"delete from [ERPBASE].[dbo].[tblmappingData] where filename in (select CONVERT(char(20),id)  from [ERPBASE].[dbo].[tblCustomerOI] where wafer_visual_inspect = '{del_id}') "
        conn.MssConn.exec(sql)
        sql = f"delete from [ERPBASE].[dbo].[tblCustomerOI] where wafer_visual_inspect = '{del_id}' "
        conn.MssConn.exec(sql)
        sql = f"delete from TBL_PO_UPLOAD_WAFER_INFO where upload_id = '{del_id}' "
        conn.OracleConn.exec(sql)


def get_cust_po_template_list(query):
    ret_dict = {'STATUS': False, 'ERR_DESC': '', 'ITEM_LIST': []}
    con = conn.HanaConn()
    cust_code = query['cust_code']
    sql = f"select TEMPLATE_SN,FILE_DESC from ZM_CDM_PO_TEMPLATE_LIST where CUST_CODE = '{cust_code}' "
    results = con.query(sql)
    if not results:
        ret_dict['ERR_DESC'] = '查询不到创建记录'
        return ret_dict

    for rs in results:
        item = {}
        item['value'] = rs[0]
        item['label'] = rs[1]
        ret_dict['ITEM_LIST'].append(item)

    ret_dict['STATUS'] = True
    return ret_dict


def get_cust_po_template_items(query):
    res = {'ERR_MSG': '', 'ITEM_LIST': []}
    con = conn.HanaConn()
    template_sn = query['template_sn']

    sql = f"SELECT FILE_LEVEL FROM ZM_CDM_PO_TEMPLATE_LIST WHERE TEMPLATE_SN = '{template_sn}'"
    print(sql)
    file_level = xstr(con.query(sql)[0][0])
    res["FILE_LEVEL"] = file_level

    sql = f'''SELECT FIELD_NAME,FIELD_DESC,FIELD_TYPE,PARSE_METHOD,PARSE_SITE,PARSE_SITE_COL_ROW,FIX_LEN,MIN_LEN,MAX_LEN,IGNORE_CHAR_1,IGNORE_CHAR_2,
    IGNORE_CHAR_3,START_CHAR,END_CHAR,FRONT_FIELD,BEHIND_FIELD,FLAG,NOT_NULL,FIX_STRING,SUB_LEN,REPLACE_OLD_STR,REPLACE_NEW_STR,REPLACE_TIMES,END_SUB_LEN,SUB_LEFT_CHAR,SUB_RIGHT_CHAR
    FROM ZM_CDM_PO_TEMPLATE_ITEM zcpti WHERE id = '{template_sn}'  and flag = '1' order by FIELD_NAME '''
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = '查询不到创建记录'
        res['ERR_SQL'] = sql
        return res

    for rs in results:
        item = {}
        item['FIELD_NAME'] = xstr(rs[0])
        item['FIELD_DESC'] = xstr(rs[1])
        item['FIELD_TYPE'] = xstr(rs[2])
        item['PARSE_METHOD'] = xstr(rs[3])
        item['PARSE_SITE'] = xstr(rs[4])
        item['PARSE_SITE_COL_ROW'] = xstr(rs[5])
        item['FIX_LEN'] = xstr(rs[6])
        item['MIN_LEN'] = xstr(rs[7])
        item['MAX_LEN'] = xstr(rs[8])
        item['IGNORE_CHAR_1'] = xstr(rs[9])
        item['IGNORE_CHAR_2'] = xstr(rs[10])
        item['IGNORE_CHAR_3'] = xstr(rs[11])
        item['START_CHAR'] = xstr(rs[12])
        item['END_CHAR'] = xstr(rs[13])
        item['FRONT_FIELD'] = xstr(rs[14])
        item['BEHIND_FIELD'] = xstr(rs[15])
        item['FLAG'] = xstr(rs[16])
        item['NOT_NULL'] = xstr(rs[17])
        item['FIX_STRING'] = xstr(rs[18])
        item['SUB_LEN'] = xstr(rs[19])
        item['REPLACE_OLD_STR'] = xstr(rs[20])
        item['REPLACE_NEW_STR'] = xstr(rs[21])
        item['REPLACE_TIMES'] = xstr(rs[22])
        item['END_SUB_LEN'] = xstr(rs[23])
        item['SUB_LEFT_CHAR'] = xstr(rs[24])  # 最左边字符
        item['SUB_RIGHT_CHAR'] = xstr(rs[25])  # 最右边字符
        trans_po_col_name(item)
        res['ITEM_LIST'].append(item)

    return res


def trans_po_col_name(item):
    con = conn.HanaConn()
    sql = f"SELECT WO_COL FROM ZM_CDM_PO_TEMPLATE_LOOKUP WHERE REMAKR = 'COMMON' AND CDM_FIELD = '{item['FIELD_NAME']}' "
    results = con.query(sql)
    if results:
        item['FIELD_COL'] = xstr(results[0][0]) + "列"
    else:
        item['FIELD_COL'] = ""


def save_cust_po_template_items(template_items):
    con = conn.HanaConn()
    template_sn = template_items['header']

    for item in template_items['items']:
        sql = f'''update ZM_CDM_PO_TEMPLATE_ITEM set BEHIND_FIELD = '{item['BEHIND_FIELD']}',END_CHAR='{item['END_CHAR']}',END_SUB_LEN='{item['END_SUB_LEN']}',FIELD_DESC='{item['FIELD_DESC']}',
        FIELD_TYPE='{item['FIELD_TYPE']}',FIX_LEN='{item['FIX_LEN']}',FRONT_FIELD='{item['FRONT_FIELD']}',IGNORE_CHAR_1='{item['IGNORE_CHAR_1']}',MAX_LEN='{item['MAX_LEN']}',NOT_NULL='{item['NOT_NULL']}',
        PARSE_METHOD='{item['PARSE_METHOD']}',PARSE_SITE='{item['PARSE_SITE']}',PARSE_SITE_COL_ROW='{item['PARSE_SITE_COL_ROW']}',START_CHAR='{item['START_CHAR']}',SUB_LEN='{item['SUB_LEN']}',
        REPLACE_OLD_STR='{item['REPLACE_OLD_STR']}',REPLACE_NEW_STR='{item['REPLACE_NEW_STR']}',REPLACE_TIMES='{item['REPLACE_TIMES']}',SUB_LEFT_CHAR='{item['SUB_LEFT_CHAR']}',SUB_RIGHT_CHAR='{item['SUB_RIGHT_CHAR']}',
        FLAG='{item['FLAG']}'
        where id = '{template_sn}' and FIELD_NAME='{item['FIELD_NAME']}'
        '''
        print(sql)

        if not con.exec_n(sql):
            return False

    con.db.commit()
    return True


def query_mo_list(mo_query):
    ret = {'ERR_DESC': '', 'DATA': []}

    mo_pn = xstr(mo_query.get('mo_pn'))
    mo_pn_type = xstr(mo_query.get('mo_pn_type'))
    con = conn.HanaConn()
    if not (mo_pn and mo_pn_type):
        ret['ERR_DESC'] = '请输入要查询的机种/物料号/工单号'
        return ret

    sql = f"SELECT CUST_CODE,MO_ID,SAP_MO_ID,CUSTOMER_DEVICE,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,PLAN_START_DATE,PLAN_END_DATE,MO_DC,CREATE_BY,CREATE_DATE,PO_ID FROM ZM_CDM_MO_HEADER WHERE FLAG = '1' and 1=1 "

    sql = sql + \
        f" AND WAFER_PN = '{mo_pn}' " if mo_pn_type == "M0" else sql

    sql = sql + \
        f" AND CUSTOMER_DEVICE = '{mo_pn}' " if mo_pn_type == 'M1' else sql

    sql = sql + \
        f" AND HT_PN = '{mo_pn}' " if mo_pn_type == 'M2' else sql

    sql = sql + \
        f" AND PRODUCT_PN = '{mo_pn}' " if mo_pn_type == 'M3' else sql

    sql = sql + \
        f" AND MO_ID = '{mo_pn}' " if mo_pn_type == 'M4' else sql

    sql = sql + \
        f" AND SAP_MO_ID like '%{mo_pn}%' " if mo_pn_type == 'M5' else sql

    sql = sql + " ORDER BY CREATE_DATE DESC"
    results = con.query(sql)
    if not results:
        ret['ERR_DESC'] = '查询不到工单记录'
        return ret

    for rs in results:
        mo_obj = {'ITEMS': []}
        mo_obj['CUST_CODE'] = xstr(rs[0])
        mo_obj['MO_ID'] = xstr(rs[1])
        mo_obj['SAP_MO_ID'] = xstr(rs[2])
        mo_obj['CUSTOMER_DEVICE'] = xstr(rs[3])
        mo_obj['HT_PN'] = xstr(rs[4])
        mo_obj['PRODUCT_PN'] = xstr(rs[5])
        mo_obj['SAP_PRODUCT_PN'] = xstr(rs[6])
        mo_obj['PLAN_START_DATE'] = xstr(rs[7])
        mo_obj['PLAN_END_DATE'] = xstr(rs[8])
        mo_obj['MO_DC'] = xstr(rs[9])
        mo_obj['CREATE_BY'] = xstr(rs[10])
        mo_obj['CREATE_DATE'] = xstr(rs[11])
        mo_obj['PO_ID'] = xstr(rs[12])

        sql2 = f"SELECT MO_ID,LOT_ID,WAFER_ID,PASSBIN_COUNT +FAILBIN_COUNT,PASSBIN_COUNT,FAILBIN_COUNT,MARK_CODE,WAFER_SN FROM ZM_CDM_PO_ITEM WHERE MO_ID = '{mo_obj['MO_ID']}' "
        results2 = con.query(sql2)
        if results2:
            for rs2 in results2:
                mo_item = {}
                mo_item['MO_ID'] = xstr(rs2[0])
                mo_item['LOT_ID'] = xstr(rs2[1])
                mo_item['WAFER_ID'] = xstr(rs2[2])
                mo_item['GROSS_DIE_QTY'] = xstr(rs2[3])
                mo_item['GOOD_DIE_QTY'] = xstr(rs2[4])
                mo_item['NG_DIE_QTY'] = xstr(rs2[5])
                mo_item['MARK_CODE'] = xstr(rs2[6])
                mo_item['WAFER_SN'] = xstr(rs2[7])
                mo_obj['ITEMS'].append(mo_item)

        ret['DATA'].append(mo_obj)

    return ret


def export_mo_list(mo_query):
    ret = {'ERR_DESC': '', 'DATA': []}

    mo_pn = xstr(mo_query.get('mo_pn'))
    print("测试", mo_query)
    mo_pn_type = xstr(mo_query.get('mo_pn_type'))
    con = conn.HanaConn()

    sql = f"""
        SELECT distinct c.CUST_CODE ,a.MO_ID AS "工单号", a.SAP_MO_ID AS "SAP工单号",c.LOT_ID ,c.LOT_WAFER_ID AS "WAFER_ID" ,c.PASSBIN_COUNT AS "良品DIES" ,
        c.FAILBIN_COUNT AS "不良品DIES",a.CREATE_DATE AS "创建时间",a.CREATE_BY AS "创建人",c.PO_ID ,c.CUSTOMER_DEVICE AS  "客户机种", c.HT_PN AS "厂内机种",c.PRODUCT_PN AS "成品料号",c.SAP_PRODUCT_PN AS "SAP料号"
        FROM ZM_CDM_MO_HEADER a
        INNER JOIN ZM_CDM_MO_ITEM b ON a.MO_ID = b.MO_ID
        INNER JOIN ZM_CDM_PO_ITEM c ON b.WAFER_SN = c.WAFER_SN
        where 1=1

    """

    sql = sql + \
        f" AND a.WAFER_PN = '{mo_pn}' " if mo_pn_type == 'M0' else sql

    sql = sql + \
        f" AND c.CUSTOMER_DEVICE = '{mo_pn}' " if (
            mo_pn_type == 'M1' and mo_pn) else sql

    sql = sql + \
        f" AND c.HT_PN = '{mo_pn}' " if (mo_pn_type == 'M2' and mo_pn) else sql

    sql = sql + \
        f" AND c.PRODUCT_PN = '{mo_pn}' " if (
            mo_pn_type == 'M3' and mo_pn) else sql

    sql = sql + \
        f" AND a.MO_ID = '{mo_pn}' " if (mo_pn_type == 'M4' and mo_pn) else sql

    sql = sql + \
        f" AND a.SAP_MO_ID like '%{mo_pn}%' " if (
            mo_pn_type == 'M5' and mo_pn) else sql

    sql = sql + \
        f" AND a.CREATE_BY = '{mo_query['user_name']}' " if (
            mo_query['export_flag'] == 'true') else sql

    sql = sql + \
        f" and a.CREATE_DATE >= '{mo_query['start_date']}' and a.CREATE_DATE <= '{mo_query['end_date']}' " if mo_query[
            'start_date'] and mo_query['end_date'] else sql

    sql = sql + " ORDER BY a.CREATE_DATE DESC"
    print(sql)
    results = con.query(sql)
    if not results:
        ret['ERR_DESC'] = '查询不到工单记录'
        return ret

    if len(results) > 3000:
        ret['ERR_DESC'] = "导出的数据量过大,请减少查询范围"
        return ret

    file_id = ttx.trans_sql(sql, "工单明细.xlsx")
    print("文件名:", file_id)
    ret['HEADER_DATA'] = file_id
    ret['SQL'] = sql

    return ret


def delete_mo_data(mo_del):
    items = mo_del.get('items')
    del_by = mo_del.get('del_by', '')
    del_reason = mo_del.get('del_reason', '')
    del_cdm = mo_del.get('del_CDM', '')
    response = {'ERR_DESC': ''}
    for item in items:
        err_desc = delete_mo_item(item, del_by, del_reason, del_cdm)
        if err_desc:
            response['ERR_DESC'] = '工单：'+item.get('MO_ID')+'=>'+err_desc
            return response

    return response


def del_mes_mo(mo_id):
    send_msg = {"HEADER": {"TXID": "20210128093620000004", "ACTION": "ORDER_RELEASE",
                           "TXTIME": "2021-01-28 09:36:20", "ACTION_ID": "D"}, "BODY": {"SHOP_ORDER": mo_id}}
    send_msg = json.dumps(send_msg)

    url = "http://10.160.2.30:9090/psb.web/api/v1/shopOrders/shopOrderCriteria"
    headers = {"Content-Type": "application/json"}

    try:
        res_data = requests.post(
            url, data=send_msg, headers=headers, timeout=(1, 5)).text
        res_dict = json.loads(res_data)
        status = res_dict.get('header', {}).get('code')
        if status != 0:
            err_msg = res_dict.get('header', {}).get('message')
            return err_msg

    except requests.exceptions.RequestException as e:
        print("工单删除请求异常", e)
        return ''

    return ''


def delete_mo_item(item, del_by, del_reason, del_cdm):
    print("删除测试:", del_cdm)
    mo_id = item.get('MO_ID')
    sap_mo_id = item.get('SAP_MO_ID')
    delete_by = del_by
    delete_reason = del_reason

    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    # 1.先删除MES
    mes_del_res = del_mes_mo(mo_id)
    if mes_del_res:
        return mes_del_res

    # 2.再删除CDM

    # 更新订单项
    sql = f"UPDATE ZM_CDM_PO_ITEM SET FLAG2='0',MO_ID='',MO_ITEM='' WHERE MO_ID = '{mo_id}' "
    if not con.exec_n(sql):
        con.db.rollback()
        print("订单表更新失败")
        return '订单表更新失败'

    # 删除工单头表
    sql = f"UPDATE ZM_CDM_MO_HEADER SET FLAG = '0',REMARK2='{delete_reason}',UPDATE_BY = '{delete_by}', UPDATE_DATE = NOW()  WHERE MO_ID = '{mo_id}' "
    if not con.exec_n(sql):
        print("工单头表删除失败")
        return '工单头表删除失败'

    # 删除工单<->库存表
    sql = f"delete from ZM_CDM_INVENTORY_POINTER where mo_id = '{mo_id}' "
    con.exec_n(sql)

    sql = f"delete from ZM_CDM_INVENTORY_POINTER where mo_id = '{mo_id}' "
    con_dw.exec_n(sql)

    # 删除工单明细表
    sql = f"UPDATE ZM_CDM_MO_ITEM SET FLAG = '0',FLAG2=ID WHERE MO_ID = '{mo_id}' "
    if not con.exec_n(sql):
        print("工单头表删除失败")
        return '工单头表删除失败'

    # 3.最后删除SAP
    # 判断在SAP是否删除掉
    sql = f"SELECT * FROM AUFK a WHERE LOEKZ ='X' AND AUFNR = '{sap_mo_id}' "
    results = con_dw.query(sql)
    if not results:
        # 删除SAP工单
        input = {
            "MO_DATA": [
                {
                    "HEADER": {
                        "SHOP_ORDER": mo_id,
                        "ACTION_ID": "D",
                        "CREATOR": delete_by,
                    }
                }
            ]
        }

        print('删除JSON:', json.dumps(input))
        action = cw.WS().send(input, 'PP009')
        if not action['status']:
            print(action['desc'])
            return action['desc']

        output = action['data']
        print("SAP删除返回", output)
        if not 'RETURN' in output:
            print('SAP工单删除接口返回值错误')
            return 'SAP工单删除接口返回值错误'

        return_node = output['RETURN']
        if return_node.get('TYPE') != 'S':
            print('SAP工单删除失败')
            return return_node.get('MESSAGE', 'SAP工单删除失败')

    # commit
    con.db.commit()
    return ''


def update_mo_data(mo_update):
    items = mo_update.get('items')
    response = {'ERR_DESC': ''}
    for item in items:
        err_desc = update_mo_item(item)
        if err_desc:
            response['ERR_DESC'] = '工单：'+item.get('MO_ID')+'=>'+err_desc
            return response
    return response


def update_mo_item(item):
    con = conn.HanaConn()
    mo_id = xstr(item.get('MO_ID'))
    po_id = xstr(item.get('PO_ID'))
    plan_start_date = xstr(item.get('PLAN_START_DATE'))
    plan_end_date = xstr(item.get('PLAN_END_DATE'))
    delete_by = xstr(item.get('CREATE_BY'))
    cust_device = xstr(item.get('CUSTOMER_DEVICE'))

    # 更新工单头
    sql = f"UPDATE ZM_CDM_MO_HEADER SET PO_ID = '{po_id}',PLAN_START_DATE='{plan_start_date}',PLAN_END_DATE='{plan_end_date}',CUSTOMER_DEVICE='{cust_device}',UPDATE_DATE = NOW(),UPDATE_BY = '{delete_by}'  WHERE MO_ID = '{mo_id}' "
    if not con.exec_n(sql):
        print("工单头更新失败")
        return '工单头更新失败'

    # 更新订单PO
    sql = f"UPDATE ZM_CDM_PO_ITEM  SET PO_ID = '{po_id}',CUSTOMER_DEVICE='{cust_device}' WHERE WAFER_SN IN (SELECT WAFER_SN FROM ZM_CDM_MO_ITEM WHERE MO_ID = '{mo_id}') "
    if not con.exec_n(sql):
        con.db.rollback()
        print("订单PO修改失败")
        return '订单PO修改失败'

    # 发送请求给SAP
      # 删除SAP工单
    input = {
        "MO_DATA": [
            {
                "HEADER": {
                    "SHOP_ORDER": mo_id,
                    "ACTION_ID": "C",
                    "CREATOR": delete_by,
                    "PO": po_id,
                    "PLAN_START_DATE": plan_start_date,
                    "PLAN_END_DATE": plan_end_date,
                    "EXTRA_PROPERTY": [{"NAME": "CUST_PART_NUM1", "VALUE": cust_device}]
                }
            }
        ]
    }

    print('修改JSON', json.dumps(input))
    action = cw.WS().send(input, 'PP009')
    if not action['status']:
        print(action['desc'])
        return action['desc']

    output = action['data']
    print("SAP修改返回", output)
    if not 'RETURN' in output:
        print('SAP工单修改接口返回值错误')
        return 'SAP工单修改接口返回值错误'

    return_node = output['RETURN']
    if return_node.get('TYPE') != 'S':
        print('SAP工单修改失败')
        return return_node.get('MESSAGE', 'SAP工单修改失败')

    # commit
    con.db.commit()
    return ''


# 查询客户机种组名
def get_cust_device_group_name(cust_device):
    res = {'ERR_MSG': '', 'GROUP_NAME': ''}
    if not cust_device:
        return res
    con = conn.HanaConnDW()
    sql = f"SELECT distinct KEY1 FROM ZM_CONFIG_TYPE_LIST a WHERE CONFIG_TYPE = '1' AND KEY2 = '{cust_device}' "
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = f"客户机种{cust_device}在系统中找不到对应的客户机种组,请联系NPI确认并维护正确的客户机种组"
        res['ERR_SQL'] = sql
        return res

    if len(results) > 1:
        res['ERR_MSG'] = f"客户机种{cust_device}在系统中找到多个客户机种组,请联系NPI确认并维护唯一的客户机种组"
        res['ERR_SQL'] = sql
        return res

    res['GROUP_NAME'] = xstr(results[0][0])
    return res


# 物料对照表
def get_product_info(product_name, product_name_type):
    con = conn.HanaConnDW()
    ret = {'ERR_DESC': '', 'ITEMS': []}

    # 客户机种=>客户机种组名
    if product_name_type == 'P1':
        res = get_cust_device_group_name(product_name)
        if res['ERR_MSG']:
            ret['ERR_DESC'] = res['ERR_MSG']
            return ret

        product_name = res['GROUP_NAME']

    # 查询SQL
    sql = ''' SELECT DISTINCT aa.ZZKHXH,aa.ZZFABXH,aa.ZZHTXH,aa.ZZCNLH,aa.ZZJYGD,aa.MATNR,aa.ZZPROCESS,aa.ZZEJDM FROM VM_SAP_MAT_INFO aa INNER JOIN
                (SELECT ZZCNLH,max(ERSDA) AS ERSDA FROM VM_SAP_MAT_INFO WHERE ZZCNLH NOT LIKE '%料号%' AND SUBSTRING(ZZCNLH,LENGTH(ZZCNLH)-2,1) <> 'W' AND LENGTH(ZZCNLH) < 16  AND LENGTH(ZZCNLH) > 12  '''

    sql = sql + \
        f" AND ZZKHXH like '%{product_name}%' " if product_name_type == 'P1' else sql
    sql = sql + \
        f" AND ZZHTXH like '%{product_name}%' " if product_name_type == 'P2' else sql
    sql = sql + \
        f" AND ZZCNLH like '%{product_name}%' " if product_name_type == 'P3' else sql
    sql = sql + \
        f" AND MATNR like '%{product_name}%' " if product_name_type == 'P4' else sql
    sql = sql + \
        "GROUP BY ZZCNLH) bb ON aa.ZZCNLH = bb.ZZCNLH AND aa.ERSDA = bb.ERSDA  "

    results = con.query(sql)
    if not results:
        ret['ERR_DESC'] = '查询不到物料信息'
        ret['ERR_SQL'] = sql
        return ret

    for rs in results:
        item = {}
        item['CUST_DEVICE'] = xstr(rs[0])
        item['FAB_DEVICE'] = xstr(rs[1])
        item['HT_DEVICE'] = xstr(rs[2])
        item['HT_PRODUCT_ID'] = xstr(rs[3])
        item['GROSS_DIES'] = int(xstr(rs[4]))
        item['SAP_PRODUCT_ID'] = xstr(rs[5]).lstrip('0')
        item['PROCESS'] = xstr(rs[6])
        item['CODE'] = xstr(rs[7])

        ret['ITEMS'].append(item)

    return ret


# 查询客户工单自定义模板
def get_cust_mo_attr(mo_query):
    res = {'ERR_MSG': '', 'ITEM_LIST': []}
    con = conn.HanaConn()
    cust_code = mo_query['cust_code']
    mo_level = mo_query['mo_level']

    sql = f"SELECT distinct CUST_CODE ,SAP_ATTR_NAME ,CDM_ATTR_NAME ,ATTR_DESC,TYPE_FLAG  FROM ZM_CDM_MO_FIELDS WHERE CUST_CODE = '{cust_code}'  AND TYPE_FLAG  <> '0' "

    sql = sql + \
        f" AND TYPE_FLAG = '{mo_level}' " if mo_level else sql

    sql = sql + \
        f" ORDER BY TYPE_FLAG "

    print(sql)
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = '查询不到创建记录'
        res['ERR_SQL'] = sql
        return res

    for rs in results:
        item = {}
        item['CUST_CODE'] = xstr(rs[0])
        item['SAP_ATTR_NAME'] = xstr(rs[1])
        item['CDM_ATTR_NAME'] = xstr(rs[2])
        item['ATTR_DESC'] = xstr(rs[3])
        item['TYPE_FLAG'] = 'WAFER层级' if xstr(rs[4]) == '3' else '工单层级'

        res['ITEM_LIST'].append(item)

    return res


# 新增工单属性
def new_cust_mo_attr(mo_new):
    print(mo_new)
    res = {'ERR_MSG': ''}
    con = conn.HanaConn()
    sql = f"INSERT INTO ZM_CDM_MO_FIELDS(CUST_CODE,SAP_ATTR_NAME,CDM_ATTR_NAME,ATTR_DESC,TYPE_FLAG) VALUES('{mo_new['item']['custCode']}','{mo_new['item']['mesAttrName']}','{mo_new['item']['cdmFieldName']}','{mo_new['item']['fieldDesc']}','{mo_new['item']['moLevel']}')"
    if not con.exec_c(sql):
        print("新增工单属性失败")
        res['ERR_MSG'] = "新增工单属性失败"
        res['ERR_SQL'] = sql
        return res

    return res


# 库存数据
def get_product_inv(lot_id, matnr_id, wafer_id, outbox_id, inv_type):
    res = {'ERR_MSG': '', 'DATA': []}

    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    if inv_type == "可开库存":
        # 已耗用量必须等于0,否则不显示在可开库存  m.MTART <> 'Z015'
        sql = f"""SELECT distinct b.MATNR,a.ZZCNLH ,b.WERKS,  b.LGORT || z.LGOBE ,b.CHARG,b.ZWAFER_LOT,b.ZWAFER_ID,b.ZGROSS_DIE_QTY, b.ZDIE_QTY,b.ZDIE_QTY_GI,(b.ZDIE_QTY-b.ZDIE_QTY_GI),b.ZBIN_NO,b.ERDAT,a.ZOUT_BOX,m.MTART,m.ZZHTXH,b.ZSEQ
        FROM ZKTMM0001 b left JOIN  VH_SAP_STOCK_INFO a on a.CHARG = b.CHARG AND a.ZBIN= b.ZBIN
        INNER JOIN MARA m ON m.MATNR = b.MATNR
        INNER JOIN ( SELECT   max(CHARG) AS MCHARG,ZWAFER_ID FROM ZKTMM0001 WHERE ZWAFER_LOT = '{lot_id}' GROUP BY ZWAFER_ID ) c ON c.MCHARG = b.CHARG AND c.ZWAFER_ID = b.ZWAFER_ID 
        left join  T001L z ON z.LGORT = b.LGORT AND z.WERKS = '1200'
        WHERE b.ZWAFER_LOT = '{lot_id}' and (b.ZDIE_QTY - b.ZDIE_QTY_GI) > 0
        """
        sql = sql + f" AND b.MATNR = '{matnr_id}' " if matnr_id else sql
        sql = sql + f" AND b.ZWAFER_ID = '{wafer_id}' " if wafer_id else sql
        sql = sql + f" AND a.ZOUT_BOX = '{outbox_id}' " if outbox_id else sql
        sql = sql + \
            " order by  b.ZWAFER_ID,(b.ZDIE_QTY-b.ZDIE_QTY_GI) desc,b.CHARG,b.ZSEQ"
    else:
        sql = f"""SELECT distinct b.MATNR,a.ZZCNLH ,b.WERKS,   b.LGORT || z.LGOBE ,b.CHARG,b.ZWAFER_LOT,b.ZWAFER_ID,b.ZGROSS_DIE_QTY, b.ZDIE_QTY,b.ZDIE_QTY_GI,(b.ZDIE_QTY-b.ZDIE_QTY_GI),b.ZBIN_NO,b.ERDAT,a.ZOUT_BOX,m.MTART,m.ZZHTXH,b.ZSEQ
        FROM ZKTMM0001 b left JOIN  VH_SAP_STOCK_INFO a on a.CHARG = b.CHARG AND a.ZBIN= b.ZBIN
        INNER JOIN MARA m ON m.MATNR = b.MATNR
        left join  T001L z ON z.LGORT = b.LGORT AND z.WERKS = '1200'
        WHERE b.ZWAFER_LOT = '{lot_id}'  and (b.ZDIE_QTY - b.ZDIE_QTY_GI) > 0
        """
        sql = sql + f" AND b.MATNR = '{matnr_id}' " if matnr_id else sql
        sql = sql + f" AND b.ZWAFER_ID = '{wafer_id}' " if wafer_id else sql
        sql = sql + f" AND a.ZOUT_BOX = '{outbox_id}' " if outbox_id else sql
        sql = sql + \
            " order by  b.ZWAFER_ID,(b.ZDIE_QTY-b.ZDIE_QTY_GI) desc,b.CHARG,b.ZSEQ"

    print(sql)
    results = con_dw.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到物料库存"
        return res

    items = len(results)
    if items > 500:
        items = 500

    for i in range(items):
        item = {}
        item['_MATNR'] = xstr(results[i][0])
        item['MATNR'] = int(xstr(results[i][0]))
        item['ZZCNLH'] = xstr(results[i][1])
        item['WERKS'] = xstr(results[i][2])
        item['LGORT'] = xstr(results[i][3])
        item['CHARG'] = xstr(results[i][4])
        item['ZWAFER_LOT'] = xstr(results[i][5])
        item['ZWAFER_ID'] = xstr(results[i][6])
        item['ZGROSS_DIE_QTY'] = xstr(results[i][7])
        item['ZDIE_QTY'] = xstr(results[i][8])
        item['ZDIE_QTY_GI'] = xstr(results[i][9])
        item['ZDIE_QTY_RM'] = xstr(results[i][10])
        item['ZBIN_NO'] = xstr(results[i][11])
        item['ERDAT'] = xstr(results[i][12])
        item['ZOUT_BOX'] = xstr(results[i][13])
        item['ZSEQ'] = xstr(results[i][16])
        item['ZWAFER_ID_NEW'] = ""

        # if inv_type == "可开库存" and item['ZWAFER_ID']:
        #     if check_mo_exist(con_ha, item['ZWAFER_ID'], xstr(results[i][0])):
        #         continue

        # 库存是否被绑定,如果绑定则跳过
        if check_wafer_inv_binding(con, item):
            print("已绑定工单")
            continue

        if xstr(results[i][14]) == "Z019":
            item['MTART'] = "Z019-晶圆"
        elif xstr(results[i][14]) == "Z013":
            item['MTART'] = "Z013-半成品"
        elif xstr(results[i][14]) == "Z015":
            item['MTART'] = "Z015-成品"
        else:
            item['MTART'] = ""

        item['ZZHTXH'] = xstr(results[i][15])

        res['DATA'].append(item)

    if len(res['DATA']) == 0:
        res['ERR_MSG'] = "查询不到物料库存"
        return res

    return res


# 查询库存是否被其他工单绑定 ,如被占用返回True
def check_wafer_inv_binding(con, item):
    sql = f"SELECT * FROM ZM_CDM_INVENTORY_POINTER zcip WHERE MANDT = '900' AND WERKS = '1200' AND CHARG = '{item['CHARG']}' AND MATNR = '{item['_MATNR']}' AND ZSEQ = '{item['ZSEQ']}' "
    print(sql)
    results = con.query(sql)
    if results:
        return True
    else:
        return False


# 判断晶圆库存是否被开过工单
def check_mo_exist(con, zwafer_id, matnr):
    sql = f"SELECT id FROM ZD_LOOKUP_EX zle WHERE id LIKE '%{zwafer_id}%' AND SUBID = 'WAFER_MAT_NO' AND VALUE = '{matnr}'"
    results = con.query(sql)
    if not results:
        return False

    mo_id = results[0][0].split(',')[1]
    sql = f"select * from zm_cdm_mo_header where mo_id = '{mo_id}' and flag = '1' "
    results = con.query(sql)
    if results:
        return True
    else:
        return False


# 订单片号数据
def get_po_wafer_info(lot_id, product_id):
    print("测试", lot_id, product_id)
    res = {'ERR_MSG': '', 'DATA': []}
    con = conn.HanaConn()

    sql = f"select distinct lot_wafer_id,sap_product_pn from ZM_CDM_PO_ITEM WHERE LOT_ID = '{lot_id}' AND FLAG = '1' AND FLAG2 = '0' AND ID = 1  "
    if product_id:
        sql = sql + f" AND PRODUCT_PN = '{product_id}'"

    results = con.query(sql)
    if results:
        # 客户lot
        for row in results:
            wafer_id = xstr(row[0])
            matnr = xstr(row[1])
            res['DATA'].append({"ZWAFER_ID": wafer_id, "MATNR": matnr})

    else:
        # fablot
        sql = f"select distinct ADD_1 || wafer_id,sap_product_pn from ZM_CDM_PO_ITEM WHERE ADD_1 = '{lot_id}' AND FLAG = '1' AND FLAG2 = '0' AND ID = 1 "
        if product_id:
            sql = sql + f" AND PRODUCT_PN = '{product_id}'"
        results2 = con.query(sql)
        if results2:
            for row in results2:
                wafer_id = xstr(row[0])
                matnr = xstr(row[1])
                res['DATA'].append({"ZWAFER_ID": wafer_id, "MATNR": matnr})
        else:
            sql = f"select distinct ADD_8 || wafer_id,sap_product_pn from ZM_CDM_PO_ITEM WHERE ADD_8 = '{lot_id}' AND FLAG = '1' AND FLAG2 = '0' AND ID = 1 "
            if product_id:
                sql = sql + f" AND PRODUCT_PN = '{product_id}'"
            results2 = con.query(sql)
            for row in results2:
                wafer_id = xstr(row[0])
                matnr = xstr(row[1])
                res['DATA'].append({"ZWAFER_ID": wafer_id, "MATNR": matnr})

    if not res['DATA']:
        abort(make_response({"ERR_MSG": "查询不到订单片号数据"}))

    return res


# 获取打标码
def get_wafer_mark_code(lot_id, lot_id_type):
    print(lot_id, lot_id_type)
    res = {'ERR_MSG': '', 'DATA': []}
    con = conn.HanaConn()

    if lot_id_type == 'P1':
        sql = f''' SELECT  CUST_CODE,HT_PN,LOT_ID,WAFER_ID,WAFER_SN,MARK_CODE,MO_ID  FROM ZM_CDM_PO_ITEM WHERE LOT_ID = '{lot_id}' AND FLAG = '1'
            '''
    else:
        sql = f''' SELECT  CUST_CODE,HT_PN,LOT_ID,WAFER_ID,WAFER_SN,MARK_CODE,MO_ID  FROM ZM_CDM_PO_ITEM WHERE MO_ID = '{lot_id}' AND FLAG = '1'
            '''

    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到订单信息"
        return res

    ht_pn = xstr(results[0][1])
    res['HEADER'] = {"HT_PN": ht_pn}

    # 获取打标码规则
    con_or = conn.OracleConn()
    sql = f"SELECT REMARK,DESCRIBE FROM TBL_MARKINGCODE_REP WHERE HT_PN = '{ht_pn}' "
    results2 = con_or.query(sql)
    if results2:
        remark = xstr(results2[0][0])
        desc = xstr(results2[0][1])
    else:
        remark = ''
        desc = "未查询到维护的打标码规则"

    items = len(results)
    if items > 500:
        items = 500

    for i in range(items):
        item = {}

        item['CUST_CODE'] = xstr(results[i][0])
        item['HT_PN'] = xstr(results[i][1])
        item['LOT_ID'] = xstr(results[i][2])
        item['WAFER_ID'] = xstr(results[i][3])
        item['WAFER_SN'] = xstr(results[i][4])
        item['MARK_CODE'] = xstr(results[i][5])
        item['MO_ID'] = xstr(results[i][6])
        item['MARK_CODE_RULE'] = remark
        item['MARK_CODE_DESC'] = desc

        res['DATA'].append(item)

    return res


def update_wafer_mark_code(mark_data):
    print(mark_data)
    # return ''
    res = {'ERR_MSG': ''}

    chg_flag = mark_data['header']['radio']
    ht_pn = mark_data['header']['htPN']
    if chg_flag == 'mo':
        mo_id = mark_data['header']['lotID']
        smc.set_marking_code_mo(ht_pn, mo_id)
    else:
        # 订单
        for item in mark_data['items']:
            wafer_sn = item['WAFER_SN']
            smc.set_marking_code_po(ht_pn, wafer_sn)

    return res


# 上传SO文件返回
def import_so_file(so_file):
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    # 文件名
    doc_file_name = so_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        so_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=0, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg = {"ERR_MSG": f"文件读取失败:{e}"}
        abort(make_response(err_msg))

    items = []
    for index, row in df.iterrows():
        item = {}
        if not 'LOTID' in row:
            err_msg = {"ERR_MSG": "订单模板错误,没有 LOTID 这一列"}
            abort(make_response(err_msg))

        item['LOTID'] = row['LOTID']

        if not 'WAFERID' in row:
            err_msg = {"ERR_MSG": "订单模板错误,没有 WAFERID 这一列"}
            abort(make_response(err_msg))

        item['WAFERID'] = row['WAFERID']
        item['wafer_id_str'] = item['WAFERID']
        item['WAFERIDLIST'] = get_wafer_id_list(item)
        item['WAFERQTY'] = len(item['WAFERIDLIST'])
        item['WAFERIDLIST_STR'] = ','.join(item['WAFERIDLIST'])
        items.append(item)

    return {"ERR_MSG": "", "ITEMS": items}


def import_so_file_2(so_file):
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    # 文件名
    doc_file_name = so_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        so_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=0, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg = {"ERR_MSG": f"文件读取失败:{e}"}
        abort(make_response(err_msg))

    items = []
    for index, row in df.iterrows():
        item = {}
        if not 'LOTID' in row:
            err_msg = {"ERR_MSG": "订单模板错误,没有 LOTID 这一列"}
            abort(make_response(err_msg))

        item['LOTID'] = row['LOTID']

        if not 'WAFERID' in row:
            err_msg = {"ERR_MSG": "订单模板错误,没有 WAFERID 这一列"}
            abort(make_response(err_msg))

        item['WAFERID'] = row['WAFERID']
        item['wafer_id_str'] = item['WAFERID']
        item['WAFERIDLIST'] = get_wafer_id_list(item)
        item['WAFERQTY'] = len(item['WAFERIDLIST'])
        item['WAFERIDLIST_STR'] = ','.join(item['WAFERIDLIST'])
        items.append(item)

    return {"ERR_MSG": "", "ITEMS": items}


# 获取文件完整的路径
def get_doc_path(doc_dir, doc_file_name):
    doc_path = os.path.join(doc_dir, doc_file_name)
    directory, file_name = os.path.split(doc_path)
    while os.path.isfile(doc_path):
        pattern = '(\d+)\)\.'
        if re.search(pattern, file_name) is None:
            file_name = file_name.replace('.', '(0).')
        else:
            current_number = int(re.findall(pattern, file_name)[-1])
            new_number = current_number + 1
            file_name = file_name.replace(
                f'({current_number}).', f'({new_number}).')
        doc_path = os.path.join(directory + os.sep + file_name)

    return doc_path


# waferlist解析
def get_wafer_id_list(po_item):
    if po_item.get('wafer_id_str') == '1' and po_item.get('wafer_qty') == '25':
        po_item['wafer_id_str'] = ''

    if not po_item.get('wafer_id_str'):
        if po_item.get('wafer_qty'):
            po_item['wafer_id_str'] = '#1-' + po_item.get('wafer_qty')

            if po_item['wafer_qty'] == '25':
                po_item['real_wafer_id'] = 'Y'
            else:
                po_item['real_wafer_id'] = 'N'

        else:
            return []

    wafer_id_str = str(po_item.get('wafer_id_str'))
    wafer_str_new = re.sub(r'[_~-]', ' _ ', wafer_id_str)
    # wafer_str_new = re.sub(r'[~-]', ' _ ', wafer_id_str)
    pattern = re.compile(r'[A-Za-z0-9_]+')
    result1 = pattern.findall(wafer_str_new)

    for i in range(1, len(result1)-1):
        if result1[i] == '_':
            if result1[i-1].isdigit() and result1[i+1].isdigit():
                bt = []
                if int(result1[i-1]) < int(result1[i+1]):
                    for j in range(int(result1[i-1])+1, int(result1[i+1])):
                        bt.append(f'{j}')
                else:
                    for j in range(int(result1[i-1])-1, int(result1[i+1]), -1):
                        bt.append(f'{j}')
                result1.extend(bt)

    wafer_id_list = sorted(set(result1), key=result1.index)
    if '_' in wafer_id_list:
        wafer_id_list.remove('_')

    for i in range(0, len(wafer_id_list)):
        if wafer_id_list[i].isdigit() and len(wafer_id_list[i]) == 1:
            wafer_id_list[i] = ('00' + wafer_id_list[i])[-2:]

    return wafer_id_list


# 获取随机数
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


# 获取ACTION
def get_so_action(con, header):
    # sql = f"SELECT SO_SN FROM ZM_CDM_SO_HEADER WHERE PO_NO='{header['BSTKD']}' AND PO_TYPE='{header['AUART']}' AND CUST_CODE = '{header['KUNNR']}' "
    # results = con.query(sql)
    # if results:
    #     action = "C"
    #     header_no = xstr(results[0][0])

    # else:
    action = "N"
    header_no = get_rand_id(8)

    # 新建SO表头记录
    sql = f'''INSERT INTO ZM_CDM_SO_HEADER(PO_NO,PO_TYPE,SO_SN,SO_CREATE_BY,SO_CREATE_DATE,CUST_CODE,FLAG,PO_UPLOAD_ID)
        values('{header['BSTKD']}','{header['AUART']}','{header_no}','{header['CREATER']}',NOW(),'{header['KUNNR']}','1','{header['UPLOAD_ID']}') '''

    if not con.exec_n(sql):
        abort(make_response({'ERR_MSG': 'SO_HEADER插入失败'}))

    return action, header_no


# 获取SAP客户
def get_sap_cust_code(cust_code):
    con = conn.HanaConnDW()
    sql = f"SELECT DISTINCT PARTNER FROM VM_SAP_PO_CUSTOMER WHERE ZZYKHH = '{cust_code}' "
    results = con.query(sql)
    if results:
        sap_cust_code = xstr(results[0][0])
        return sap_cust_code

    return ''


# 获取sap料号
def get_sap_product_pn(product_pn):
    mat_data = get_mat_master_data(product_no=product_pn)
    if not mat_data:
        abort(make_response({"ERR_MSG": "查询不到物料号, 请输入正确的物料号"}))
    return mat_data[0]['MATNR'], mat_data[0]['ZZKHXH'], mat_data[0]['ZZFABXH'], mat_data[0]['ZZJYGD']


# 手动创建SO
def create_so(so_data):
    # print(so_data)

    header = so_data['header']
    items = so_data['items']
    con = conn.HanaConn()
    so_data_list = {'SO_DATA': []}

    # header
    so_data = {'HEADER': {}, 'ITEM': []}
    cust_code = header['CUST_CODE'].strip()
    po_id = header['CUST_PO'].strip()
    creater = header['USER'].strip()
    trad_cust_code = header['TRAD_CUST_CODE'].strip(
    ) if header['TRAD_CUST_CODE'] else cust_code
    po_type = header['SO_TYPE'].strip()
    sap_cust_code = get_sap_cust_code(cust_code)
    trad_sap_cust_code = sap_cust_code if trad_cust_code == cust_code else get_sap_cust_code(
        trad_cust_code)

    header_node = {}
    so_data['HEADER'] = header_node
    header_node['AUART'] = po_type
    header_node['KUNNR'] = sap_cust_code
    header_node['KUNRE'] = trad_sap_cust_code
    header_node['BSTKD'] = po_id
    header_node['UPLOAD_ID'] = get_rand_id(8)
    header_node['CREATER'] = creater
    header_node['ACTION'], header_node['HEAD_NO'] = get_so_action(
        con, header_node)

    product_pn = header['PRODUCT_ID'].strip()
    sap_product_pn, cust_pn, fab_pn, wafer_gross_dies = get_sap_product_pn(
        product_pn)

    gross_dies = int(header['QUANTITY'])
    po_date = header['PO_DATE']

    item = {}
    item['ACTION'] = 'N'
    item['ITEM_NO'] = get_rand_id(6)
    item['BSTDK'] = po_date
    item['BNAME'] = header_node['CREATER']
    item['MATNR'] = sap_product_pn
    item['KWMENG'] = gross_dies
    item['WAFER_LIST'] = []

    # 插入SO行项目表
    sql = f"insert into zm_cdm_so_item(SO_SN,CDM_ITEM_SN,SAP_PRD_ID,PRD_ID,CREATE_BY,CREATE_DATE,QTY,FLAG) VALUES('{header_node['HEAD_NO']}','{item['ITEM_NO']}','{sap_product_pn}','{product_pn}','{creater}',NOW(),{gross_dies},'1')"
    con.exec_n(sql)

    if items:
        for row in items:
            print(row)
            lot_id = row['LOTID'].strip()
            waferlist = row['WAFERIDLIST']
            for wafer_id in waferlist:
                wafer_node = {}

                wafer_node['ACTION'] = 'N'
                wafer_node['ZCUST_LOT'] = lot_id
                wafer_node['ZCUST_WAFER_ID'] = lot_id + wafer_id
                wafer_node['ZCUST_DEVICE'] = cust_pn
                wafer_node['ZFAB_DEVICE'] = fab_pn
                wafer_node['ZGOODDIE_QTY'] = wafer_gross_dies
                item['WAFER_LIST'].append(wafer_node)

    so_data['ITEM'].append(item)

    so_data_list['SO_DATA'].append(so_data)
    # print(so_data_list)

    so_res = get_data_from_web_api("SD017", so_data_list)
    print(so_res)

    if 'RETURN' in so_res['RES_DATA_D']:
        sap_resp = so_res['RES_DATA_D']['RETURN'][0]
        if 'MESSAGE' in sap_resp and sap_resp['MESSAGE'] == '成功':
            so_id = sap_resp.get('VBELN', '')
            so_item = sap_resp.get('POSNR', '')
            so_sn = sap_resp.get('HEAD_NO', '')
            so_item_sn = sap_resp.get('ITEM_NO', '')

            if so_id and so_item and so_item_sn and so_sn:
                # 成功更新
                # 更新SO头表
                sql = f"update ZM_CDM_SO_HEADER set SO_NO = '{so_id}' where SO_SN = '{so_sn}' "
                con.exec_n(sql)

                # 更新SO行表
                sql = f"UPDATE ZM_CDM_SO_ITEM SET SO_ITEM_SN='{so_item}' where SO_SN='{so_sn}' and CDM_ITEM_SN='{so_item_sn}' "
                con.exec_n(sql)

                # 提交事务
                con.db.commit()

    return {'ERR_MSG': "", "RES_DATA": so_res.get('RES_DATA_D')}


# 更新晶圆片号
def update_wafer_id(wafer_data):
    print(wafer_data)
    err_msg = ''
    items = wafer_data['items']
    if items:
        for item in items:
            lot_id = item['LOTID']
            wafer_id_list = item['WAFERIDLIST']
            update_wafer_po_data(lot_id, wafer_id_list)

    return {'ERR_MSG': err_msg}


def update_wafer_po_data(lot_id, wafer_id_list):
    con = conn.HanaConn()

    # 清空片号
    sql = f"update zm_cdm_po_item set flag3 = wafer_sn where  lot_id = '{lot_id}' and flag = '1' and flag2 = '0' and id = 0 "
    if con.exec_n(sql):
        # 清空
        sql = f"update zm_cdm_po_item set id = 0 where lot_id = '{lot_id}' and flag = '1' and flag2 = '0' "
        con.exec_n(sql)

        for row in wafer_id_list:
            wafer_id = row
            lot_wafer_id = lot_id + wafer_id

            sql = f"select wafer_sn from zm_cdm_po_item where lot_id = '{lot_id}' and flag = '1' and flag2 = '0' and  id = 0 "
            results = con.query(sql)
            if not results:
                continue

            wafer_sn = results[0][0]

            sql = f"UPDATE ZM_CDM_PO_ITEM SET lot_wafer_id = '{lot_wafer_id}', wafer_id = '{wafer_id}', id = 1,update_date=now() where wafer_sn = '{wafer_sn}'  "
            con.exec_n(sql)

    con.db.commit()


def update_wafer_inv_data(lot_id):
    con = conn.HanaConn()
    conDW = conn.HanaConnDW()
    # 查询订单
    wafer_id_list = []
    sql = f"SELECT lot_wafer_id FROM ZM_CDM_PO_ITEM WHERE LOT_ID = '{lot_id}' and lot_wafer_id <> ''  and  id = '1'  "
    results = con.query(sql)
    if not results:
        return {"ERR_MSG": "该lot的订单不存在,或需要更新真实订单片号"}

    for row in results:
        wafer_id_list.append(row[0])

    print(wafer_id_list)

    sql = f'''
            SELECT b.ZSEQ,b.MATNR,b.WERKS,b.CHARG,b.ZWAFER_LOT FROM VH_SAP_STOCK_INFO a INNER JOIN
                ZKTMM0001 b on a.CHARG = b.CHARG
                INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = a.MATNR
                WHERE a.ZWAFER_LOT = '{lot_id}' AND c.MTART in ('Z019')
            '''

    results = conDW.query(sql)
    if results:
        w_wafer_qty = len(results)
        r_wafer_qty = len(wafer_id_list)
        cnt = w_wafer_qty if w_wafer_qty < r_wafer_qty else r_wafer_qty
        for i in range(cnt):
            inv_node = {}

            inv_node['FMSYS'] = "CDM"
            inv_node['FMDOCNO'] = "CDM_" + get_rand_id(6)
            inv_node['FMDOCITEM'] = "01"
            inv_node['FMCOUNT'] = "1"
            inv_node['USERID'] = "SYSTEM"
            inv_node['WORKBENCH'] = "CDM"
            inv_node['ACTION_ID'] = "U"
            inv_node['ZSEQ'] = results[i][0]
            inv_node['MATNR'] = results[i][1]
            inv_node['WERKS'] = results[i][2]
            inv_node['CHARG'] = results[i][3]
            inv_node['ZWAFER_LOT'] = results[i][4]
            inv_node['ZWAFER_ID'] = wafer_id_list[i]

            req_node = {"PO_WF_INFO": inv_node}
            get_data_from_web_api("MM138", req_node)
    else:
        results = con.query(
            f"select distinct ADD_1 from zm_cdm_po_item where lot_id = '{lot_id}'")
        if results:
            fab_lot = results[0][0]
            sql = f'''
                SELECT b.ZSEQ,b.MATNR,b.WERKS,b.CHARG,b.ZWAFER_LOT FROM VH_SAP_STOCK_INFO a INNER JOIN
                ZKTMM0001 b on a.CHARG = b.CHARG
                INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = a.MATNR
                WHERE a.ZWAFER_LOT = '{fab_lot}' AND c.MTART in ('Z019')
            '''

            results = conDW.query(sql)
            if results:
                w_wafer_qty = len(results)
                r_wafer_qty = len(wafer_id_list)
                cnt = w_wafer_qty if w_wafer_qty < r_wafer_qty else r_wafer_qty
                for i in range(cnt):
                    inv_node = {}

                    inv_node['FMSYS'] = "CDM"
                    inv_node['FMDOCNO'] = "CDM_" + get_rand_id(6)
                    inv_node['FMDOCITEM'] = "01"
                    inv_node['FMCOUNT'] = "1"
                    inv_node['USERID'] = "SYSTEM"
                    inv_node['WORKBENCH'] = "CDM"
                    inv_node['ACTION_ID'] = "U"
                    inv_node['ZSEQ'] = results[i][0]
                    inv_node['MATNR'] = results[i][1]
                    inv_node['WERKS'] = results[i][2]
                    inv_node['CHARG'] = results[i][3]
                    inv_node['ZWAFER_LOT'] = results[i][4]
                    inv_node['ZWAFER_ID'] = wafer_id_list[i].replace(
                        lot_id, fab_lot)

                    req_node = {"PO_WF_INFO": inv_node}
                    get_data_from_web_api("MM138", req_node)

            else:
                return {"ERR_MSG": "该lot的库存不存在"}
        else:
            return {"ERR_MSG": "该lot的订单不存在"}

    return ''


def get_so_sn(so_id, so_item):
    con = conn.HanaConn()
    sql = f"SELECT SO_SN FROM ZM_CDM_SO_HEADER WHERE SO_NO = '{so_id}' "
    results = con.query(sql)
    if not results:
        abort(make_response("查询不到SO唯一码"))

    so_sn = xstr(results[0][0])

    sql = f"SELECT CDM_ITEM_SN FROM ZM_CDM_SO_ITEM zcsi WHERE SO_SN = '{so_sn}' AND SO_ITEM_SN = '{so_item}'"
    print(sql)
    results = con.query(sql)
    if not results:
        abort(make_response("查询不到SO ITEM唯一码"))

    so_item_sn = xstr(results[0][0])

    return so_sn, so_item_sn


def delete_po_data2(po_del):
    print(po_del)
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    # 判断是否开工单
    sql = f"select * from ZM_CDM_PO_ITEM where mo_id <> '' and  UPLOAD_ID = '{po_del['upload_id']}' "
    results = con.query(sql)
    if results:
        abort(make_response("该票订单已经有部分创建了工单, 必须先删除工单,否则无法整票删除"))

    # 删除SO
    sql = f"SELECT DISTINCT PO_ID,SO_ID,SO_ITEM FROM ZM_CDM_PO_ITEM zcpi WHERE UPLOAD_ID = '{po_del['upload_id']}' "
    results = con.query(sql)
    if results:
        for row in results:
            po_id = xstr(row[0])
            so_id = xstr(row[1])
            so_item = xstr(row[2])

            if not so_id:
                continue

            so_sn, so_item_sn = get_so_sn(so_id, so_item)

            del_dict = {'SO_DATA': [{'HEADER': {'ACTION': 'C', 'BSTKD': po_id, 'HEAD_NO': so_sn}, 'ITEM': [
                {'ACTION': 'D', 'ITEM_NO': so_item_sn}]}]}

            delete_sap_so_data(del_dict)

            # 删除表
            sql = f"delete from ZM_CDM_SO_ITEM where SO_SN = '{so_sn}' AND SO_ITEM_SN = '{so_item}' "
            con.exec_n(sql)

            sql = f"select * from ZM_CDM_SO_ITEM where SO_SN = '{so_sn}' "
            results2 = con.query(sql)
            if not results2:
                sql = f"delete from zm_cdm_so_header where SO_SN = '{so_sn}' "
                con.exec_n(sql)

    # 备份
    sql = f"insert into zm_cdm_po_item_delete select * from ZM_CDM_PO_ITEM where upload_id = '{po_del['upload_id']}' "
    con.exec_n(sql)

    if po_del.get('header'):
        sql = f"update zm_cdm_po_item_delete set update_date=NOW(),UPDATE_BY='{po_del['header']['userName']}',STATUS='{po_del['header']['delReason']}' WHERE UPLOAD_ID = '{po_del['upload_id']}' and instr(update_by,'删除') = 0 "
        print(sql)
        con.exec_n(sql)

    # 删除订单表
    sql = f"delete from ZM_CDM_PO_ITEM where upload_id = '{po_del['upload_id']}' "
    con.exec_n(sql)

    sql = f"delete from ZM_CDM_PO_ITEM where upload_id = '{po_del['upload_id']}' "
    con_dw.exec_n(sql)

    con.db.commit()
    con_dw.db.commit()

    return {"ERR_MSG": ""}


# 查询BOM数据
def get_product_bom_info(product_name):
    resp = {'ERR_MSG': '', 'ITEMS_DATA': []}
    con = conn.HanaConnDW()

    sql = f"""SELECT MA.MATNR,MA.ZZHTXH ,MA.ZZCNLH ,MK.MAKTX ,SP.POSNR ,MA1.ZZCNLH ,SP.IDNRK,MK1.MAKTX,SP.MENGE,SP.MEINS,
    SK.BMENG ,SP.SORTF,P02.STAGE ,CASE WHEN SP.ALPRF='00' THEN '' ELSE SP.ALPRF END AS "优先级",SP.ALPGR AS "替代群组"
    FROM MARA MA
    INNER JOIN MAKT MK ON MK.MATNR  =MA.MATNR AND MK.SPRAS='1'
    INNER JOIN MAST MT ON MT.MATNR =MA.MATNR AND MT.MATNR =MK.MATNR  AND MT.WERKS ='1200'
    INNER JOIN STKO SK ON SK.STLNR =MT.STLNR 
    INNER JOIN STPO SP ON SP.STLNR =SK.STLNR 
    INNER JOIN MARA MA1 ON MA1.MATNR=SP.IDNRK 
    INNER JOIN MAKT MK1 ON MK1.MATNR =MA1.MATNR  AND MK1.SPRAS='1'
    INNER JOIN ZKTPP0002 P02 ON P02.ARBPL = SP.SORTF AND P02.WERKS ='1200'
    WHERE MT.WERKS ='1200' AND ma.ZZCNLH = '{product_name}'
    ORDER BY MA.MATNR,SP.IDNRK desc, SP.POSNR
    """
    results = con.query(sql)

    # 检查
    if not results:
        resp['ERR_MSG'] = "查询不到物料BOM信息"
        return resp

    # 赋值
    for row in results:
        item = {}
        item['MATNR'] = xstr(row[0])
        item['ZZHTXH'] = xstr(row[1])
        item['ZZCNLH'] = xstr(row[2])
        item['MAKTX'] = xstr(row[3])
        item['POSNR'] = xstr(row[4])
        item['ZZCNLH2'] = xstr(row[5])
        item['IDNRK'] = xstr(row[6])
        item['MAKTX2'] = xstr(row[7])
        item['MENGE'] = xstr(row[8])
        item['MEINS'] = xstr(row[9])
        item['BMENG'] = xstr(row[10])
        item['SORTF'] = xstr(row[11])
        item['STAGE'] = xstr(row[12])

        resp['ITEMS_DATA'].append(item)

    return resp


# 手动上传订单文件
def upload_common_po_file(header_data, header_file):
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    # 文件名
    doc_file_name = header_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)

    # 保存文件
    try:
        header_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    res = parse_common_po_file(header_data, doc_path)
    return res


def parse_common_po_file(header, doc_path):
    print(header, doc_path)

    res = {"ERR_MSG": "", "SUCCESS_MSG": ""}

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        abort(make_response({"ERR_MSG": f"文件读取失败{e}"}))

    con = conn.HanaConn()
    success_cnt = 0
    header['upload_id'] = get_rand_id(8)
    for index, row in df.iterrows():
        if index == 0:
            continue

        if len(row) != 33:
            abort(make_response(
                {"ERR_MSG": f"当前上传文件的列数({len(row)}列)和设定的模板列数(33列)不一致"}))

        item = {}
        item['NO'] = index
        item['PO_ID'] = xstr(row[1])
        item['CUST_CODE'] = xstr(row[2])
        item['ADDRESS_CODE'] = xstr(row[3])
        item['FAB_DEVICE'] = xstr(row[4])
        item['CUSTOMER_DEVICE'] = xstr(row[5])
        item['ADD_0'] = xstr(row[6])
        item['PO_DATE'] = xstr(row[8])
        item['LOT_ID'] = xstr(row[9])
        item['WAFER_ID'] = xstr(row[10])
        item['PO_GOOD_DIE'] = xstr(row[12])
        item['HTXH'] = xstr(row[13])
        item['ADD_1'] = xstr(row[14])
        item['ADD_2'] = xstr(row[15])
        item['ADD_3'] = xstr(row[16])
        item['ADD_4'] = xstr(row[17])
        item['ADD_5'] = xstr(row[18])
        item['ADD_6'] = xstr(row[19])
        item['ADD_7'] = xstr(row[20])
        item['ADD_8'] = xstr(row[21])
        item['ADD_9'] = xstr(row[22])
        item['ADD_10'] = xstr(row[23])
        item['ADD_11'] = xstr(row[24])
        item['ADD_12'] = xstr(row[25])
        item['ADD_13'] = xstr(row[26])
        item['ADD_14'] = xstr(row[27])
        item['ADD_15'] = xstr(row[28])
        item['WAFER_LIST'] = get_wafer_id_list(item)

        # 根据对照表获取其他信息
        get_sap_mat_info(item)

        # 检查数据
        check_po_data(con, header, item)

        # 保存数据
        save_po_data(con, header, item)

        success_cnt = success_cnt + 1

    if not success_cnt:
        abort(make_response({"ERR_MSG":  "文件没有数据,请检查文件"}))

    con.db.commit()
    res["SUCCESS_MSG"] = f"成功维护{success_cnt}笔数据"
    return res


# 根据对照表获取其他信息
def get_sap_mat_info(item):
    mat_info = get_mat_data(customer_device=item['CUSTOMER_DEVICE'])
    if not mat_info:
        # 用厂内料号查询
        mat_info = get_mat_data(product_no=item['HTXH'])
        if not mat_info:
            abort(make_response(
                {"ERR_MSG":  f"订单第{item['NO']}行无法关联出厂内成品料号,请检查订单数据"}))

    if len(mat_info) > 1:
        abort(make_response(
            {"ERR_MSG":  f"订单第{item['NO']}行无法对应出唯一的厂内成品料号,请检查订单数据"}))
    if not item['CUSTOMER_DEVICE']:
        item['CUSTOMER_DEVICE'] = mat_info[0]['ZZKHXH']

    if not item['FAB_DEVICE']:
        item['FAB_DEVICE'] = mat_info[0]['ZZFABXH']
    item['ZZHTXH'] = mat_info[0]['ZZHTXH']
    item['ZZCNLH'] = mat_info[0]['ZZCNLH']
    item['MATNR'] = mat_info[0]['MATNR']
    item['ZZPROCESS'] = mat_info[0]['ZZPROCESS']
    item['ZZEJDM'] = mat_info[0]['ZZEJDM']
    item['ZZJYGD'] = mat_info[0]['ZZJYGD']
    item['ZZBASESOMO'] = mat_info[0]['ZZBASESOMO']
    item['ZZKHDM'] = mat_info[0]['ZZKHDM']
    item['ZZLKHZY1'] = mat_info[0]['ZZLKHZY1']
    item['ZZLKHZY2'] = mat_info[0]['ZZLKHZY2']
    item['ZZLKHZY3'] = mat_info[0]['ZZLKHZY3']
    item['ZZLKHZY4'] = mat_info[0]['ZZLKHZY4']
    item['ZZLKHZY5'] = mat_info[0]['ZZLKHZY5']
    item['ZZLCBZ'] = mat_info[0]['ZZLCBZ']


def check_po_data(con, header, item):
    pass


def save_po_data(con, header, item):
    pass


def get_wafer_list(item):
    wafer_id_str = str(item.get('WAFER_ID'))
    wafer_str_new = re.sub(r'[_~-]', ' _ ', wafer_id_str)
    # wafer_str_new = re.sub(r'[~-]', ' _ ', wafer_id_str)
    pattern = re.compile(r'[A-Za-z0-9_]+')
    result1 = pattern.findall(wafer_str_new)

    for i in range(1, len(result1)-1):
        if result1[i] == '_':
            if result1[i-1].isdigit() and result1[i+1].isdigit():
                bt = []
                if int(result1[i-1]) < int(result1[i+1]):
                    for j in range(int(result1[i-1])+1, int(result1[i+1])):
                        bt.append(f'{j}')
                else:
                    for j in range(int(result1[i-1])-1, int(result1[i+1]), -1):
                        bt.append(f'{j}')
                result1.extend(bt)

    wafer_id_list = sorted(set(result1), key=result1.index)
    if '_' in wafer_id_list:
        wafer_id_list.remove('_')

    for i in range(0, len(wafer_id_list)):
        if wafer_id_list[i].isdigit() and len(wafer_id_list[i]) == 1:
            wafer_id_list[i] = ('00' + wafer_id_list[i])[-2:]

    return wafer_id_list


# 异常上传
def exception_upload(pics, docs, header_data):
    save_exception_upload_files(pics, docs, header_data)

    print(header_data)
    save_exception_data(header_data)
    res = {"ERR_DESC": ""}

    return res


# 保存文件
def save_exception_upload_files(pics, docs, header_data):
    # 文件目录
    doc_dir = "/opt/CDM_PRD/CDM_2.0_WEB/static/file/exception_docs"
    doc_dir_www = "/www/wwwroot/cmp/static/file/exception_docs"
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    if not os.path.exists(doc_dir_www):
        os.makedirs(doc_dir_www)

    header_data['pic_url_list'] = []
    header_data['doc_url_list'] = []

    for pic in pics:
        # 文件名
        doc_file_name = get_rand_id(8) + os.path.splitext(pic.filename)[-1]
        doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
        doc_path_www = get_doc_path(
            doc_dir=doc_dir_www, doc_file_name=doc_file_name)

        print(doc_dir_www)
        # 保存文件
        try:
            # pic.save(doc_path)
            pic.save(doc_path_www)
        except Exception as e:
            abort(make_response({"ERR_MSG": f"截图保存失败:{e}"}))
        header_data['pic_url_list'].append(doc_path_www)

    for doc in docs:
        # 文件名
        doc_file_name = doc.filename
        # doc_file_name = get_rand_id(8) + os.path.splitext(doc.filename)[-1]
        doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
        doc_path_www = get_doc_path(
            doc_dir=doc_dir_www, doc_file_name=doc_file_name)

        # 保存文件
        try:
            # doc.save(doc_path)
            doc.save(doc_path_www)
        except Exception as e:
            abort(make_response({"ERR_MSG": "excel文件保存失败"}))

        header_data['doc_url_list'].append(doc_path_www)


# 保存记录
def save_exception_data(header_data):
    print(header_data)
    con = conn.HanaConn()
    pic_urls = '%%'.join(header_data['pic_url_list'])
    doc_urls = '%%'.join(header_data['doc_url_list'])
    excep_from = header_data.get('excep_from', '')  # 异常发起人
    excep_to = header_data.get('excep_to', '')  # 指定处理人
    excep_grp = "ERP"  # 默认ERP组
    if "," in header_data.get('excep_to_grp', ''):
        excep_to_grp = header_data['excep_to_grp'].split(',')
        excep_grp = excep_to_grp[0]
        excep_to = excep_to_grp[1]
        if excep_to == "PJ":
            excep_to = "潘健"
        elif excep_to == "TW":
            excep_to = "汤威"
        elif excep_to == "LJY":
            excep_to = "李佳音"
        elif excep_to == "LL":
            excep_to = "刘璐"
        elif excep_to == "DH":
            excep_to = "杜昊"
        else:
            excep_to = ""

    excep_type = header_data.get('excep_type', '')  # 异常/需求/其他
    excep_level = header_data.get('excep_level', '')  # 优先级
    sql = f"""INSERT INTO ZM_CDM_EXCEPTION_LIST(REQUEST_ID,REQUEST_DATE,REQUEST_BY,REQUEST_DESC,REQUEST_TYPE,REQUEST_TO,REQUEST_PICS,REQUEST_DOCS,REQUEST_REMARK_1,REQUEST_STATUS,FLAG,REQUEST_REMARK_2)
    VALUES(ZM_CDM_EXCEPTION_LIST_SEQ.NEXTVAL,now(),'{excep_from}','{header_data['excep_desc']}','{excep_type}','{excep_to}','{pic_urls}','{doc_urls}','{excep_level}','0','Y','{excep_grp}')

    """
    print(sql)
    con.exec_c(sql)

    if not header_data['user_name'] in ("07885", "15918", "14526", "21655", "22097", "18120", "25753"):
        # 企业微信提醒
        info = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]异常/需求发起人：{excep_from}" + "\n" + "异常/需求简述:" + \
            header_data['excep_desc'] + "\n" + "指定处理:" + excep_to + "\n"

        info = '{"msgtype":"text","text":{"content":"' + info + '"}}'
        cmd = "curl 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2dcaa6ee-fcc3-4fe8-b67a-1fa487b1cc0e' -H 'Content-Type: application/json' -d  '" + info + "'"
        print(cmd)
        os.system(cmd)


# 查询异常记录
def get_exception_items(header_data):
    header_data = json.loads(header_data)
    res = {"ERR_DESC": ""}

    con = conn.HanaConn()

    # sql = f"""SELECT REQUEST_ID,to_char(REQUEST_DATE ,'YYYY-MM-DD hh24:mi'),REQUEST_BY,REQUEST_DESC,REQUEST_TYPE,REQUEST_TO,replace(REQUEST_PICS,'/www/wwwroot/cmp',''),replace(REQUEST_DOCS,'/www/wwwroot/cmp',''),REQUEST_STATUS,REQUEST_STATUS_REMARK,
    # REQUEST_REMARK_1,REQUEST_REMARK_2,to_char(REQUEST_UPDATE_DATE,'YYYY-MM-DD hh24:mi'), to_char(REQUEST_CLOSED_DATE,'YYYY-MM-DD hh24:mi')
    # FROM  ZM_CDM_EXCEPTION_LIST WHERE FLAG='Y'  """

    sql = f"""SELECT REQUEST_ID,to_char(REQUEST_DATE ,'YYYY-MM-DD hh24:mi'),REQUEST_BY,REQUEST_DESC,REQUEST_TYPE,REQUEST_TO,replace(REQUEST_PICS,'/www/wwwroot/cmp',''),replace(REQUEST_DOCS,'/www/wwwroot/cmp',''),REQUEST_STATUS,REQUEST_STATUS_REMARK,
    REQUEST_REMARK_1,REQUEST_REMARK_2,to_char(
        REQUEST_UPDATE_DATE,'YYYY-MM-DD hh24:mi'), to_char(REQUEST_CLOSED_DATE,'YYYY-MM-DD hh24:mi')
    FROM  ZM_CDM_EXCEPTION_LIST WHERE FLAG='Y'  """

    # IT人员
    if header_data.get('excepGrp'):
        it_grp = header_data['excepGrp'][0]
        it_name = header_data['excepGrp'][1]
        if it_grp == "ERP":
            if it_name == "TW":
                it_name = "汤威"
            elif it_name == "LJY":
                it_name = "李佳音"
            elif it_name == "PJ":
                it_name = "潘健"
            elif it_name == "LL":
                it_name = "刘璐"
            elif it_name == "DH":
                it_name = "杜昊"
            else:
                it_name = ""
        else:
            it_name = ""

        if it_name:
            sql = sql + \
                f" AND REQUEST_TO = '{it_name}' "

    # 异常
    if header_data['radio_2'] == 0:
        sql = sql + \
            f" AND REQUEST_TYPE = '异常' "

    # 需求
    if header_data['radio_2'] == 1:
        sql = sql + \
            f" AND REQUEST_TYPE = '需求' "

    # 需求
    if header_data['radio_2'] == 2:
        sql = sql + \
            f" AND REQUEST_TYPE = '其他' "

    # 待处理
    if header_data['radio'] == 0:
        sql = sql + \
            f" AND REQUEST_STATUS = '0' "

    # 进行中
    if header_data['radio'] == 1:
        sql = sql + \
            f" AND REQUEST_STATUS = '1' "

    # 已完结
    if header_data['radio'] == 2:
        sql = sql + \
            f" AND REQUEST_STATUS = '2' "

    sql = sql + " ORDER BY to_char(REQUEST_DATE ,'YYYY-MM-DD hh24:mi') desc"

    results = con.query(sql)
    if not results:
        res['ERR_DESC'] = "当前没有项目"

     # 总数
    res['TOTAL_QTY'] = len(results)

    # 当前页数量
    limit = int(header_data['pageSize'])
    if header_data['currentPage'] == '1':
        offset = 0
    else:
        offset = (int(header_data['currentPage']) - 1) * limit

    sql = sql + f" LIMIT {limit} OFFSET {offset}"

    print(sql)

    results = con.query(sql)
    res['ITEMS_DATA'] = []
    for row in results:
        obj = {}
        obj['REQUEST_ID'] = row[0]
        obj['REQUEST_DATE'] = xstr(row[1])
        obj['REQUEST_BY'] = xstr(row[2])
        obj['REQUEST_DESC'] = xstr(row[3])
        obj['REQUEST_TYPE'] = xstr(row[4])
        obj['REQUEST_TO'] = xstr(row[5])
        obj['REQUEST_PICS'] = xstr(row[6]).split("%%") if row[6] else None
        obj['REQUEST_DOCS'] = xstr(row[7]).split("%%") if row[7] else None
        obj['REQUEST_STATUS'] = xstr(row[8])
        obj['REQUEST_STATUS_REMARK'] = xstr(row[9])
        obj['REQUEST_REMARK_1'] = xstr(row[10])
        if obj['REQUEST_REMARK_1'] == "H":
            obj['REQUEST_REMARK_1'] = "高"
        elif obj['REQUEST_REMARK_1'] == "M":
            obj['REQUEST_REMARK_1'] = "中"
        elif obj['REQUEST_REMARK_1'] == "L":
            obj['REQUEST_REMARK_1'] = "低"

        obj['REQUEST_REMARK_2'] = xstr(row[11])
        obj['REQUEST_UPDATE_DATE'] = xstr(row[12])
        obj['REQUEST_CLOSED_DATE'] = xstr(row[13])
        res['ITEMS_DATA'].append(obj)

    return res


# 删除异常项
def remove_exception_item(del_data):
    res = {"ERR_DESC": ""}
    con = conn.HanaConn()
    for item in del_data['items']:
        item_id = item['REQUEST_ID']
        sql = f"UPDATE ZM_CDM_EXCEPTION_LIST SET FLAG='N' WHERE REQUEST_ID={item_id} "
        con.exec_c(sql)

    return res


# 更新异常项
def update_exception_item(update_data):
    res = {"ERR_DESC": ""}
    con = conn.HanaConn()

    for item in update_data['items']:
        print(item)

        item_id = item['REQUEST_ID']
        sql = f"UPDATE ZM_CDM_EXCEPTION_LIST SET  REQUEST_DESC= '{item['REQUEST_DESC']}',REQUEST_STATUS='{item['REQUEST_STATUS']}',REQUEST_TO='{item['REQUEST_TO']}',REQUEST_UPDATE_DATE=NOW(),REQUEST_STATUS_REMARK='{item['REQUEST_STATUS_REMARK']}'  WHERE REQUEST_ID={item_id} "
        con.exec_c(sql)

        # 企业微信提醒
        # if not update_data['header']['userName'] in ("07885", "15918", "14526", "21655", "22097", "18120", "25753"):
        if item['REQUEST_STATUS'] == "2":
            info = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]申请人:" + item['REQUEST_BY'] + "\n" + "异常/需求简述:" + item['REQUEST_DESC'] + \
                "\n" + f"IT:{item['REQUEST_TO']}处理完成,请验证：" + \
                "\n" + "IT处理回复:" + item['REQUEST_STATUS_REMARK']

            info = '{"msgtype":"text","text":{"content":"' + info + '"}}'
            cmd = "curl 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2dcaa6ee-fcc3-4fe8-b67a-1fa487b1cc0e' -H 'Content-Type: application/json' -d  '" + info + "'"
            print(cmd)
            os.system(cmd)

    return res


# 导出异常/明细
def export_exception_item(export_data):
    con = conn.HanaConn()

    ret = {'ERR_DESC': '', 'DATA': []}

    sql = f"""SELECT REQUEST_TYPE as "类别",to_char(REQUEST_DATE ,'YYYY-MM-DD hh24:mi') as "需求日期",REQUEST_BY as "需求人",REQUEST_DESC as "描述",REQUEST_TO as "处理人", CASE REQUEST_STATUS WHEN '0' THEN '待处理' WHEN '1' THEN '处理中' WHEN '2' THEN '完结' END AS "状态"  ,REQUEST_STATUS_REMARK as "处理状态说明",
    REQUEST_REMARK_1 as "优先级",to_char(REQUEST_UPDATE_DATE,'YYYY-MM-DD hh24:mi') as "需求更新日期"
    FROM  ZM_CDM_EXCEPTION_LIST WHERE FLAG='Y'  """

    if export_data['header'].get('excepGrp'):
        it_grp = export_data['header']['excepGrp'][0]
        it_name = export_data['header']['excepGrp'][1]
        if it_grp == "ERP":
            if it_name == "TW":
                it_name = "汤威"
            elif it_name == "LJY":
                it_name = "李佳音"
            elif it_name == "PJ":
                it_name = "潘健"
            elif it_name == "LL":
                it_name = "刘璐"
            elif it_name == "DH":
                it_name = "杜昊"
            else:
                it_name = ""
        else:
            it_name = ""

        if it_name:
            sql = sql + \
                f" AND REQUEST_TO = '{it_name}' "

    # 异常
    if export_data['header']['radio_2'] == 0:
        sql = sql + \
            f" AND REQUEST_TYPE = '异常' "

    # 需求
    if export_data['header']['radio_2'] == 1:
        sql = sql + \
            f" AND REQUEST_TYPE = '需求' "

    # 需求
    if export_data['header']['radio_2'] == 2:
        sql = sql + \
            f" AND REQUEST_TYPE = '其他' "

    # 待处理
    if export_data['header']['radio'] == 0:
        sql = sql + \
            f" AND REQUEST_STATUS = '0' "

    # 进行中
    if export_data['header']['radio'] == 1:
        sql = sql + \
            f" AND REQUEST_STATUS = '1' "

    # 已完结
    if export_data['header']['radio'] == 2:
        sql = sql + \
            f" AND REQUEST_STATUS = '2' "

    sql = sql + " ORDER BY to_char(REQUEST_DATE ,'YYYY-MM-DD hh24:mi') desc"

    results = con.query(sql)
    if not results:
        ret['ERR_DESC'] = '查询不到工单记录'
        return ret

    if len(results) > 3000:
        ret['ERR_DESC'] = "导出的数据量过大,请减少查询范围"
        return ret

    file_id = ttx.trans_sql(sql, "异常_需求明细.xlsx")
    print("文件名:", file_id)
    ret['HEADER_DATA'] = file_id
    ret['SQL'] = sql

    return ret


def get_username_by_userid(userid):
    con = conn.MysqlConn()
    sql = f"select NAME,EMPLOYEE_NUMBER from HRM_EMPLOYEE_INFO where EMPLOYEE_NUMBER = '{userid}' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    else:
        return ""


# ----------------------------------------------------------------线边库存模块---------------------------
# 获取库位调拨料号清单
def get_part_id_list(header_data):
    con = conn.HanaConnDW()
    part_id_list = []

    # 指定料号
    specify_part_id = ""
    if header_data.get('productID'):
        sql = f"SELECT MATNR FROM MARA m WHERE ZZCNLH = '{header_data['productID']}'"
        results = con.query(sql)
        if not results:
            abort(make_response({"ERR_DESC": "您输入的物料号不存在"}))

        specify_part_id = xstr(results[0][0])

    # 所有调拨料号
    sql = f"SELECT DISTINCT MATNR FROM MSEG WHERE BWART = '311' AND LGORT ='{header_data['moLocation']}' "
    if specify_part_id:
        sql = sql + f" AND MATNR = '{specify_part_id}' "

    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_DESC": f"{header_data['moLocation']}没有调拨的料号记录"}))

    for row in results:
        part_id = xstr(row[0])
        part_id_list.append(part_id)

    return part_id_list


# 查询线边库存
def get_xb_inv_items_new(header_data):
    res = {"ERR_DESC": "", "ITEMS_DATA": []}

    # 获取库位调拨料号清单
    part_id_list = get_part_id_list(header_data)


def get_xb_inv_items(header_data):
    print(header_data)
    if isinstance(header_data, str):
        header_data = json.loads(header_data)

    res = {"ERR_DESC": "", "ITEMS_DATA": []}
    con = conn.HanaConnDW()

    sql = f"""SELECT INVENTORY_DC,INVENTORY_ID,LOCATION,LOCATION_DESC,PART,b.MAKTX,b.MATNR,UNIT,BALANCE_QTY,ISSUE_QTY,
    COMMIT_QTY,CONSUME_QTY FROM  ZM_INVENTORY_CK a INNER JOIN VM_SAP_MAT_INFO b ON a.PART = b.ZZCNLH  WHERE  a.FLAG = 'Y'
    """

    # 仓位
    if header_data.get('moLocation'):
        sql = sql + \
            f" AND location = '{header_data['moLocation']}' "

    # 料号
    if header_data.get('productID'):
        sql = sql + \
            f" AND PART = '{header_data['productID']}' "

    sql = sql + " ORDER BY PART, CREATE_DATE DESC"

    results = con.query(sql)
    print(sql)
    if not results:
        res['ERR_DESC'] = "查询不到数据, 请确认库存地点是否正确"
        return res

    for row in results:
        item_data = {}

        # 如果不是当天的,则新增一行当天的
        if xstr(row[0]) != datetime.datetime.now().strftime('%Y%m%d'):
            copy_xb_inv_items(con, row, item_data)

        else:
            item_data['INVENTORY_DC'] = xstr(row[0])
            item_data['INVENTORY_ID'] = row[1]
            item_data['LOCATION'] = xstr(row[2])
            item_data['LOCATION_DESC'] = item_data['LOCATION'] + \
                "_" + xstr(row[3])
            item_data['PART'] = xstr(row[4])
            item_data['PART_DESC'] = xstr(row[5])
            item_data['SAP_PART'] = int(xstr(row[6]))
            # 单位
            item_data['UNIT'] = xstr(row[7])
            # 结余量
            item_data['BALANCE_QTY'] = xstr(row[8])
            # 调拨量
            item_data['ISSUE_QTY'] = get_311_data(
                con, item_data['SAP_PART'], item_data['LOCATION'])
            # 盘点量
            item_data['COMMIT_QTY'] = xstr(row[10])
            # 耗用量
            item_data['CONSUME_QTY'] = xstr(row[11])

            # 库存量
            item_data['INV_QTY'] = "5000"

        res['ITEMS_DATA'].append(item_data)
    print(sql)
    return res


def copy_xb_inv_items(con, row, item_data):
    # 库存量
    inv_qty = 50
    # 调拨量
    add_qty = get_311_data(con,  row[6], row[2])

    # 状态切换
    sql = f"update ZM_INVENTORY_CK set flag = 'N' where INVENTORY_ID = {row[1]}  "
    con.exec_c(sql)

    # 新插一笔
    sql = f"""INSERT INTO ZM_INVENTORY_CK(INVENTORY_ID,INVENTORY_DC,LOCATION,LOCATION_DESC,PART,PART_DESC,SAP_PART,UNIT,BALANCE_QTY,ISSUE_QTY,INV_QTY,CREATE_BY,CREATE_DATE,FLAG)
    values(ZM_INVENTORY_CK_SEQ.NEXTVAL,to_char(now(), 'YYYYMMDD'),'{xstr(row[2])}','{xstr(row[2])+"_" + xstr(row[3])}',
           '{xstr(row[4])}','{xstr(row[5])}','{xstr(row[6])}','{xstr(row[7])}',{row[10]},{add_qty},{inv_qty},'07885',NOW(),'Y' )
    """
    con.exec_c(sql)

    print(sql)

    item_data['INVENTORY_DC'] = xstr(row[0])
    # item_data['INVENTORY_ID'] = row[1]

    item_data['LOCATION'] = xstr(row[2])
    item_data['LOCATION_DESC'] = xstr(row[3])
    item_data['PART'] = xstr(row[4])
    item_data['PART_DESC'] = xstr(row[5])
    item_data['SAP_PART'] = int(xstr(row[6]))
    # 单位
    item_data['UNIT'] = xstr(row[7])
    # 结余量
    item_data['BALANCE_QTY'] = xstr(row[10])
    # 调拨量
    item_data['ISSUE_QTY'] = add_qty
    # 盘点量
    item_data['COMMIT_QTY'] = ""
    # 耗用量
    item_data['CONSUME_QTY'] = ""
    # 库存量
    item_data['INV_QTY'] = inv_qty


# 获取当日调拨
def get_311_data(con, sap_part_id, location):
    cur_date = datetime.datetime.now().strftime('%Y%m%d')
    sap_part_id = ("0000000000" + str(sap_part_id))[-18:]
    print(sap_part_id)
    sql = f"SELECT SUM(MENGE),MEINS,LGORT,MATNR FROM MSEG WHERE MATNR ='{sap_part_id}' AND  BWART='311' AND LGORT ='{location}' AND BUDAT_MKPF = '{cur_date}' GROUP  BY MATNR,LGORT,MEINS "
    print(sql)
    results = con.query(sql)
    if results:
        return float(results[0][0])
    else:
        return 0


# 天气从接口获取
def get_new_weather(query_data):
    print(query_data)
    con = conn.HanaConn()
    res_dict = {"LOGIN_TIMES": 0}

    # 登录系统记录
    sql = f"insert into ZM_CDM_LOGIN_HISTORY(LOGIN_USER_ID,LOGIN_USER_NAME,LOGIN_TIME) values('{query_data['user_id']}','{query_data['user_name']}',now())"
    con.exec_c(sql)

    # 天气
    url = "https://devapi.qweather.com/v7/weather/now?location=101190404&key=3c520712fb4e499db8676efb3aa6b4c0"
    headers = {"Content-Type": "application/json"}

    try:
        res_data = requests.get(
            url, headers=headers, timeout=(1, 5)).text

        res_dict = json.loads(res_data)

    except requests.exceptions.RequestException as e:
        print("天气获取接口异常", e)

    # 当日登录次数统计,返回前台
    sql = f"SELECT count(1) FROM ZM_CDM_LOGIN_HISTORY WHERE to_char(LOGIN_TIME,'YYYYMMDD') = TO_CHAR(now(),'YYYYMMDD') "
    results = con.query(sql)
    if results:
        res_dict['LOGIN_TIMES'] = results[0][0]
    # print(res_data)
    return res_dict


# ---------------------------------------------------------------用户权限管理------------------------------
# 获取用户权限
def get_user_rights(query_data):
    res = {"ERR_MSG": "", "MENUS": {
        "TOP_MENU_ID": [], "SUB_MENU_ID": [], "CUR_MENU_ID": []}}
    con = conn.HanaConn()
    user_id = query_data['user_id']
    sub_id = query_data['sub_id']

    # 一级菜单
    sql = f"""
            SELECT DISTINCT b.TOP_MENU_ID FROM ZM_CDM_USER_MENU a
            INNER JOIN ZM_CDM_MENU_LOOKUP b
            ON a.CUR_MENU_ID = b.CUR_MENU_ID 
            WHERE a.USER_ID = '{user_id}'  AND a.FLAG = '1'  order by b.TOP_MENU_ID
    """

    results = con.query(sql)
    for row in results:
        res['MENUS']['TOP_MENU_ID'].append(xstr(row[0]))

    # 二级菜单
    sql = f"""
            SELECT DISTINCT b.SUB_MENU_ID FROM ZM_CDM_USER_MENU a
            INNER JOIN ZM_CDM_MENU_LOOKUP b
            ON a.CUR_MENU_ID = b.CUR_MENU_ID 
            WHERE a.USER_ID = '{user_id}'  AND a.FLAG = '1' order by b.SUB_MENU_ID
    """
    results = con.query(sql)
    for row in results:
        res['MENUS']['SUB_MENU_ID'].append(xstr(row[0]))

    # 当前功能菜单
    sql = f"""
            SELECT DISTINCT b.CUR_MENU_ID FROM ZM_CDM_USER_MENU a
            INNER JOIN ZM_CDM_MENU_LOOKUP b
            ON a.CUR_MENU_ID = b.CUR_MENU_ID 
            WHERE a.USER_ID = '{user_id}'  AND a.FLAG = '1' 
    """

    if sub_id:
        sql = sql + f" AND b.SUB_MENU_ID = '{sub_id}' "

    sql = sql + " ORDER BY b.CUR_MENU_ID "

    results = con.query(sql)
    for row in results:
        res['MENUS']['CUR_MENU_ID'].append(xstr(row[0]))

    return res


# 获取所有功能菜单
def get_menus_options():
    con = conn.HanaConn()
    res = {"ERR_MSG": "", "OPTIONS": [], "OPTIONS2": []}
    sql = "SELECT DISTINCT TOP_MENU_ID,CUR_MENU_GROUP FROM ZM_CDM_MENU_LOOKUP ORDER BY TOP_MENU_ID "
    results = con.query(sql)
    for row in results:
        top_item = {}
        top_item['value'] = xstr(row[0])
        top_item['label'] = xstr(row[1])

        # 用户组list
        res['OPTIONS2'].append(top_item)

        top_item['children'] = []
        sql = f"SELECT DISTINCT CUR_MENU_ID,CUR_MENU_NAME FROM ZM_CDM_MENU_LOOKUP WHERE TOP_MENU_ID='{top_item['value']}' ORDER BY CUR_MENU_ID "
        results2 = con.query(sql)
        for row2 in results2:
            cur_item = {}
            cur_item['value'] = xstr(row2[0])
            cur_item['label'] = cur_item['value'] + '-' + xstr(row2[1])
            top_item['children'].append(cur_item)

        # 菜单组list
        res['OPTIONS'].append(top_item)

    return res


# 新增用户权限
def add_user_rights(data):
    user_id = data['userName']
    menu_id = data['menuID'][-1]

    con = conn.HanaConn()

    # 检查是否已经存在该权限
    sql = f"SELECT * FROM ZM_CDM_USER_MENU zcum WHERE USER_ID = '{user_id}' AND CUR_MENU_ID = '{menu_id}'"
    results = con.query(sql)
    if results:
        abort(make_response({"ERR_MSG": f"{user_id}已经有{menu_id}的权限,无需新增"}))

    # 新增
    sql = f"INSERT INTO ZM_CDM_USER_MENU(USER_ID,CUR_MENU_ID,ID,CREATE_DATE,FLAG) values('{user_id}','{menu_id}',ZM_CDM_USER_MENU_SEQ.nextval,now(), '1')"
    con.exec_c(sql)

    res = {"ERR_MSG": ""}

    return res


# 删除用户权限
def del_user_rights(data):
    user_id = data['userName']
    menu_id = data['menuID'][-1]

    con = conn.HanaConn()

    # 检查是否已经存在该权限
    sql = f"SELECT * FROM ZM_CDM_USER_MENU WHERE USER_ID = '{user_id}' AND CUR_MENU_ID = '{menu_id}'"
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": f"{user_id}没有{menu_id}的权限,无需删除"}))

    # 删除
    sql = f"DELETE FROM ZM_CDM_USER_MENU WHERE USER_ID = '{user_id}' AND CUR_MENU_ID = '{menu_id}' "
    con.exec_c(sql)

    res = {"ERR_MSG": ""}

    return res


# 新增用户组权限
def add_user_group_rights(data):
    user_id = data['userName']
    menu_group_id = data['menuGrp']

    con = conn.HanaConn()

    # 先清除
    sql = f""" DELETE FROM ZM_CDM_USER_MENU WHERE CUR_MENU_ID IN 
        (SELECT CUR_MENU_ID FROM ZM_CDM_MENU_LOOKUP WHERE TOP_MENU_ID = '{menu_group_id}' ) AND USER_ID = '{user_id}' """
    print(sql)
    con.exec_c(sql)

    # 新增
    sql = f"""
        INSERT INTO ZM_CDM_USER_MENU(USER_ID,CUR_MENU_ID,ID,CREATE_DATE,FLAG)
        SELECT '{user_id}',CUR_MENU_ID ,ZM_CDM_USER_MENU_SEQ.nextval,now(),'1' FROM ZM_CDM_MENU_LOOKUP WHERE TOP_MENU_ID = '{menu_group_id}'
    """
    print(sql)
    con.exec_c(sql)

    res = {"ERR_MSG": ""}

    return res


# 删除用户权限
def del_user_group_rights(data):
    user_id = data['userName']
    menu_group_id = data['menuGrp']

    con = conn.HanaConn()

    # 清除
    sql = f""" DELETE FROM ZM_CDM_USER_MENU WHERE CUR_MENU_ID IN 
        (SELECT CUR_MENU_ID FROM ZM_CDM_MENU_LOOKUP WHERE TOP_MENU_ID = '{menu_group_id}' ) AND USER_ID = '{user_id}' """
    # print(sql)
    con.exec_c(sql)

    res = {"ERR_MSG": ""}

    return res


# 拷贝用户权限
def copy_user_rights(query_data):
    con = conn.HanaConn()
    user_name = query_data['user_name']
    user_name_copy = query_data['user_name_copy']

    # 获取权限list
    sql = f"SELECT * FROM ZM_CDM_USER_MENU zcum WHERE USER_ID = '{user_name_copy}' AND FLAG = '1' "
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": f"用户:{user_name_copy}没有可用权限, 无法拷贝"}))

    # 开始复制
    # 先清楚权限
    sql = f"DELETE FROM ZM_CDM_USER_MENU where USER_ID = '{user_name}' "
    con.exec_c(sql)

    sql = f"""INSERT INTO ZM_CDM_USER_MENU(USER_ID,CUR_MENU_ID,ID,CREATE_DATE,FLAG)
    SELECT '{user_name}',CUR_MENU_ID ,ZM_CDM_USER_MENU_SEQ.nextval,now(),FLAG FROM ZM_CDM_USER_MENU WHERE USER_ID = '{user_name_copy}'
    """
    con.exec_c(sql)

    res = {"ERR_MSG": ""}
    return res


# 创建用户
def create_user(query_data):
    con = conn.HanaConn()

    user_name = query_data['user_name']
    user_real_name = get_user_real_name(user_name)

    res = {"ERR_MSG": "", "ITEMS_DATA": []}

    # 判断是否有账号
    sql = f"SELECT * FROM ZM_CDM_USER_INFO WHERE USER_ID = '{user_name}' "
    results = con.query(sql)
    if results:
        abort(make_response(
            {"ERR_MSG": f"账号:{user_name}已建立,请勿再次建立"}))

    # 创建
    sql = f"INSERT INTO ZM_CDM_USER_INFO(ID, USER_ID, USER_NAME, USER_PASSWD ,FLAG ,CREATE_DATE) VALUES(ZM_CDM_USER_INFO_SEQ.NEXTVAL,'{user_name}','{user_real_name}','{user_name}','Y',NOW())"
    con.exec_c(sql)

    return res


# 获取用户权限清单
def get_user_rights_list(query_data):
    con = conn.HanaConn()
    user_name = query_data['user_name']
    user_real_name = get_user_real_name(user_name)
    res = {"ERR_MSG": "", "ITEMS_DATA": [],
           "USER_NAME": user_real_name, "USER_PASSWD": ""}

    # 判断是否有账号
    sql = f"SELECT USER_PASSWD FROM ZM_CDM_USER_INFO WHERE USER_ID = '{user_name}' "
    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"账号:{user_name}-还未建立", "USER_NAME": user_real_name}))

    res['USER_PASSWD'] = xstr(results[0][0])

    # 获取权限list
    sql = f"""SELECT a.USER_ID ,a.CUR_MENU_ID,b.CUR_MENU_NAME,b.CUR_MENU_GROUP,a.ID ,a.FLAG ,to_char(a.CREATE_DATE,'YYYY-MM-DD hh24:mi')
    FROM ZM_CDM_USER_MENU a 
    INNER JOIN ZM_CDM_MENU_LOOKUP b ON a.CUR_MENU_ID = b.CUR_MENU_ID 
    WHERE a.USER_ID = '{user_name}' 
    ORDER BY a.flag desc, a.CUR_MENU_ID 
    """
    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"账号:{user_name}-查询不到权限", "USER_NAME": user_real_name}))

    for row in results:
        item = {}
        item['USER_ID'] = xstr(row[0])
        item['USER_NAME'] = get_user_real_name(item['USER_ID'])
        item['CUR_MENU_ID'] = xstr(row[1])
        item['CUR_MENU_NAME'] = xstr(row[1]) + '-' + xstr(row[2])
        item['CUR_MENU_GROUP'] = xstr(row[3])
        item['ID'] = row[4]
        item['ACTIVE_FLAG'] = xstr(row[5])
        if item['ACTIVE_FLAG'] == '1':
            item['ACTIVE_FLAG'] = "是"
        else:
            item['ACTIVE_FLAG'] = "否"
        item['CREATE_DATE'] = xstr(row[6])
        res['ITEMS_DATA'].append(item)

    return res


# 激活用户权限
def active_user_rights(data):
    res = {"ERR_MSG": ""}
    con = conn.HanaConn()
    for row in data['items']:
        right_id = row['ID']
        # 更新
        sql = f"UPDATE ZM_CDM_USER_MENU SET FLAG = '1' where id = {right_id} "
        print(sql)
        con.exec_c(sql)

    return res


# 禁用用户权限
def frozen_user_rights(data):
    res = {"ERR_MSG": ""}
    con = conn.HanaConn()
    for row in data['items']:
        right_id = row['ID']
        # 更新
        sql = f"UPDATE ZM_CDM_USER_MENU SET FLAG = '0' where id = {right_id} "
        print(sql)
        con.exec_c(sql)

    return res


# --------------------------包装工艺 工单信息, WO导出
# 获取文件上传ID
def get_upload_id_po(query_data):
    con = conn.HanaConn()
    resp = {"ERR_MSG": "", "ITEMS_DATA": []}

    # MO ID
    if query_data['selType'] == 'P1':
        sql = f"SELECT DISTINCT UPLOAD_ID FROM ZM_CDM_PO_ITEM WHERE MO_ID = '{query_data['selText']}' "

    # LOT ID
    elif query_data['selType'] == 'P2':
        sql = f"SELECT DISTINCT UPLOAD_ID FROM ZM_CDM_PO_ITEM WHERE LOT_ID = '{query_data['selText']}' "

    # WAFER ID
    elif query_data['selType'] == 'P3':
        sql = f"SELECT DISTINCT UPLOAD_ID FROM ZM_CDM_PO_ITEM WHERE LOT_WAFER_ID = '{query_data['selText']}' "

    # 查询
    print(sql)
    results = con.query(sql)

    # 检查数据是否存在
    if not results:
        resp['ERR_MSG'] = "查询不到可导出数据"
        return resp

    for row in results:
        upload_id = xstr(row[0])
        if check_file_existed(upload_id, '1'):
            resp['ITEMS_DATA'].append(upload_id)

    # 检查源文件是否存在
    if not resp['ITEMS_DATA']:
        resp['ERR_MSG'] = "源文件不存在"
        return resp

    return resp


def get_upload_id_wo(query_data):
    con = conn.HanaConn()
    resp = {"ERR_MSG": "", "ITEMS_DATA": []}

    # MO ID
    if query_data['selType'] == 'P1':
        sql = f"SELECT DISTINCT UPLOAD_ID FROM ZM_CDM_PO_ITEM WHERE MO_ID = '{query_data['selText']}' "

    # LOT ID
    elif query_data['selType'] == 'P2':
        sql = f"SELECT DISTINCT UPLOAD_ID FROM ZM_CDM_PO_ITEM WHERE LOT_ID = '{query_data['selText']}' "

    # WAFER ID
    elif query_data['selType'] == 'P3':
        sql = f"SELECT DISTINCT UPLOAD_ID FROM ZM_CDM_PO_ITEM WHERE LOT_WAFER_ID = '{query_data['selText']}' "

    print(sql)
    results = con.query(sql)

    # 检查数据是否存在
    if not results:
        resp['ERR_MSG'] = "查询不到可导出数据"
        return resp

    for row in results:
        upload_id = xstr(row[0])
        if check_file_existed(upload_id, '2'):
            resp['ITEMS_DATA'].append(upload_id)

    # 检查源文件是否存在
    if not resp['ITEMS_DATA']:
        resp['ERR_MSG'] = "源文件不存在"
        return resp

    return resp


# 根据文件ID获取远程下载文件
def get_download_file_path(file_id):
    con = conn.HanaConn()
    sql = f"SELECT  FILE_PATH,FILE_NAME,FILE_ABS_PATH FROM ZM_CDM_FILE_ID_LIST WHERE FILE_ID = '{file_id}' "
    results = con.query(sql)
    if results:
        # 文件路径
        file_path = xstr(results[0][0])
        # 文件名
        file_name = xstr(results[0][1])
        # 文件完整路径
        file_abs_path = xstr(results[0][2])

        return file_path, file_name, file_abs_path
    else:
        return "", "", ""


# 根据文件ID获取远程下载文件
def get_download_file_path_2(file_id, flag):
    con = conn.HanaConn()
    if flag == '1':
        # PO
        sql = f"SELECT WPO_REQ_LOG FROM ZM_CDM_PO_HEADER WHERE UPLOAD_ID = '{file_id}' "
    else:
        # WO
        sql = f"SELECT WPO_RES_LOG FROM ZM_CDM_PO_HEADER WHERE UPLOAD_ID = '{file_id}' "

    results = con.query(sql)
    if results:
        # 文件绝对路径
        file_abs_path = xstr(results[0][0])

        # 文件路径
        file_path = os.path.dirname(file_abs_path)

        # 文件名
        file_name = os.path.basename(file_abs_path)

        return file_path, file_name, file_abs_path
    else:
        return "", "", ""


# 检查文件是否存在
def check_file_existed(upload_id, flag):
    con = conn.HanaConn()
    if flag == '1':
        # PO
        sql = f"SELECT WPO_REQ_LOG FROM ZM_CDM_PO_HEADER WHERE UPLOAD_ID = '{upload_id}' "
    else:
        # WO
        sql = f"SELECT WPO_RES_LOG FROM ZM_CDM_PO_HEADER WHERE UPLOAD_ID = '{upload_id}' "

    results = con.query(sql)
    if results:
        # 文件绝对路径
        file_abs_path = xstr(results[0][0])
        if not file_abs_path:
            return False

        if not os.path.exists(file_abs_path):
            return False
        else:
            return True

    else:
        return False


# 查询MES工单
def get_mes_mo_data(query_data):
    print(query_data)
    con = conn.HanaConn()
    resp = {"ERR_MSG": "", "ITEMS_DATA": [], "XL_HEADER": [], "XL_ITEM": []}
    xl_items_data = []

    # MO ID
    if query_data['selType'] == 'P1':
        # 查询LOT
        sql = f"select distinct lot_id from zm_cdm_po_item where mo_id = '{query_data['selText']}' "
        results = con.query(sql)
        if results:
            lot_id = xstr(results[0][0])
        else:
            lot_id = ""

        # 查询MES工单数据
        sql = f"SELECT ID,SUBID,VALUE FROM ZD_LOOKUP_EX WHERE id LIKE '%{query_data['selText']}%' ORDER BY ID,SUBID  "

    else:
        resp['ERR_MSG'] = "请输入工单号查询"
        return resp

    results = con.query(sql)
    if not results:
        resp['ERR_MSG'] = "查询不到工单信息, 请确认工单是否正确"
        return resp

    # LIST
    for i in range(len(results)):
        item = {}
        item['MO_ID'] = xstr(results[i][0]).split(',')[1]
        item['KEY_1'] = xstr(results[i][0])
        item['KEY_2'] = xstr(results[i][1])

        item['ID'] = xstr(results[i][0]).split(',')[-1]
        if '::' in item['ID']:
            item['MO_LEVEL'] = 'WAFER层级'
            item['ID'] = item['ID'].split('::')[0]

            # 判断是LOT层级还是WAFER层级
            if item['ID'] == lot_id:
                item['MO_LEVEL'] = 'LOT层级'
        else:
            item['MO_LEVEL'] = '表头层级'
            item['ID'] = ''

        item['SUBID'] = xstr(results[i][1])
        item['VALUE'] = xstr(results[i][2])

        resp['ITEMS_DATA'].append(item)

        tu_item = (item['MO_ID'], item['MO_LEVEL'],
                   item['ID'], item['SUBID'], item['VALUE'])
        resp['XL_ITEM'].append(tu_item)

    resp['XL_HEADER'] = [('工单号'), ('工单层级'), ('ID'), ('SUBID'), ('VALUE')]

    return resp


# 导出MES工单数据
def export_mes_mo(req_data):
    resp = {"ERR_MSG": "", "XL_ID": ""}
    if not req_data.get('xlHeader'):
        resp['ERR_MSG'] = "没有表头数据,无法导出"
        return resp

    if not req_data.get('xlItems'):
        resp['ERR_MSG'] = "没有表行数据,无法导出"
        return resp

    # 数据
    xl_file_name = f"MES工单{req_data['moID']}数据.xlsx"

    resp['XL_ID'] = ttx.renderData2(
        req_data['xlItems'], req_data['xlHeader'], xl_file_name)

    return resp


# -----------------------------------------------------------------
if __name__ == '__main__':
    a = {'header': {'userName': '07885', 'queryValue': '17c2b474', 'queryType': 'M7',
                    'queryDate': [], 'delReason': 'dw'}, 'upload_id': '17c2b474'}
    delete_po_data2(a)
