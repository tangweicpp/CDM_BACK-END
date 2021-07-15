import conn_db as conn
import uuid
from mm_mat_info import get_mat_master_data
from web_api_client import get_data_from_web_api
import time
import datetime
import requests
import json
from requests.auth import HTTPBasicAuth
import mm_mat_info as mmi


def xstr(s):
    return '' if s is None else str(s).strip()


# 查询AA是否有SO
def get_aa_so(sap_product_id):
    res = {'ERR_MSG': ''}
    con = conn.HanaConn()

    # 根据机种查询SO
    sql = f'''SELECT aa.SO_NO,bb.SO_ITEM_SN,bb.SAP_PRD_ID,bb.PRD_ID,aa.PO_NO FROM ZM_CDM_SO_HEADER aa
            INNER JOIN ZM_CDM_SO_ITEM bb ON bb.SO_SN = aa.SO_SN 
            WHERE bb.SAP_PRD_ID = '{sap_product_id}'  '''

    print(sql)

    results = con.query(sql)
    if not results:
        return None

    res['SO_ID'] = xstr(results[0][0])
    res['SO_ITEM'] = xstr(results[0][1])
    res['SAP_PRODUCT_PN'] = xstr(results[0][2])
    res['PRODUCT_PN'] = xstr(results[0][3])
    res['PO_ID'] = xstr(results[0][4])
    return res


# 查询AA的订单数据
def get_aa_po_data(mo_query):
    print(mo_query)

    con = conn.HanaConn()
    con_or = conn.OracleConn()
    # FO成品料号
    mo_type_2 = mo_query['mo_type_2']
    cust_device = mo_query['cust_device']
    lot_id = mo_query['lot_id']
    print(mo_type_2, lot_id)

    res = {'ERR_MSG': ''}

    # 量产必须维护MPN
    if mo_type_2 == 'P':
        sql = f"SELECT MARKINGCODEFIRST FROM CUSTOMERMPNATTRIBUTES  WHERE PART = '{cust_device}' "
        results = con_or.query(sql)
        if not results:
            res['ERR_MSG'] = f'AA的量产工单机种必须维护好MPN信息,请联系NPI维护=> {cust_device}'
            res['ERR_SQL'] = sql
            return res

    # elif mo_type_2 == 'S':
    #     sql = f"SELECT MARKINGCODEFIRST FROM CUSTOMERMPNATTRIBUTES  WHERE PART = '{cust_device}' "
    #     results = con_or.query(sql)
    #     if not results:
    #         res['ERR_MSG'] = f'AA的量产工单机种必须维护好MPN信息,请联系NPI维护=> {cust_device}'
    #         res['ERR_SQL'] = sql
    #         return res

    # 查询数据
    sql = f'''SELECT DISTINCT aa.CUST_CODE,aa.CUSTOMER_DEVICE,aa.FAB_DEVICE,bb.LOT_ID,cc.LOT_WAFER_ID,cc.PASSBIN_COUNT,cc.FAILBIN_COUNT,cc.WAFER_SN,aa.CUST_FAB_DEVICE_1,bb.ADD_2 
        FROM ZM_CDM_PO_ITEM aa
        INNER JOIN ZM_CDM_PO_ITEM bb ON bb.FAB_DEVICE = aa.FAB_DEVICE 
        INNER JOIN ZM_CDM_PO_ITEM cc ON cc.LOT_ID = bb.LOT_ID 
        WHERE  aa.ADD_8 = 'FEDS' AND aa.FLAG = '1' AND aa.CUSTOMER_DEVICE = '{cust_device}'  and cc.LOT_ID = '{lot_id}'
        AND bb.ADD_8='BC' AND bb.FLAG = '1'
        AND cc.ADD_8 = 'MAP' AND cc.FLAG = '1' 
    '''

    print(sql)
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = '查询不到订单数据'
        res['ERR_SQL'] = sql
        return res

    res['ITEM_LIST'] = []

    for row in results:
        item = {}
        item['CUST_CODE'] = xstr(row[0])
        item['CUST_DEVICE'] = xstr(row[1])
        item['FAB_DEVICE'] = xstr(row[2])
        item['LOT_ID'] = xstr(row[3])
        item['WAFER_ID'] = xstr(row[4])
        item['GOOD_QTY'] = row[5]
        item['NG_QTY'] = row[6]
        item['QTY'] = item['GOOD_QTY'] + item['NG_QTY']
        item['WAFER_SN'] = xstr(row[7])
        item['COO'] = xstr(row[9])

        item['SAP_PRODUCT_PN'] = get_sap_product_pn(
            item['CUST_DEVICE'], item['FAB_DEVICE'])

        print("测试:", item['SAP_PRODUCT_PN'])
        item_so_data = get_aa_so(item['SAP_PRODUCT_PN'])
        if not item_so_data:
            res['ERR_MSG'] = '查询不到SO记录,请确认SO已经建立'
            return res

        item['PO_ID'] = item_so_data['PO_ID']
        item['SO_ID'] = item_so_data['SO_ID']
        item['SO_ITEM'] = item_so_data['SO_ITEM']
        item['PRODUCT_PN'] = item_so_data['PRODUCT_PN']
        item['PART_PRODUCT_PN'] = xstr(row[8])
        item['MARK_CODE'] = get_aa_mark_code(
            item['LOT_ID'], item['WAFER_ID'], mo_type_2, item['CUST_DEVICE'])

        res['ITEM_LIST'].append(item)

    return res


def get_sap_product_pn(customer_device, fab_device):
    print(customer_device, fab_device)
    res = get_mat_master_data(
        customer_device=customer_device, fab_device=fab_device)

    return res[0]['MATNR']


# 获取AA的打标码
def get_aa_mark_code(lot_id, wafer_id, mo_type_2, customer_device):
    con_or = conn.OracleConn()
    mark_code = ''
    # 量产
    if mo_type_2 == 'P':
        sql = f"SELECT MARKINGCODEFIRST || ONMarkingCodeSeq.QTSeq('{wafer_id}')  FROM CUSTOMERMPNATTRIBUTES c WHERE PART = '{customer_device}' "
        results = con_or.query(sql)
        if results:
            mark_code = xstr(results[0][0])

    elif mo_type_2 == 'S':
        sql = f"SELECT ONSTMarkingCodeSeq.QTSeq('{wafer_id}','{lot_id}')  FROM DUAL "
        results = con_or.query(sql)
        if results:
            mark_code = xstr(results[0][0])

    return mark_code


def get_cust_device(product_name):
    mat = mmi.get_mat_master_data(product_no=product_name)
    return mat[0]['ZZKHXH'], mat[0]['ZZHTXH'], mat[0]['ZZFABXH']


# 更新AA订单数据
def update_aa_po_data(mo_data):
    res = {'ERR_MSG': ''}
    con = conn.HanaConn()

    print("AA工单类型:", mo_data['header']['moType2'])
    mo_type = 'YP02' if mo_data['header']['moType2'] == 'P' else 'YP01'
    lot_list = []

    for item in mo_data['items']:
        cust_device = item.get('CUST_DEVICE', '')
        fab_device = item.get('FAB_DEVICE', '')
        po_id = item.get('PO_ID', '')
        so_id = item.get('SO_ID', '')
        so_item = item.get('SO_ITEM', '')
        product_name = item.get('PRODUCT_PN', '')
        cust, ht, fab = get_cust_device(product_name)
        sap_product_name = item.get('SAP_PRODUCT_PN', '')
        part_product_name = item.get('PART_PRODUCT_PN', '')
        wafer_sn = item.get('WAFER_SN', '')
        mark_code = item.get('MARK_CODE', '')
        coo = item.get('COO','')

        # 更新库存片号
        if not cust_device in "FSA4480UCX":
            lot_id = item.get('LOT_ID', '')
            if not lot_id in lot_list:
                # update_wafer_inv_id(lot_id)
                run(lot_id, '')
                lot_list.append(lot_id)

        # if len(mark_code) != 10:
        #     res['ERR_MSG'] = 'AA打标码位数不正确,正确为10位. 样品=>联系内勤维护EBR, 量产=>联系NPI维护MPN'
        #     return res

        # 更新数据
        sql = f'''UPDATE ZM_CDM_PO_ITEM SET CUSTOMER_DEVICE = '{cust_device}', FAB_DEVICE= '{fab_device}',
        PRODUCT_PN= '{product_name}', CUST_FAB_DEVICE_1 = '{part_product_name}',HT_PN='{ht}',
        SAP_PRODUCT_PN ='{sap_product_name}' ,PO_ID= '{po_id}',PO_TYPE='{mo_type}',ADD_8='SO',
        SO_ID = '{so_id}',SO_ITEM='{so_item}',MARK_CODE = '{mark_code}',ADD_2='{coo}'
        where WAFER_SN = '{wafer_sn}'  '''

        if not con.exec_n(sql):
            res['ERR_MSG'] = 'AA订单更新失败,请联系IT处理'
            return res

    con.db.commit()
    return res


# 获取随机id
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


# 获取wafer_id list
def get_wafer_id_list(lot_id):
    con = conn.HanaConn()
    wafer_id_list = []
    sql = f"SELECT distinct LOT_WAFER_ID FROM ZM_CDM_PO_ITEM zcpi WHERE LOT_ID = '{lot_id}' AND add_8 = 'MAP' and ID = 1  "
    results = con.query(sql)
    if results:
        for row in results:
            wafer_id_list.append(xstr(row[0]))

    return wafer_id_list


# 更新晶圆库存ID
def update_wafer_inv_id(lot_id):
    err_msg = ''
    conDW = conn.HanaConnDW()

    wafer_id_list = get_wafer_id_list(lot_id)
    user_name = ''

    # 查询等量的晶圆库存批次信息
    sql = f''' SELECT b.ZSEQ,b.MATNR,b.WERKS,b.CHARG,b.ZWAFER_LOT FROM VH_SAP_STOCK_INFO a INNER JOIN 
            ZKTMM0001 b on a.CHARG = b.CHARG 
            WHERE a.ZWAFER_LOT = '{lot_id}'
        '''
    results = conDW.query(sql)
    if not results:
        err_msg = f"(DW)查询不到的订单对应的FAB LOT晶圆库存"
        return err_msg

    w_wafer_qty = len(results)
    r_wafer_qty = len(wafer_id_list)
    s_wafer_qty = w_wafer_qty if w_wafer_qty < r_wafer_qty else r_wafer_qty

    for i in range(s_wafer_qty):
        inv_node = {}

        inv_node['FMSYS'] = "CDM"
        inv_node['FMDOCNO'] = "CDM_" + get_rand_id(3)
        inv_node['FMDOCITEM'] = "01"
        inv_node['FMCOUNT'] = "1"
        inv_node['USERID'] = user_name
        inv_node['WORKBENCH'] = "CDM"
        inv_node['ACTION_ID'] = "U"
        inv_node['ZSEQ'] = xstr(results[i][0])
        inv_node['MATNR'] = xstr(results[i][1])
        inv_node['WERKS'] = xstr(results[i][2])
        inv_node['CHARG'] = xstr(results[i][3])
        inv_node['ZWAFER_LOT'] = xstr(results[i][4])
        inv_node['ZWAFER_ID'] = wafer_id_list[i]

        req_node = {"PO_WF_INFO": inv_node}
        res = get_data_from_web_api("MM138", req_node)
        print(res)

    return ''


# 获取随机数
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


def run(lot_id, fab_lot_id=''):
    con = conn.HanaConn()
    conDW = conn.HanaConnDW()
    wafer_id_list = []
    if lot_id:
        sql = f"SELECT lot_wafer_id FROM ZM_CDM_PO_ITEM zcpi WHERE LOT_ID = '{lot_id}' and lot_wafer_id <> '' and id = '1' "
        results = con.query(sql)
        for row in results:
            wafer_id_list.append(row[0])

    elif fab_lot_id:
        sql = f"SELECT replace(lot_wafer_id,lot_id, add_1) FROM ZM_CDM_PO_ITEM zcpi WHERE ADD_1 = '{fab_lot_id}' and lot_wafer_id <> '' and id = '1' "
        results = con.query(sql)
        for row in results:
            wafer_id_list.append(row[0])

    print(wafer_id_list)

    if lot_id:
        sql = f'''SELECT b.ZSEQ,b.MATNR,b.WERKS,b.CHARG,b.ZWAFER_LOT FROM VH_SAP_STOCK_INFO a INNER JOIN
                    ZKTMM0001 b on a.CHARG = b.CHARG
                    INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = a.MATNR 
                    WHERE a.ZWAFER_LOT = '{lot_id}' AND c.MTART in ('Z019')
        '''
    elif fab_lot_id:
        sql = f'''SELECT b.ZSEQ,b.MATNR,b.WERKS,b.CHARG,b.ZWAFER_LOT FROM VH_SAP_STOCK_INFO a INNER JOIN
                    ZKTMM0001 b on a.CHARG = b.CHARG
                    INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = a.MATNR 
                    WHERE a.ZWAFER_LOT = '{fab_lot_id}' AND c.MTART in ('Z019')
        '''
    else:
        sql = ""

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


def get_data_from_web_api(req_type, req_str):
    res = {"ERR_MSG": ""}
    if not (req_type and req_str):
        res["ERR_MSG"] = "请求类别和请求内容不可为空"
        return res

    if req_type == "SD017":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/SD/KSSD017"
        req_url = "http://192.168.98.126:50000/RESTAdapter/SD/KSSD017"
    elif req_type == "PP009":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/PP/KSPP009"
        req_url = "http://192.168.98.126:50000/RESTAdapter/PP/KSPP009"
    elif req_type == "MM108":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/MM/KSMM108"
        req_url = "http://192.168.98.126:50000/RESTAdapter/MM/KSMM108"
    elif req_type == "MM138":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/MM/KSMM138"
        req_url = "http://192.168.98.126:50000/RESTAdapter/MM/KSMM138"
    else:
        res["ERR_MSG"] = f"暂不包含请求类别{req_type}的WEB接口"
        return res

    res = send_request_to_web_api(req_url, req_str)
    return res


def send_request_to_web_api(req_url, req_str):
    res = {"ERR_MSG": ""}
    # req_user = "WS_USER"
    req_user = "pouser"
    # req_passwd = "ht1234"
    req_passwd = "Qwer4321"
    req_content_type = {"Content-Type": "application/json"}

    try:
        if isinstance(req_str, dict):
            req_str = json.dumps(req_str)
        if not isinstance(req_str, str):
            res['ERR_MSG'] = '接口输入参数非法'
            return res

        res["REQ_URL"] = req_url
        res["REQ_DATA_S"] = req_str
        res["RES_DATA_S"] = requests.post(url=req_url, data=req_str, headers=req_content_type,
                                          auth=HTTPBasicAuth(req_user, req_passwd)).text

        res["RES_DATA_D"] = json.loads(res["RES_DATA_S"])

    except Exception as e:
        res['ERR_MSG'] = f"接口返回数据异常:{e}"
        return res

    else:
        return res


if __name__ == "__main__":
    # print(get_wafer_id_list('8917519001'))

    a = {'cust_device': 'AR0330CM1C21SHKA0-CP', 'mo_type_2': 'P'}

    get_aa_po_data(a)
