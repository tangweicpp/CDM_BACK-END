import conn_db as conn
import uuid
import web_api_client as wac
from flask import abort, make_response


def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


def check_is_exists(wafer_id):
    con_dw = conn.HanaConnDW()
    sql = f"SELECT * FROM ZKTMM0001 WHERE ZWAFER_ID = '{wafer_id}' AND WERKS = '1200' AND SUBSTRING(MATNR,11,1) = '4'"
    results = con_dw.query(sql)
    if results:
        return True
    else:
        return False


def get_po_wafer_id_list(lot_id):
    con = conn.HanaConn()
    wafer_id_list = []

    # 查询lot的真实片号
    sql = f"SELECT LOT_WAFER_ID,LOT_ID,ADD_1 FROM ZM_CDM_PO_ITEM WHERE (LOT_ID = '{lot_id}' or ADD_1 = '{lot_id}') AND INSTR(LOT_WAFER_ID,'+') = 0 AND ID = 1 and flag2='0' ORDER BY WAFER_ID"
    print(sql)
    results = con.query(sql)
    if not results:
        print("没有查询到有效的真实订单片号, 请先维护好真实片号")
        abort(make_response({"ERR_MSG": f"LOT:{lot_id}没有查询到真实的订单片号"}))

    for row in results:
        wafer_id = row[0]
        wafer_lot = row[1]
        fab_lot = row[2]

        if lot_id == fab_lot:
            wafer_id = wafer_id.replace(wafer_lot, lot_id)

        if not wafer_id in wafer_id_list:
            if not check_is_exists(wafer_id):
                wafer_id_list.append(wafer_id)

    return wafer_id_list


# 设定片号
def update_wafer_id(wafer_items, wafer_id_list):
    wafer_id_success = []
    soure_len = len(wafer_id_list)
    dest_len = len(wafer_items)
    set_len = soure_len if soure_len < dest_len else dest_len

    # 循环更新
    for i in range(set_len):
        set_obj = {}
        set_obj['WERKS'] = wafer_items[i]['WERKS']
        set_obj['MATNR'] = wafer_items[i]['MATNR']
        set_obj['CHARG'] = wafer_items[i]['CHARG']
        set_obj['ZWAFER_LOT'] = wafer_items[i]['ZWAFER_LOT']
        set_obj['ZSEQ'] = wafer_items[i]['ZSEQ']
        set_obj['ZWAFER_ID'] = wafer_id_list[i]

        submit_request(set_obj)
        wafer_id_success.append(wafer_id_list[i])

    return wafer_id_list


# 提交接口更新请求
def submit_request(set_obj):
    request = {"PO_WF_INFO": {}}
    request['PO_WF_INFO']['FMSYS'] = "CDM"
    request['PO_WF_INFO']['FMDOCNO'] = "CDM_" + get_rand_id(8)
    request['PO_WF_INFO']['FMDOCITEM'] = "CDM"
    request['PO_WF_INFO']['FMCOUNT'] = "1"
    request['PO_WF_INFO']['USERID'] = "CDM"
    request['PO_WF_INFO']['WORKBENCH'] = "CDM"
    request['PO_WF_INFO']['ACTION_ID'] = "U"
    request['PO_WF_INFO']['ZSEQ'] = set_obj['ZSEQ']
    request['PO_WF_INFO']['MATNR'] = set_obj['MATNR']
    request['PO_WF_INFO']['WERKS'] = set_obj['WERKS']
    request['PO_WF_INFO']['CHARG'] = set_obj['CHARG']
    request['PO_WF_INFO']['ZWAFER_LOT'] = set_obj['ZWAFER_LOT']
    request['PO_WF_INFO']['ZWAFER_ID'] = set_obj['ZWAFER_ID']

    wac.get_data_from_web_api("MM138", request)

    # 更新DW
    con_dw = conn.HanaConnDW()
    sql = f"""update ZKTMM0001 set ZWAFER_ID = '{set_obj['ZWAFER_ID']}' where WERKS = '{set_obj['WERKS']}' and MATNR='{set_obj['MATNR']}' and CHARG='{set_obj['CHARG']}' and ZWAFER_LOT='{set_obj['ZWAFER_LOT']}' and
     ZSEQ='{set_obj['ZSEQ']}'
    """
    print(sql)

    con_dw.exec_c(sql)


# 更新晶圆片号
def set_wafer_id(lot_id):
    con_dw = conn.HanaConnDW()
    res = {"ERR_MSG": ""}
    # 待维护订单列表
    wafer_items = []

    # 查询是否有库存
    sql = f"SELECT * FROM ZKTMM0001 WHERE ZWAFER_LOT = '{lot_id}' AND WERKS = '1200' AND SUBSTRING(MATNR,11,1) = '4' "
    results = con_dw.query(sql)
    if not results:
        print(f"查询不到LOT:{lot_id}的晶圆库存")
        abort(make_response({"ERR_MSG": f"查询不到LOT:{lot_id}的晶圆库存"}))

    # 查询是否已经满lot
    # sql = f"SELECT * FROM ZKTMM0001 WHERE ZWAFER_LOT = '{lot_id}' AND WERKS = '1200' AND SUBSTRING(MATNR,11,1) = '4' AND ZWAFER_ID <> '' "
    # results = con_dw.query(sql)
    # if len(results) >= 25:
    #     print(f"LOT:{lot_id}的晶圆已经补全,无需再次更新")
    #     abort(make_response({"ERR_MSG": f"LOT:{lot_id}的晶圆已经补全,无需再次更新"}))

    # # 获取待维护片号
    sql = f"SELECT WERKS,MATNR,CHARG,ZWAFER_LOT,ZSEQ FROM ZKTMM0001 WHERE ZWAFER_LOT = '{lot_id}' AND WERKS = '1200' AND SUBSTRING(MATNR,11,1) = '4'  "
    results = con_dw.query(sql)
    if not results:
        print(f"LOT:{lot_id}没有待维护的晶圆片号")
        abort(make_response({"ERR_MSG": f"LOT:{lot_id}没有待维护的晶圆片号"}))

    for row in results:
        wafer_item = {}
        wafer_item['WERKS'] = row[0]
        wafer_item['MATNR'] = row[1]
        wafer_item['CHARG'] = row[2]
        wafer_item['ZWAFER_LOT'] = row[3]
        wafer_item['ZSEQ'] = row[4]

        wafer_items.append(wafer_item)

    # 获取订单晶圆片号列表
    wafer_id_list = get_po_wafer_id_list(lot_id)
    if not wafer_id_list:
        abort(make_response({"ERR_MSG": f"LOT:{lot_id}晶圆库存片号已经更新完成无需再次更新"}))

    # 更新片号
    res["ITEMS"] = update_wafer_id(wafer_items, wafer_id_list)
    return res


def xstr(s):
    return '' if s is None else str(s).strip()


def update_wafer_id_new(data, header_data):
    print(data)
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()
    user_name = header_data.get('userName', '')
    for row in data:
        # 检查BOM关系

        # f_matnr = ('00000000000000000'+row['F_MATNR'])[-18:]
        c_matnr = xstr(row['MATNR'])
        zdie_gi = row['ZDIE_QTY_GI']  # 耗用量
        zwafer_id = row['ZWAFER_ID']

        # sql = f""" 	SELECT string_agg(b.IDNRK,''',''') FROM VM_SAP_V_MAT a
        # INNER JOIN VM_SAP_V_ITEM b on a.STLNR = b.STLNR
        # INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = b.IDNRK
        # WHERE a.MATNR = '{f_matnr}'
        # AND c.MTART IN ('Z019','Z013','Z015')
        # AND a.WERKS = '1200'
        # """
        # results = con_dw.query(sql)
        # if not results:
        #     return {"ERR_DESC": f"成品物料{f_matnr}对应的BOM查询不到"}

        # c_matnr_list = xstr(results[0][0])
        # if not c_matnr_list:
        #     return {"ERR_DESC": f"成品物料{f_matnr} BOM对应的晶圆/半成品查询不到"}

        # if not c_matnr in c_matnr_list:
        #     return {"ERR_DESC": f"成品物料{f_matnr} BOM无法对应晶圆{c_matnr}"}

        # 已耗用不可更新
        if zdie_gi != '0':
            return {"ERR_DESC": f"晶圆{zwafer_id} 有耗用记录,无法更新晶圆片号"}

        # 非晶圆不可更新
        if c_matnr[:1] != '4':
            print(c_matnr)
            return {"ERR_DESC": f"片号{zwafer_id}对应料号的非晶圆料号,无法更新晶圆片号"}

        # 更新
        request = {"PO_WF_INFO": {}}
        request['PO_WF_INFO']['FMSYS'] = "CDM"
        request['PO_WF_INFO']['FMDOCNO'] = "CDM_" + get_rand_id(8)
        request['PO_WF_INFO']['FMDOCITEM'] = "CDM"
        request['PO_WF_INFO']['FMCOUNT'] = "1"
        request['PO_WF_INFO']['USERID'] = "CDM"
        request['PO_WF_INFO']['WORKBENCH'] = "CDM"
        request['PO_WF_INFO']['ACTION_ID'] = "U"
        request['PO_WF_INFO']['ZSEQ'] = row['ZSEQ']
        request['PO_WF_INFO']['MATNR'] = row['MATNR']
        request['PO_WF_INFO']['WERKS'] = row['WERKS']
        request['PO_WF_INFO']['CHARG'] = row['CHARG']
        request['PO_WF_INFO']['ZWAFER_LOT'] = row['ZWAFER_LOT']
        request['PO_WF_INFO']['ZWAFER_ID'] = row['ZWAFER_ID_NEW']

        # 判断客户
        sql = f"select cust_code from zm_cdm_po_item where (lot_id ='{row['ZWAFER_LOT']}' or add_1 = '{row['ZWAFER_LOT']}') and cust_code in ('US008','US010') "
        results = con.query(sql)
        if not results:
            if row['ZWAFER_ID']:
                if user_name in ("07885", "15918"):
                    pass
                else:
                    return {"ERR_DESC": "已经有片号的 , 不允许再次更新片号"}

        api_res = wac.get_data_from_web_api("MM138", request)
        print(api_res['RES_DATA_D'])

        # 更新DW
        if api_res['RES_DATA_D'].get('PO_RESULT', {}).get('STATUS', '') == "S":
            sql = f"""update ZKTMM0001 set ZWAFER_ID = '{row['ZWAFER_ID_NEW']}' where WERKS = '{row['WERKS']}' and SUBSTRING(MATNR,11)='{row['MATNR']}' and CHARG='{row['CHARG']}' and ZWAFER_LOT='{row['ZWAFER_LOT']}' and
            ZSEQ='{row['ZSEQ']}' and MANDT = '900'
            """
            print(sql)
            con_dw.exec_c(sql)
        else:
            return {"ERR_DESC": "片号更新失败"}

    return {"ERR_DESC": ""}


if __name__ == "__main__":
    # lot_id = "672Z17"
    # set_wafer_id(lot_id)

    data = [{'CHARG': '2002131775', 'ERDAT': '20210309', 'LGORT': '1904', 'MATNR': '000000000042205163', 'MTART': 'Z019-晶圆', 'WERKS': '1200', 'ZBIN_NO': '', 'ZDIE_QTY': '8397', 'ZDIE_QTY_GI': '0', 'ZDIE_QTY_RM': '8397', 'ZGROSS_DIE_QTY': '8397', 'ZOUT_BOX': '', 'ZSEQ': '001', 'ZWAFER_ID': 'DPA945.01-ES07', 'ZWAFER_ID_NEW': 'DPA945.01-ES07', 'ZWAFER_LOT': 'DPA945.01-ES', 'ZZCNLH': '60N008B00000CF', 'ZZHTXH': 'N008B', 'F_MATNR': '32108984'}, {'CHARG': '2002131775', 'ERDAT': '20210309', 'LGORT': '1904', 'MATNR': '000000000042205163', 'MTART': 'Z019-晶圆', 'WERKS': '1200', 'ZBIN_NO': '', 'ZDIE_QTY': '8397', 'ZDIE_QTY_GI': '0', 'ZDIE_QTY_RM': '8397', 'ZGROSS_DIE_QTY': '8397', 'ZOUT_BOX': '', 'ZSEQ': '002', 'ZWAFER_ID': 'DPA945.01-ES08', 'ZWAFER_ID_NEW': 'DPA945.01-ES08', 'ZWAFER_LOT': 'DPA945.01-ES', 'ZZCNLH': '60N008B00000CF', 'ZZHTXH': 'N008B', 'F_MATNR': '32108984'}, {'CHARG': '2002131775', 'ERDAT': '20210309', 'LGORT': '1904', 'MATNR': '000000000042205163', 'MTART': 'Z019-晶圆', 'WERKS': '1200', 'ZBIN_NO': '', 'ZDIE_QTY': '8397', 'ZDIE_QTY_GI': '0', 'ZDIE_QTY_RM': '8397', 'ZGROSS_DIE_QTY': '8397', 'ZOUT_BOX': '', 'ZSEQ': '003', 'ZWAFER_ID': 'DPA945.01-ES09', 'ZWAFER_ID_NEW': 'DPA945.01-ES09', 'ZWAFER_LOT': 'DPA945.01-ES', 'ZZCNLH': '60N008B00000CF', 'ZZHTXH': 'N008B', 'F_MATNR': '32108984'},
            {'CHARG': '2002131775', 'ERDAT': '20210309', 'LGORT': '1904', 'MATNR': '000000000042205163', 'MTART': 'Z019-晶圆', 'WERKS': '1200', 'ZBIN_NO': '', 'ZDIE_QTY': '8397', 'ZDIE_QTY_GI': '0', 'ZDIE_QTY_RM': '8397', 'ZGROSS_DIE_QTY': '8397', 'ZOUT_BOX': '', 'ZSEQ': '004', 'ZWAFER_ID': 'DPA945.01-ES10', 'ZWAFER_ID_NEW': 'DPA945.01-ES10', 'ZWAFER_LOT': 'DPA945.01-ES', 'ZZCNLH': '60N008B00000CF', 'ZZHTXH': 'N008B', 'F_MATNR': '32108984'}, {'CHARG': '2002131775', 'ERDAT': '20210309', 'LGORT': '1904', 'MATNR': '000000000042205163', 'MTART': 'Z019-晶圆', 'WERKS': '1200', 'ZBIN_NO': '', 'ZDIE_QTY': '8397', 'ZDIE_QTY_GI': '0', 'ZDIE_QTY_RM': '8397', 'ZGROSS_DIE_QTY': '8397', 'ZOUT_BOX': '', 'ZSEQ': '005', 'ZWAFER_ID': 'DPA945.01-ES11', 'ZWAFER_ID_NEW': 'DPA945.01-ES11', 'ZWAFER_LOT': 'DPA945.01-ES', 'ZZCNLH': '60N008B00000CF', 'ZZHTXH': 'N008B', 'F_MATNR': '32108984'}, {'CHARG': '2002131775', 'ERDAT': '20210309', 'LGORT': '1904', 'MATNR': '000000000042205163', 'MTART': 'Z019-晶圆', 'WERKS': '1200', 'ZBIN_NO': '', 'ZDIE_QTY': '8397', 'ZDIE_QTY_GI': '0', 'ZDIE_QTY_RM': '8397', 'ZGROSS_DIE_QTY': '8397', 'ZOUT_BOX': '', 'ZSEQ': '006', 'ZWAFER_ID': 'DPA945.01-ES12', 'ZWAFER_ID_NEW': 'DPA945.01-ES12', 'ZWAFER_LOT': 'DPA945.01-ES', 'ZZCNLH': '60N008B00000CF', 'ZZHTXH': 'N008B', 'F_MATNR': '32108984'}]

    update_wafer_id_new(data)
