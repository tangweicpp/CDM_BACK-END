# 标签打印模块
import conn_db as conn
import json
from flask import abort, make_response
import trans_sql_to_xl as ttx


def xstr(s):
    return '' if s is None else str(s).strip()


# FC蓝膜标签打印
def get_mo_inv_data(header_data):
    con = conn.HanaConn()
    mo_id = header_data['mo_id']

    res = {"ERR_MSG": "", "ITEMS_DATA": []}
    sql = f"SELECT REMARK FROM ZM_CDM_MO_ITEM zcmi WHERE MO_ID = '{mo_id}' "
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "工单号不存在"}))

    for row in results:
        wafer_inv_json = xstr(row[0])
        if wafer_inv_json:
            wafer_inv_data = json.loads(wafer_inv_json)
            for row2 in wafer_inv_data:
                inv_item = {}
                inv_item['ZWAFER_LOT'] = row2['ZWAFER_LOT']
                inv_item['ZWAFER_ID'] = row2['ZWAFER_ID']
                inv_item['ZOUT_BOX'] = row2['ZOUT_BOX']
                inv_item['CHARG'] = row2['CHARG']
                inv_item['ZDIE_QTY_GI'] = row2['ZDIE_QTY_GI']
                inv_item['ZBIN_NO'] = row2['ZBIN_NO']
                inv_item['LGORT'] = row2['LGORT']
                inv_item['MATNR'] = row2['MATNR']
                inv_item['ZSEQ'] = row2['ZSEQ']

                # 获取当前实际库存情况
                get_current_inv_data(inv_item)

                res['ITEMS_DATA'].append(inv_item)

    return res


# 获取当前库存明细
def get_current_inv_data(inv_item):
    con_dw = conn.HanaConnDW()

    sql = f"SELECT ZGROSS_DIE_QTY,ZDIE_QTY,ZDIE_QTY_GI, ZDIE_QTY-ZDIE_QTY_GI  FROM ZKTMM0001 WHERE MANDT = '900' AND WERKS = '1200' AND CHARG = '{inv_item['CHARG']}' AND MATNR = '{inv_item['MATNR']}' AND ZSEQ = '{inv_item['ZSEQ']}'"
    results = con_dw.query(sql)
    if results:
        inv_item['ZGROSS_DIE_QTY'] = int(results[0][0])
        inv_item['ZDIE_QTY'] = int(results[0][1])
        inv_item['ZDIE_QTY_GI_CURR'] = int(results[0][2])
        inv_item['ZDIE_QTY_RM_CURR'] = int(results[0][3])


# 打印蓝膜标签
def print_label_01(data):
    for row in data['items']:
        out_box_id = row['ZOUT_BOX']
        print(out_box_id)

    return "success"


# -------------------------------------蓝膜库存管理-------------------
def get_mo_crop_data(header_data):
    res = {"ERR_MSG": "", "ITEMS_DATA": []}

    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    # 检查
    if header_data['query_type'] == 'P1':
        sql = f"""SELECT A.MO_ID, B.ZZPROCESS FROM ZM_CDM_MO_HEADER A
            INNER JOIN MARA B ON A.PRODUCT_PN = B.ZZCNLH
            WHERE A.MO_ID = '{header_data['query_value']}' AND B.ZZPROCESS LIKE '%FC%' AND A.FLAG = '1'
        """
        results = con_dw.query(sql)
        if not results:
            abort(make_response(
                {"ERR_MSG": f"工单号:{header_data['query_value']}不存在,或非FC段工单", "ERR_SQL": sql}))

        # 3. 查询工单耗用库存记录
        sql = f"SELECT * FROM ZM_CDM_INVENTORY_POINTER WHERE MO_ID = '{header_data['query_value']}' "
        results = con_dw.query(sql)
        if not results:
            abort(make_response(
                {"ERR_MSG": f"工单耗用上段物料的记录查询不到,请联系IT确认异常", "ERR_SQL": sql}))

    elif header_data['query_type'] in ('P2', 'P3'):
        if not (header_data['query_start_date'] or header_data['query_end_date']):
            abort(make_response({"ERR_MSG": "请输入查询的日期区间"}))

    # 基础SQL
    sql_base = f"""SELECT A.MO_ID,A.PRODUCT_PN, to_char(A.CREATE_DATE,'YYYY-MM-DD hh24:mi'),B.ZWAFER_ID,C.LGORT,E.LGOBE,
        C.ZBIN AS "货位",C.CHARG ,D.ZZCNLH,SUM(C.ZDIE_QTY - C.ZDIE_QTY_GI) AS "库存数",
        SUM(CASE WHEN C.ZBIN_NO NOT IN ('E','F') THEN C.ZDIE_QTY - C.ZDIE_QTY_GI
        ELSE 0 END) AS "良品",SUM(CASE WHEN C.ZBIN_NO IN ('E','F') THEN C.ZDIE_QTY - C.ZDIE_QTY_GI
        ELSE 0 END) AS "不良",
        ROUND(D.ZZJYGD,0) AS GROSS_DIE,F.ZZKHGD,F.SFC,to_char(F.PACKING_TIME,'YYYY-MM-DD hh24:mi'),A.HT_PN,B.MO_WAFER_ID,X.WAFER_STAT,X.WAFER_LOCATION,X.LOCATION_ID
        FROM ZM_CDM_MO_HEADER A
        INNER JOIN ZM_CDM_INVENTORY_POINTER B ON B.MO_ID = A.MO_ID
        INNER JOIN ZKTMM0001 C ON C.MANDT = B.MANDT AND C.WERKS = B.WERKS
        AND C.CHARG = B.CHARG AND C.MATNR = B.MATNR AND C.ZWAFER_LOT = B.ZWAFER_LOT
        AND C.ZWAFER_ID = B.ZWAFER_ID
        INNER JOIN MARA m ON m.ZZCNLH = A.PRODUCT_PN AND m.ZZPROCESS LIKE '%FC%'
        LEFT JOIN MARA D ON D.MATNR = C.MATNR
        LEFT JOIN T001L E ON E.LGORT = C.LGORT AND E.WERKS = '1200'
        LEFT JOIN ZKTPP0008_LOGA F ON F.CHARG = C.CHARG AND F.WAFER_ID = C.ZWAFER_ID
        AND F.BIN_CODE_ID = C.ZBIN_NO
        LEFT JOIN ZM_CDM_WAFER_CROP_ITEMS X ON X.WAFER_ID = B.ZWAFER_ID AND X.WAFER_CHARG = B.CHARG
        """

    # 工单/机种/客户
    if header_data['query_type'] == 'P1':
        sql_1 = f" WHERE A.MO_ID = '{header_data['query_value']}' "

    elif header_data['query_type'] == 'P2':
        sql_1 = f" WHERE A.CUST_CODE = '{header_data['query_value']}' "

    elif header_data['query_type'] == 'P3':
        sql_1 = f" WHERE A.HT_PN = '{header_data['query_value']}' "

    else:
        sql_1 = ""

    # 已入/待入
    if header_data['query_limit'] == "待入蓝膜仓":
        sql_2 = f" AND (X.WAFER_STAT IS NULL OR  X.WAFER_STAT <> 'MOVE_IN') "

    elif header_data['query_limit'] == "已入蓝膜仓":
        sql_2 = f"  AND X.WAFER_STAT = 'MOVE_IN' "

    else:
        sql_2 = ""

    # 日期区间
    if header_data['query_start_date'] and header_data['query_end_date']:
        sql_3 = f" AND A.CREATE_DATE >= '{header_data['query_start_date']}' AND A.CREATE_DATE <= '{header_data['query_end_date']}' "

    else:
        sql_3 = ""

    # GROUP By
    sql_group = """
        GROUP BY A.MO_ID,A.PRODUCT_PN,to_char(A.CREATE_DATE,'YYYY-MM-DD hh24:mi'),B.ZWAFER_ID,C.LGORT,E.LGOBE,A.HT_PN,B.MO_WAFER_ID,X.WAFER_STAT,X.WAFER_LOCATION,X.LOCATION_ID,
        C.ZBIN,C.CHARG ,D.ZZCNLH,ROUND(D.ZZJYGD,0),F.ZZKHGD,F.SFC,to_char(
            F.PACKING_TIME,'YYYY-MM-DD hh24:mi')
        ORDER BY A.MO_ID,B.ZWAFER_ID
    """

    sql = sql_base + sql_1 + sql_2 + sql_3 + sql_group
    print(sql)
    results = con_dw.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "查询不到明细数据", "ERR_SQL": sql}))

    # 明细数据
    for row in results:
        inv_item = {}
        inv_item['MO_ID'] = xstr(row[0])
        inv_item['PRODUCT_PN'] = xstr(row[1])
        inv_item['CREATE_DATE'] = xstr(row[2])
        inv_item['ZWAFER_ID'] = xstr(row[3])
        inv_item['LGORT'] = xstr(row[4]) + '-' + xstr(row[5])
        inv_item['ZBIN'] = xstr(row[6])
        inv_item['CHARG'] = xstr(row[7])
        inv_item['ZZCNLH'] = xstr(row[8])
        inv_item['INV_DIES'] = int(row[9])
        inv_item['INV_GOOD_DIES'] = int(row[10])
        inv_item['INV_NG_DIES'] = int(row[11])
        inv_item['GROSS_DIE'] = xstr(row[12])
        inv_item['ZZKHGD'] = xstr(row[13])
        inv_item['SFC'] = xstr(row[14])
        inv_item['PACKING_TIME'] = xstr(row[15])
        inv_item['HT_PN'] = xstr(row[16])
        inv_item['MO_WAFER_ID'] = xstr(row[17])
        inv_item['LOCATION_ID'] = xstr(row[20])

        if xstr(row[18]) == "MOVE_IN":
            inv_item['INV_STAT'] = "MOVE_IN"
            inv_item['ZBIN'] = inv_item['LOCATION_ID']
        else:
            inv_item['INV_STAT'] = "MOVE_OUT"

        if xstr(row[19]) == "4912":
            inv_item['LGORT'] = "4912-FCDA蓝膜半成品保税线边仓"
        elif xstr(row[19]) == "4012":
            inv_item['LGORT'] = "4012-FCDA蓝膜半成品非保税线边仓"

        # 实物片号
        inv_item['RWAFER_ID'] = get_real_wafer_id(
            con, mo_id=inv_item['MO_ID'], wafer_id=inv_item['MO_WAFER_ID'])

        res['ITEMS_DATA'].append(inv_item)

    # 汇总数据
    # 总工单数
    sql_base = f"""SELECT COUNT(DISTINCT A.MO_ID)
        FROM ZM_CDM_MO_HEADER A
        INNER JOIN ZM_CDM_INVENTORY_POINTER B ON B.MO_ID = A.MO_ID
        INNER JOIN ZKTMM0001 C ON C.MANDT = B.MANDT AND C.WERKS = B.WERKS
        AND C.CHARG = B.CHARG AND C.MATNR = B.MATNR AND C.ZWAFER_LOT = B.ZWAFER_LOT
        AND C.ZWAFER_ID = B.ZWAFER_ID
        INNER JOIN MARA m ON m.ZZCNLH = A.PRODUCT_PN AND m.ZZPROCESS LIKE '%FC%'
        LEFT JOIN MARA D ON D.MATNR = C.MATNR
        LEFT JOIN T001L E ON E.LGORT = C.LGORT AND E.WERKS = '1200'
        LEFT JOIN ZKTPP0008_LOGA F ON F.CHARG = C.CHARG AND F.WAFER_ID = C.ZWAFER_ID
        AND F.BIN_CODE_ID = C.ZBIN_NO
        LEFT JOIN ZM_CDM_WAFER_CROP_ITEMS X ON X.WAFER_ID = B.ZWAFER_ID AND X.WAFER_CHARG = B.CHARG
    """

    sql = sql_base + sql_1 + sql_2 + sql_3
    results = con_dw.query(sql)
    if results:
        res['TOTAL_MO_QTY'] = int(results[0][0])

    # 待入工单数
    sql = sql_base + sql_1 + sql_2 + \
        " AND (X.WAFER_STAT <> 'MOVE_IN' OR X.WAFER_STAT IS NULL ) " + sql_3
    results = con_dw.query(sql)
    if results:
        res['MOVE_OUT_MO_QTY'] = int(results[0][0])

    # 已入工单数
    res['MOVE_IN_MO_QTY'] = res['TOTAL_MO_QTY'] - res['MOVE_OUT_MO_QTY']

    # 总片数
    sql_base = f"""SELECT COUNT(DISTINCT B.ZWAFER_ID)
        FROM ZM_CDM_MO_HEADER A
        INNER JOIN ZM_CDM_INVENTORY_POINTER B ON B.MO_ID = A.MO_ID
        INNER JOIN ZKTMM0001 C ON C.MANDT = B.MANDT AND C.WERKS = B.WERKS
        AND C.CHARG = B.CHARG AND C.MATNR = B.MATNR AND C.ZWAFER_LOT = B.ZWAFER_LOT
        AND C.ZWAFER_ID = B.ZWAFER_ID
        INNER JOIN MARA m ON m.ZZCNLH = A.PRODUCT_PN AND m.ZZPROCESS LIKE '%FC%'
        LEFT JOIN MARA D ON D.MATNR = C.MATNR
        LEFT JOIN T001L E ON E.LGORT = C.LGORT AND E.WERKS = '1200'
        LEFT JOIN ZKTPP0008_LOGA F ON F.CHARG = C.CHARG AND F.WAFER_ID = C.ZWAFER_ID
        AND F.BIN_CODE_ID = C.ZBIN_NO
        LEFT JOIN ZM_CDM_WAFER_CROP_ITEMS X ON X.WAFER_ID = B.ZWAFER_ID AND X.WAFER_CHARG = B.CHARG
    """

    sql = sql_base + sql_1 + sql_2 + sql_3
    results = con_dw.query(sql)
    if results:
        res['TOTAL_MO_WAFER_QTY'] = int(results[0][0])

    # 已入片数
    sql = sql_base + sql_1 + sql_2 + " AND X.WAFER_STAT = 'MOVE_IN' " + sql_3
    results = con_dw.query(sql)
    if results:
        res['MOVE_IN_MO_WAFER_QTY'] = int(results[0][0])

    # 待入片数
    res['MOVE_OUT_MO_WAFER_QTY'] = res['TOTAL_MO_WAFER_QTY'] - \
        res['MOVE_IN_MO_WAFER_QTY']

    return res


# 查询实物录入的片号
def get_real_wafer_id(con, mo_id, wafer_id):
    sql = f"SELECT ITEM_NAME_ORIG FROM ZD_CUSTOMER_ITEM WHERE ITEM_NAME = '{wafer_id}' AND SHOP_ORDER_BO LIKE '%{mo_id}%' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    else:
        return ""


# 退入蓝膜仓
def wafer_crop_move_in(request_data):
    res = {"ERR_MSG": ""}
    con = conn.HanaConnDW()
    user_name = request_data['header']['userName'].strip()
    location_id = request_data['header']['invLocation'].strip()

    for item in request_data['items']:
        print(item)
        wafer_inv_dies = item['INV_DIES']
        mo_id = item['MO_ID'].strip()

        # 保税非保蓝膜仓确认
        if mo_id[:1] == "A":
            mo_bonded_location = "4912"
        else:
            mo_bonded_location = "4012"

        # 插入维护动作表
        sql = f"""INSERT INTO ZM_CDM_WAFER_CROP_ACTIONS(WAFER_ID,WAFER_CHARG,WAFER_ACTION,ACTION_BY,ACTION_TIME,REMAKR,ID,MO_ID,REAL_WAFER_ID,LOCATION,LOCATION_ID)
        VALUES('{item['ZWAFER_ID']}','{item['CHARG']}','MOVE_IN','{user_name}',NOW(),'退回蓝膜仓',ZM_CDM_WAFER_CROP_ACTIONS_SEQ.NEXTVAL,'{item['MO_ID']}','{item['RWAFER_ID']}','{mo_bonded_location}','{location_id}')
        """
        con.exec_c(sql)

        # 插入维护明细表
        # 检查是否存在,存在则update
        sql = f"SELECT * from ZM_CDM_WAFER_CROP_ITEMS where WAFER_ID = '{item['ZWAFER_ID']}' AND WAFER_CHARG = '{item['CHARG']}' "
        results = con.query(sql)
        if results:
            # UPDATE
            sql = f"""UPDATE ZM_CDM_WAFER_CROP_ITEMS SET WAFER_STAT = 'MOVE_IN',WAFER_DIES={wafer_inv_dies},WAFER_LOCATION='{mo_bonded_location}',
            UPDATE_BY='{user_name}',UPDATE_TIME=NOW(),LOCATION_ID='{location_id}' WHERE WAFER_ID = '{item['ZWAFER_ID']}' AND WAFER_CHARG = '{item['CHARG']}' """
        else:
            # INSERT
            sql = f"""INSERT INTO ZM_CDM_WAFER_CROP_ITEMS(WAFER_ID,WAFER_CHARG,WAFER_STAT,WAFER_DIES,WAFER_LOCATION,UPDATE_BY,UPDATE_TIME,REMAKR,ID,LOCATION_ID,REAL_WAFER_ID)
            VALUES('{item['ZWAFER_ID']}','{item['CHARG']}','MOVE_IN',{wafer_inv_dies},'{mo_bonded_location}','{user_name}',NOW(),'退回蓝膜仓',ZM_CDM_WAFER_CROP_ITEMS_SEQ.NEXTVAL,'{location_id}','{item['RWAFER_ID']}')
            """

        con.exec_c(sql)

    return res


# 调出蓝膜仓
def wafer_crop_move_out(request_data):
    res = {"ERR_MSG": ""}
    con = conn.HanaConnDW()
    user_name = request_data['header']['userName'].strip()
    # mo_id = request_data['header']['cdmMOID'].strip()

    for item in request_data['items']:

        print(item)
        wafer_inv_dies = item['INV_DIES']

        # 插入维护动作表
        sql = f"""INSERT INTO ZM_CDM_WAFER_CROP_ACTIONS(WAFER_ID,WAFER_CHARG,WAFER_ACTION,ACTION_BY,ACTION_TIME,REMAKR,ID,MO_ID,REAL_WAFER_ID)
        VALUES('{item['ZWAFER_ID']}','{item['CHARG']}','MOVE_OUT','{user_name}',NOW(),'移出蓝膜仓',ZM_CDM_WAFER_CROP_ACTIONS_SEQ.NEXTVAL,'{item['MO_ID']}','{item['RWAFER_ID']}')
        """
        con.exec_c(sql)

        # 插入维护明细表
        # 检查是否存在,存在则update
        sql = f"SELECT * from ZM_CDM_WAFER_CROP_ITEMS where WAFER_ID = '{item['ZWAFER_ID']}' AND WAFER_CHARG = '{item['CHARG']}' "
        results = con.query(sql)
        if results:
            # UPDATE
            sql = f"""UPDATE ZM_CDM_WAFER_CROP_ITEMS SET WAFER_STAT = 'MOVE_OUT',WAFER_DIES={wafer_inv_dies},WAFER_LOCATION='',
            UPDATE_BY='{user_name}',UPDATE_TIME=NOW() WHERE WAFER_ID = '{item['ZWAFER_ID']}' AND WAFER_CHARG = '{item['CHARG']}' """
        # else:
        #     # INSERT
        #     sql = f"""INSERT INTO ZM_CDM_WAFER_CROP_ITEMS(WAFER_ID,WAFER_CHARG,WAFER_STAT,WAFER_DIES,WAFER_LOCATION,UPDATE_BY,UPDATE_TIME,REMAKR,ID)
        #     VALUES('{item['ZWAFER_ID']}','{item['CHARG']}','MOVE_IN',{wafer_inv_dies},'{mo_bonded_location}','{user_name}',NOW(),'移出蓝膜仓',ZM_CDM_WAFER_CROP_ITEMS_SEQ.NEXTVAL)
        #     """

            con.exec_c(sql)

    return res


# -----------------------------------历史维护记录查询-------------------
def get_mo_crop_history(header_data):
    print(header_data)
    res = {"ERR_MSG": "", "ITEMS_DATA": []}

    con_dw = conn.HanaConnDW()

    sql_base = f"""
            SELECT  A.REAL_WAFER_ID ,A.MO_ID ,A.WAFER_ID ,A.WAFER_CHARG,A.WAFER_ACTION,A.ACTION_BY, to_char(A.ACTION_TIME,'YYYY-MM-DD hh24:mi') ,
        A.REMAKR ,A.LOCATION ,A.LOCATION_ID ,
        B.HT_PN,B.CUST_CODE FROM ZM_CDM_WAFER_CROP_ACTIONS A
        LEFT JOIN ZM_CDM_MO_HEADER B ON A.MO_ID = B.MO_ID
        WHERE 1 = 1
    """

    if header_data['query_type'] == 'P1' and header_data['query_value']:
        sql_1 = f" AND A.MO_ID = '{header_data['query_value']}' "
    elif header_data['query_type'] == 'P2' and header_data['query_value']:
        sql_1 = f" AND B.CUST_CODE = '{header_data['query_value']}' "
    elif header_data['query_type'] == 'P3' and header_data['query_value']:
        sql_1 = f" AND B.HT_PN = '{header_data['query_value']}' "
    else:
        sql_1 = ""

    # 日期区间
    if header_data['query_start_date'] and header_data['query_end_date']:
        sql_2 = f" AND A.ACTION_TIME >= '{header_data['query_start_date']}' AND A.ACTION_TIME <= '{header_data['query_end_date']}' "

    else:
        sql_2 = ""

    sql_order_by = " ORDER BY A.ACTION_TIME DESC "

    sql = sql_base + sql_1 + sql_2 + sql_order_by
    print(sql)

    results = con_dw.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "查询不到维护记录"}))

    for row in results:
        item = {}
        item['RWAFER_ID'] = xstr(row[0])
        item['MO_ID'] = xstr(row[1])
        item['ZWAFER_ID'] = xstr(row[2])
        item['CHARG'] = xstr(row[3])
        item['INV_STAT'] = xstr(row[4])
        item['ACTION_BY'] = xstr(row[5])
        item['ACTION_TIME'] = xstr(row[6])
        item['REMAKR'] = xstr(row[7])
        item['LOCATION'] = xstr(row[8])
        item['ZBIN'] = xstr(row[9])
        item['HT_PN'] = xstr(row[10])
        item['CUST_CODE'] = xstr(row[11])

        res['ITEMS_DATA'].append(item)

    return res


def export_mo_crop_history(header_data):
    print(header_data)
    res = {"ERR_MSG": ""}

    sql_base = f"""
            SELECT  A.REAL_WAFER_ID ,A.MO_ID ,A.WAFER_ID ,A.WAFER_CHARG,A.WAFER_ACTION,A.ACTION_BY,to_char(A.ACTION_TIME,'YYYY-MM-DD hh24:mi'),
        A.REMAKR ,A.LOCATION ,A.LOCATION_ID ,
        B.HT_PN,B.CUST_CODE FROM ZM_CDM_WAFER_CROP_ACTIONS A 
        LEFT JOIN ZM_CDM_MO_HEADER B ON A.MO_ID = B.MO_ID 
        WHERE 1 = 1 
    """

    if header_data['query_type'] == 'P1' and header_data['query_value']:
        sql_1 = f" AND A.MO_ID = '{header_data['query_value']}' "
    elif header_data['query_type'] == 'P2' and header_data['query_value']:
        sql_1 = f" AND B.CUST_CODE = '{header_data['query_value']}' "
    elif header_data['query_type'] == 'P3' and header_data['query_value']:
        sql_1 = f" AND B.HT_PN = '{header_data['query_value']}' "
    else:
        sql_1 = ""

    # 日期区间
    if header_data['query_start_date'] and header_data['query_end_date']:
        sql_2 = f" AND A.ACTION_TIME >= '{header_data['query_start_date']}' AND A.ACTION_TIME <= '{header_data['query_end_date']}' "

    else:
        sql_2 = ""

    sql_order_by = " ORDER BY A.ACTION_TIME DESC "

    sql = sql_base + sql_1 + sql_2 + sql_order_by
    print(sql)

    file_id = ttx.trans_sql_dw(sql, "蓝膜领退料记录.xlsx")
    print("文件名:", file_id)
    res['HEADER_DATA'] = file_id
    res['SQL'] = sql
    return res


if __name__ == "__main__":
    get_mo_crop_data({'query_type': 'P1', 'query_value': 'ATC-210525784', 'query_start_date': '2021-06-23 00:00:00',
                      'query_end_date': '2021-07-14 00:00:00', 'mo_id': 'ATC-210525784', 'mo_cust_code': '', 'mo_ht_pn': ''})
