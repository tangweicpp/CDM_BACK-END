import conn_db as conn
import trans_sql_to_xl as ttx
import os
import re
import datetime
import pandas as pd
from flask import make_response, abort
# 库存盘点模块


def xstr(s):
    return '' if s is None else str(s).strip()


# 获取当日待盘点数据
def get_xb_inv_items(header_data):
    print(header_data)
    res = {"ERR_DESC": "", "ITEMS_DATA": [],
           "TOTAL_PART_QTY": 0, "TOTAL_PD_PART_QTY": 0}

    # 初始化数据
    init_cur_part_id_data(header_data)
    con = conn.HanaConnDW()

    # 查询数据
    sql = f"""SELECT INVENTORY_ID,INVENTORY_DC,LOCATION,LOCATION_DESC,PART,PART_DESC,SAP_PART,UNIT,BALANCE_QTY,ISSUE_QTY,
    COMMIT_QTY,CONSUME_QTY,UPDATE_BY,to_char(UPDATE_DATE ,'YYYY-MM-DD hh24:mi') FROM ZM_INVENTORY_CK WHERE LOCATION = '{header_data['moLocation']}' 
    and INVENTORY_DC = to_char(NOW(),'YYYYMMDD') AND FLAG = '1' order by BALANCE_QTY desc,INVENTORY_ID """
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_DESC": "查询不到当日可盘点数据"}))

    res['TOTAL_PART_QTY'] = len(results)

    for row in results:
        item_data = {}
        item_data['INVENTORY_ID'] = row[0]
        item_data['INVENTORY_DC'] = xstr(row[1])
        item_data['LOCATION'] = xstr(row[2])
        item_data['LOCATION_DESC'] = xstr(row[3])
        item_data['PART'] = xstr(row[4])
        item_data['PART_DESC'] = xstr(row[5])
        item_data['SAP_PART'] = xstr(row[6])
        # 单位
        item_data['UNIT'] = xstr(row[7])
        # 结余量
        item_data['BALANCE_QTY'] = '前日未盘' if row[8] == -1 else xstr(row[8])

        if item_data['BALANCE_QTY'] != '前日未盘':
            res['TOTAL_PD_PART_QTY'] = res['TOTAL_PD_PART_QTY'] + 1

        # 调拨量
        item_data['ISSUE_QTY'] = get_311_data(
            con, item_data['SAP_PART'], item_data['LOCATION'])
        # 盘点量
        item_data['COMMIT_QTY'] = xstr(row[10])
        # 耗用量
        item_data['CONSUME_QTY'] = xstr(row[11])
        item_data['UPDATE_BY'] = xstr(row[12])
        item_data['UPDATE_DATE'] = xstr(row[13])
        # 库存量
        item_data['INV_QTY'] = get_sys_inventory(
            con, item_data['LOCATION'], item_data['SAP_PART'])

        res['ITEMS_DATA'].append(item_data)

    return res


# 系统库存
def get_sys_inventory(con_dw, location, sap_product_id):
    sap_product_id = ("000000000000000000000" + sap_product_id)[-18:]
    sql = f"SELECT sum(CLABS) FROM MCHB WHERE LGORT = '{location}' AND MATNR = '{sap_product_id}' "
    results = con_dw.query(sql)
    if results:
        if xstr(results[0][0]):
            return int(results[0][0])
        else:
            return 0
    else:
        return 0


# 导出当日盘点数据
def export_xb_inv_items(header_data):
    print(header_data)
    res = {"ERR_DESC": ""}

    # 初始化数据
    con = conn.HanaConnDW()

    # 查询数据
    if header_data['queryStartDate'] and header_data['queryEndDate']:
        sql = f"""SELECT  INVENTORY_DC AS "盘点日期", LOCATION || '_' || LOCATION_DESC as "库位",PART as "物料号",SAP_PART as "SAP料号",PART_DESC as "物料描述",UNIT as "物料单位",BALANCE_QTY as "前日结余",ISSUE_QTY as "当日调拨",
        CONSUME_QTY as "当日耗用",COMMIT_QTY as "当日实盘",UPDATE_BY as "盘点人",to_char(UPDATE_DATE ,'YYYY-MM-DD hh24:mi')  as "盘点时间" FROM ZM_INVENTORY_CK WHERE LOCATION = '{header_data['moLocation']}' 
        AND FLAG = '1' AND INVENTORY_DC >= '{header_data['queryStartDate']}' AND INVENTORY_DC <= '{header_data['queryEndDate']}' order by INVENTORY_DC,INVENTORY_ID"""
    else:
        sql = f"""SELECT INVENTORY_DC AS "盘点日期",LOCATION || '_' || LOCATION_DESC as "库位",PART as "物料号",SAP_PART as "SAP料号",PART_DESC as "物料描述",UNIT as "物料单位",BALANCE_QTY as "前日结余",ISSUE_QTY as "当日调拨",
        CONSUME_QTY as "当日耗用",COMMIT_QTY as "当日实盘",UPDATE_BY as "盘点人",to_char(UPDATE_DATE ,'YYYY-MM-DD hh24:mi')  as "盘点时间" FROM ZM_INVENTORY_CK WHERE LOCATION = '{header_data['moLocation']}' 
        and INVENTORY_DC = to_char(NOW(),'YYYYMMDD') AND FLAG = '1' order by INVENTORY_ID """

    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_DESC": "查询不到盘点记录"}))

    file_id = ttx.trans_sql_dw(sql, f"{header_data['moLocation']}盘点历史.xlsx")
    print("文件名:", file_id)
    res['HEADER_DATA'] = file_id

    return res


# 初始化当天日期的物料清单
def init_cur_part_id_data(header_data):
    location_id = header_data['moLocation']
    user_name = header_data['userName']
    location_desc = get_location_desc(location_id)

    con = conn.HanaConnDW()

    # 判断今天物料数据是否初始化
    sql = f"SELECT * FROM ZM_INVENTORY_CK zic WHERE INVENTORY_DC = to_char(NOW(),'YYYYMMDD') AND LOCATION = '{location_id}' AND FLAG = '1' "
    results = con.query(sql)
    if not results:
        # 初始化数据
        # 获取当前库位的物料清单
        sql = f"SELECT SAP_PART FROM ZM_INVENTORY_CK where flag = 'INIT' AND LOCATION = '{location_id}'"
        results2 = con.query(sql)
        if not results2:
            print(f"当前库位:{location_id}没有配置盘点物料清单")
            abort(make_response({"ERR_DESC": f"当前库位:{location_id}没有配置盘点物料清单"}))

        for row in results2:
            sap_part_id = xstr(row[0])

            # 1.获取物料属性
            part_id, part_desc, part_unit = get_part_info(con, sap_part_id)

            # 2.获取当前物料结余的数量
            part_remain_qty = get_part_remain_qty(
                con, sap_part_id, location_id)

            # 3.插入数据
            if part_remain_qty['FLAG']:
                sql = f"""INSERT INTO ZM_INVENTORY_CK(INVENTORY_ID,INVENTORY_DC,LOCATION,LOCATION_DESC,PART,PART_DESC,SAP_PART,UNIT,BALANCE_QTY,CREATE_BY,CREATE_DATE,FLAG)
                VALUES(ZM_INVENTORY_CK_SEQ.NEXTVAL,TO_CHAR(NOW(),'YYYYMMDD'),'{location_id}','{location_desc}','{part_id}','{part_desc}','{sap_part_id}','{part_unit}',{part_remain_qty['QTY']},'{user_name}',NOW(),'1')
                """
                con.exec_n(sql)
            else:
                sql = f"""INSERT INTO ZM_INVENTORY_CK(INVENTORY_ID,INVENTORY_DC,LOCATION,LOCATION_DESC,PART,PART_DESC,SAP_PART,UNIT,BALANCE_QTY,CREATE_BY,CREATE_DATE,FLAG)
                VALUES(ZM_INVENTORY_CK_SEQ.NEXTVAL,TO_CHAR(NOW(),'YYYYMMDD'),'{location_id}','{location_desc}','{part_id}','{part_desc}','{sap_part_id}','{part_unit}',-1,'{user_name}',NOW(),'1')
                """
                con.exec_n(sql)

    # SQL提交
    con.db.commit()


# 根据库位获取库位描述
def get_location_desc(location_id):
    con = conn.HanaConn()
    sql = f"SELECT VALUE FROM ZM_CDM_KEY_LOOK_UP zcklu WHERE KEY = '{location_id}' AND REMAKR = '1200' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    else:
        return ""


# 根据sap料号查询料号其他属性
def get_part_info(con, sap_part_id):
    sql = f"SELECT ZZCNLH,MAKTX,MEINS  FROM VM_SAP_MAT_INFO vsmi WHERE MATNR = '{sap_part_id}' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0]), xstr(results[0][1]), xstr(results[0][2])
    else:
        return "", "", ""


# 根据厂内料号查询出SAP料号
def get_sap_part_id(con, part_id):
    sql = f"SELECT MATNR  FROM VM_SAP_MAT_INFO vsmi WHERE ZZCNLH = '{part_id}' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    else:
        return ""


# 获取当前物料结余的数量
def get_part_remain_qty_del(con, sap_part_id, location_id):
    sql = f"SELECT COMMIT_QTY ,BALANCE_QTY FROM ZM_INVENTORY_CK WHERE sap_part = '{sap_part_id}' AND LOCATION = '{location_id}' AND FLAG = '1' ORDER BY INVENTORY_ID desc"
    results = con.query(sql)
    # 如果没有记录,返回0
    if not results:
        sql = f"SELECT COMMIT_QTY ,BALANCE_QTY FROM ZM_INVENTORY_CK WHERE sap_part = '{sap_part_id}' AND LOCATION = '{location_id}' AND FLAG = 'INIT' ORDER BY INVENTORY_ID desc"
        results = con.query(sql)
        if results:
            commit_qty = results[0][0]
            balance_qty = results[0][1]
            if commit_qty and commit_qty >= 0:
                return commit_qty
            else:
                if balance_qty and balance_qty >= 0:
                    return balance_qty
                else:
                    return 0
        else:
            return 0

    # 如果有记录,取commit_qty(上次的盘点数量), 否则取上次的balance_qty(上次的结余数量), 否则取0
    commit_qty = results[0][0]
    balance_qty = results[0][1]
    if commit_qty and commit_qty >= 0:
        return commit_qty
    else:
        if balance_qty and balance_qty >= 0:
            return balance_qty
        else:
            return 0


# 获取当前物料结余的数量
def get_part_remain_qty(con, sap_part_id, location_id):
    res = {"FLAG": True, "QTY": 0}
    sql = f"SELECT COMMIT_QTY FROM ZM_INVENTORY_CK WHERE INVENTORY_DC = TO_CHAR(add_days(CURRENT_DATE ,-1),'YYYYMMDD')  AND LOCATION = '{location_id}' and SAP_PART = '{sap_part_id}' and COMMIT_QTY is null "
    results = con.query(sql)
    if results:
        res['FLAG'] = False
        return res

    sql = f"SELECT COMMIT_QTY FROM ZM_INVENTORY_CK WHERE INVENTORY_DC = TO_CHAR(add_days(CURRENT_DATE ,-1),'YYYYMMDD')  AND LOCATION = '{location_id}' and SAP_PART = '{sap_part_id}' "
    results = con.query(sql)
    if results:
        res['FLAG'] = True
        res['QTY'] = float(results[0][0])
        return res
    else:
        res['FLAG'] = False
        return res


# 获取当日调拨
def get_311_data(con, sap_part_id, location):
    # sql = f"""SELECT SUM(MENGE),MEINS,LGORT,MATNR FROM MSEG WHERE MATNR ='{sap_part_id}' AND  BWART='311' AND LGORT ='{location}'
    # AND BUDAT_MKPF = TO_CHAR(NOW(),'YYYYMMDD') GROUP BY MATNR,LGORT,MEINS """

    sql = f"""SELECT SUM(MENGE),MEINS,LGORT,MATNR FROM MSEG WHERE MATNR ='{sap_part_id}' AND  BWART='311' AND LGORT ='{location}' 
    AND BUDAT_MKPF = TO_CHAR(add_days(CURRENT_DATE ,-1),'YYYYMMDD')  GROUP BY MATNR,LGORT,MEINS """

    results = con.query(sql)
    if results:
        return float(results[0][0])
    else:
        return 0


# ---------------------------------------------------保存数据
def save_xb_inv_items(save_data):
    print(save_data)
    con = conn.HanaConnDW()
    user_name = save_data['header']['userName']
    for row in save_data['items']:
        inventory_id = row['INVENTORY_ID']
        # 盘点数量
        commit_qty = float(row['COMMIT_QTY']) if row['COMMIT_QTY'] else 0
        # 调拨量
        issue_qty = float(row['ISSUE_QTY']) if row['ISSUE_QTY'] else 0
        # consume_qty = float(row['CONSUME_QTY']) if row['CONSUME_QTY'] else 0

        if row['BALANCE_QTY'] == "前日未盘":
            inv_qty = float(row['INV_QTY']) if row['INV_QTY'] else 0
            sql = f"update ZM_INVENTORY_CK set COMMIT_QTY = {commit_qty},ISSUE_QTY={issue_qty},INV_QTY={inv_qty},UPDATE_BY='{user_name}',UPDATE_DATE=now() where INVENTORY_ID = {inventory_id} "

        else:
            # 前日结余
            balance_qty = float(
                row['BALANCE_QTY']) if row['BALANCE_QTY'] else 0
            # 当前库存
            inv_qty = float(row['INV_QTY']) if row['INV_QTY'] else 0
            # 当日耗用
            consume_qty = balance_qty + issue_qty - commit_qty

            sql = f"update ZM_INVENTORY_CK set COMMIT_QTY = {commit_qty},BALANCE_QTY={balance_qty},ISSUE_QTY={issue_qty},CONSUME_QTY={consume_qty},INV_QTY={inv_qty},UPDATE_BY='{user_name}',UPDATE_DATE=now() where INVENTORY_ID = {inventory_id} "

        con.exec_n(sql)

    con.db.commit()

    return {"ERR_DESC": ""}


# ---------------------------------------------------物料清单下载
def download_xb_part_list(header_data):
    con = conn.HanaConnDW()
    res = {"ERR_DESC": ""}
    location_id = header_data['moLocation']

    sql = f"""SELECT LOCATION as "库位",LOCATION_DESC as "库位描述",PART as "物料号",PART_DESC as "物料描述",'' as "期初数量" FROM ZM_INVENTORY_CK WHERE FLAG = 'INIT' AND LOCATION = '{location_id}' """
    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_DESC": f"库存点:{location_id}未导入期初盘点清单,请先上传期初盘点清单"}))

    file_id = ttx.trans_sql_dw(sql, f"{location_id}物料清单.xlsx")
    print("文件名:", file_id)
    res['HEADER_DATA'] = file_id
    return res


# ------------------------------------------------------导入物料清单
def upload_xb_part_list(header_file, header_data):
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

    # 解析保存
    res = parse_xb_part_list(header_data, doc_path)
    return res


# 解析物料清单,并保存
def parse_xb_part_list(header, doc_path):
    print(header, doc_path)

    res = {"ERR_MSG": "", "SUCCESS_MSG": ""}
    part_list = []

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        abort(make_response({"ERR_MSG": f"文件读取失败{e}"}))

    con = conn.HanaConnDW()
    success_cnt = 0

    sql = f"UPDATE ZM_INVENTORY_CK SET INVENTORY_DC=INVENTORY_DC || 'BACK_UP' WHERE INVENTORY_DC= to_char(NOW(),'YYYYMMDD') AND LOCATION = '{header['moLocation']}' "
    con.exec_n(sql)

    for index, row in df.iterrows():
        if index == 0:
            continue

        if len(row) != 6:
            abort(make_response(
                {"ERR_MSG": f"当前上传文件的列数({len(row)}列)和设定的模板列数(6列)不一致"}))

        part = xstr(row[3])
        if not part in part_list:
            part_list.append(part)
        # else:
        #     abort(make_response(
        #         {"ERR_MSG": f"第{index+2}项料号{part}重复"}))

        if row[5]:
            qty = float(row[5])
        else:
            qty = 0
        sap_part_id = get_sap_part_id(con, part)
        part_id, part_desc, part_unit = get_part_info(con, sap_part_id)
        location_desc = get_location_desc(header['moLocation'])
        if not sap_part_id:
            abort(make_response(
                {"ERR_MSG": f"第{index+2}项料号{part}找不到sap料号,请确认是否错误"}))

        # 判断是否是新物料
        sql = f"SELECT * FROM ZM_INVENTORY_CK WHERE LOCATION= '{header['moLocation']}' AND FLAG = 'INIT' AND PART = '{part}' "
        results = con.query(sql)
        if results:
            continue

        # 保存数据
        sql = f"""INSERT INTO ZM_INVENTORY_CK(INVENTORY_ID,INVENTORY_DC,LOCATION,LOCATION_DESC,PART,PART_DESC,SAP_PART,UNIT,BALANCE_QTY,CREATE_BY,CREATE_DATE,FLAG) 
        values(ZM_INVENTORY_CK_SEQ.NEXTVAL,'','{header['moLocation']}','{location_desc}','{part}','{part_desc}','{sap_part_id}','{part_unit}',{qty},'{header['userName']}',NOW(),'INIT') """
        con.exec_n(sql)
        success_cnt = success_cnt + 1

    con.db.commit()
    res["SUCCESS_MSG"] = f"新增{success_cnt}笔料号"
    return res


# ------------------------------------------------------导入盘点数据
def import_xb_inv_items(header_file, header_data):
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
    res = parse_part_inv_data(header_data, doc_path)
    return res


# 解析保存物料清单
def parse_part_inv_data(header, doc_path):
    print(header, doc_path)

    res = {"ERR_MSG": "", "SUCCESS_MSG": ""}
    part_list = []
    part_qty_obj = {}

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        abort(make_response({"ERR_MSG": f"文件读取失败{e}"}))

    con = conn.HanaConnDW()
    success_cnt = 0

    for index, row in df.iterrows():
        if index == 0:
            continue

        if len(row) != 6:
            abort(make_response(
                {"ERR_MSG": f"当前上传文件的列数({len(row)}列)和设定的模板列数(6列)不一致"}))

        part = xstr(row[3])
        if not part in part_list:
            part_list.append(part)
        else:
            abort(make_response(
                {"ERR_MSG": f"第{index+2}项料号{part}重复"}))

        if row[5]:
            qty = float(row[5])
        else:
            qty = 0
        sap_part_id = get_sap_part_id(con, part)
        if not sap_part_id:
            abort(make_response(
                {"ERR_MSG": f"第{index+2}项料号{part}找不到sap料号,请确认是否错误"}))

        # 返回数据
        part_qty_obj[part] = qty

        success_cnt = success_cnt + 1

    if not success_cnt:
        abort(make_response({"ERR_MSG":  "文件没有数据,请检查文件"}))

    con.db.commit()
    res["SUCCESS_MSG"] = f"成功维护{success_cnt}笔数据"
    res['ITEMS'] = part_qty_obj
    return res


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


# -----------程序发布说明
def publish_sys_info(header):
    print(header)
    info = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]CDM程序更新/重启通知(5分钟后重启服务),请尽快退出业务操作:\n" + \
        "IT说明:\n" + header['header']['sysUpdateDesc']

    info = '{"msgtype":"text","text":{"content":"' + info + '"}}'
    cmd = "curl 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=15d6ab85-51ba-44fc-a18f-48cdc89db192' -H 'Content-Type: application/json' -d  '" + info + "'"
    print(cmd)
    os.system(cmd)

# -----------程序发布说明


def publish_sys_info_finish(header):
    print(header)
    info = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]CDM程序更新/重启完成,请正常使用"

    info = '{"msgtype":"text","text":{"content":"' + info + '"}}'
    cmd = "curl 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=15d6ab85-51ba-44fc-a18f-48cdc89db192' -H 'Content-Type: application/json' -d  '" + info + "'"
    print(cmd)
    os.system(cmd)


if __name__ == "__main__":
    get_xb_inv_items(
        {'moLocation': '4004', 'userName': '07885', 'productID': ''})

    # save_xb_inv_items({'header': {'userName': '07885', 'moLocation': '4005', 'productID': ''}, 'items': [{'BALANCE_QTY': '0.0', 'COMMIT_QTY': '4', 'CONSUME_QTY': '', 'INVENTORY_DC': '20210428', 'INVENTORY_ID': 154, 'INV_QTY': '5000',
    #                   'ISSUE_QTY': 10, 'LOCATION': '4005', 'LOCATION_DESC': 'BP原材料非保税线边仓', 'PART': '6-99990A210XT', 'PART_DESC': 'KS_光刻胶_1GAL/瓶_THB-151N', 'SAP_PART': '000000000011103345', 'UNIT': 'GLL', 'UPDATE_BY': '', 'UPDATE_DATE': ''}]})

    # parse_common_po_file({'moLocation': '4006', 'userName': '07885',
    #  'productID': ''}, "/opt/CDM/cdm_1.1_flask/docs/4005物料清单 (14).xlsx")
