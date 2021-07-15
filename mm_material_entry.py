import conn_db as conn
import json
from flask import abort, make_response
import trans_sql_to_xl as ttx


def xstr(s):
    return '' if s is None else str(s).strip()


# -------------------------------------1.PDA出大仓-必须扫PDA出--------------------------------------------------------------
# 物料信息解析存储
def entry_data(req_data):
    resp = {'CODE': 0, 'MSG': ''}
    if not req_data:
        resp['CODE'] = 4
        resp['MSG'] = "PDA请求JSON不可为空"
        return resp

    # 数据获取
    try:
        req_data = json.loads(req_data)
    except Exception as e:
        resp['CODE'] = 1
        resp['MSG'] = f"PDA请求JSON读取转换失败{req_data}"
        return resp

    # 数据检查
    resp = check_data(req_data)

    # 数据维护
    resp = save_data(req_data)

    return resp


# 检查请求数据
def check_data(req_data):
    resp = {'CODE': 0, 'MSG': ''}

    # HEADER
    if not req_data.get('HEADER'):
        resp['CODE'] = 2
        resp['MSG'] = f"PDA请求JSON没有HEADER节点:{req_data}"
        return resp

    if not req_data['HEADER'].get('ACTION_ID'):
        resp['CODE'] = 2
        resp['MSG'] = f"PDA请求JSON没有ACTION_ID节点:{req_data}"
        return resp

    if not req_data['HEADER']['ACTION_ID'] in ('N'):
        resp['CODE'] = 2
        resp['MSG'] = f"PDA请求JSON的ACTION_ID节点数据异常:{req_data}"
        return resp

    # BODY
    if not req_data.get('BODY'):
        resp['CODE'] = 2
        resp['MSG'] = f"PDA请求JSON没有BODY节点:{req_data}"
        return resp

    if not (req_data['BODY'].get('ORDER') and req_data['BODY'].get('LOC_FROM') and req_data['BODY'].get('LOC_TO')
            and req_data['BODY'].get('LOT_LIST')):
        resp['CODE'] = 2
        resp['MSG'] = f"PDA请求JSON的BODY节点数据缺失:{req_data}"
        return resp

    return resp


# 获取SAP主数据
def get_material_master_data(con_dw, sap_matnr):
    resp = {'CODE': 0, 'MSG': ''}

    sap_matnr = (
        '00000000000000' + sap_matnr)[-18:]
    sql = f"SELECT ZZCNLH,MAKTX,MEINS FROM VM_SAP_MAT_INFO m WHERE MATNR = '{sap_matnr}' "
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 3
        resp['MSG'] = f"查询不到SAP物料主数据:{sap_matnr}"
        abort(make_response(resp))

    zzcnlh = xstr(results[0][0])
    maktx = xstr(results[0][1])
    meins = xstr(results[0][2])
    return zzcnlh, maktx, meins


# 按批次获取有效期
def get_material_lot_validity_date(con_dw, matnr, lot):
    resp = {'CODE': 0, 'MSG': ''}

    sql = f"SELECT DISTINCT VFDAT FROM MCHA WHERE MATNR='{matnr}' AND CHARG = '{lot}' "
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 5
        resp['MSG'] = f"DW中查询不到物料{matnr}批次{lot}的有效期"
        resp['SQL'] = sql
        abort(make_response(resp))

    validity_date = xstr(results[0][0])
    if len(validity_date) != 8:
        resp['MSG'] = f"DW中查询到-物料{matnr}批次{lot}的有效期:{validity_date}格式有误"
        abort(make_response(resp))

    return validity_date


# By料号先进先出,给出OUT清单
def get_material_lot_out_list(con_dw, order):
    resp = {'CODE': 0, 'MSG': ''}

    order_new = order + '+'

    sql = f" SELECT MATNR,sum(BDMNG) FROM ZM_MC_ORIGN_OUT_MAIN WHERE RSNUM = '{order}' GROUP BY MATNR ORDER BY MATNR   "
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 5
        resp['MSG'] = f"DW中查询不到预留单{order}汇总信息"
        resp['SQL'] = sql
        abort(make_response(resp))

    # 数据
    for row in results:
        matnr = xstr(row[0])
        matnr_qty = row[1]

        # 排序查询
        sql = f"SELECT S_CHARG,BDMNG,VFDAT,M_CHARG,CREATE_BY,to_char(CREATE_DATE ,'YYYY-MM-DD hh24:mi') FROM ZM_MC_INVENTORY_ITEM WHERE MATNR = '{matnr}' ORDER BY VFDAT DESC, S_CHARG"
        results2 = con_dw.query(sql)
        if not results2:
            resp['CODE'] = 5
            resp['MSG'] = f"DW中查询不到管制仓库存明细"
            resp['SQL'] = sql
            abort(make_response(resp))

        tmp_total_qty = 0
        for row2 in results2:
            tmp_s_charg = xstr(row2[0])
            tmp_qty = row2[1]
            tmp_vfdat = xstr(row2[2])
            tmp_m_charg = xstr(row2[3])
            tmp_create_by = xstr(row2[4])
            tmp_create_date = xstr(row2[5])
            tmp_total_qty = tmp_total_qty + tmp_qty

            # 数量判断
            # 1. 累计数量小于等于物料总数
            if tmp_total_qty <= matnr_qty:
                # insert
                sql = f"""INSERT INTO ZM_MC_IN_OUT_RELATION_ITEM(ID,RSNUM_IN,RSNUM_OUT,M_CHARG_OUT,S_CHARG_OUT,MATNR,BDMNG,VFDAT,CREATE_BY,CREATE_DATE,CONTROL_OUT_FLAG)
                    VALUES(ZM_MC_IN_OUT_RELATION_ITEM_SEQ.NEXTVAL,'{order}','{order_new}','{tmp_m_charg}','{tmp_s_charg}','{matnr}',{tmp_qty},'{tmp_vfdat}','{tmp_create_by}','{tmp_create_date}','0')
                """
                exec_res, exec_msg = con_dw.exec_n_2(sql)
                if not exec_res:
                    resp['CODE'] = 5
                    resp['MSG'] = f"ZM_MC_IN_OUT_RELATION_ITEM插入失败:{exec_msg}"
                    resp['SQL'] = sql
                    abort(make_response(resp))

            else:
                resp['CODE'] = 4
                resp['MSG'] = f"{matnr}累计数量{tmp_total_qty}无法完全匹配{matnr_qty},请人工确认"
                resp['SQL'] = sql
                abort(make_response(resp))

            # 匹配完毕
            if tmp_total_qty == matnr_qty:
                break


# 保存请求数据
def save_data(req_data):
    resp = {'CODE': 0, 'MSG': ''}
    con_dw = conn.HanaConnDW()

    req_data['BODY']['ORDER'] = (
        '00000000000' + req_data['BODY']['ORDER'])[-10:]
    header = req_data['BODY']

    # 预留单唯一性校验
    sql = f"SELECT * FROM ZM_MC_ORIGN_OUT_MAIN WHERE RSNUM = '{header['ORDER']}'"
    results = con_dw.query(sql)
    if results:
        resp['CODE'] = 5
        resp['MSG'] = f"预留单{header['ORDER']}已存在,不可再次录入"
        return resp

    if req_data['HEADER']['ACTION_ID'] == 'N':
        # 新增数据
        for row in req_data['BODY']['LOT_LIST']:
            # 必要元素检查
            if not row.get('SAP_MATERIAL'):
                resp['CODE'] = 3
                resp['MSG'] = "LOT_LIST中缺少SAP_MATERIAL"
                return resp

            if not row.get('LOT'):
                resp['CODE'] = 3
                resp['MSG'] = "LOT_LIST中缺少主LOT"
                return resp

            if not row.get('QTY_M'):
                resp['CODE'] = 3
                resp['MSG'] = "LOT_LIST中缺少QTY_M或QTY_M不可为0"
                return resp

            if not row.get('ITEM_LIST'):
                resp['CODE'] = 3
                resp['MSG'] = "LOT_LIST中缺少ITEM_LIST"
                return resp

            # 获取SAP物料主数据
            sap_matnr = row['SAP_MATERIAL']
            zzcnlh, maktx, meins = get_material_master_data(con_dw, sap_matnr)

            # 获取物料有效期
            validity_date = get_material_lot_validity_date(
                con_dw, sap_matnr, row['LOT'])

            # 主表数据
            top_id = con_dw.query(
                "SELECT ZM_MC_ORIGN_OUT_MAIN_SEQ.NEXTVAL FROM DUMMY")[0][0]

            # LOT_LIST SAVE
            sql = f"""INSERT INTO ZM_MC_ORIGN_OUT_MAIN(ID,RSNUM,MATNR,ZZCNLH,MAKTX,MEINS,M_CHARG,BDMNG,BDTER,BDART,LGORT,UMLGO,FLAG,CREATE_BY,CREATE_DATE,REMARK_1,REMARK_2,REMARK_3)
            VALUES('{top_id}','{header['ORDER']}','{sap_matnr}','{zzcnlh}','{maktx}',
            '{meins}','{row['LOT']}',{row['QTY_M']},TO_CHAR(NOW(),'YYYYMMDD'),'YR', '{header['LOC_FROM']}','{header['LOC_TO']}','1','{header.get('USER','')}',NOW(),'{validity_date}','','')
            """
            exec_res, exec_msg = con_dw.exec_n_2(sql)
            if not exec_res:
                resp['CODE'] = 5
                resp['MSG'] = f"ZM_MC_ORIGN_OUT_MAIN插入失败:{exec_msg}"
                resp['SQL'] = sql
                return resp

            # 子表
            for item in row['ITEM_LIST']:
                # 必要元素检查
                if not item.get('SUBLOT'):
                    resp['CODE'] = 3
                    resp['MSG'] = "ITEM_LIST中缺少SUBLOT"
                    return resp

                if not item.get('SUBQTY'):
                    resp['CODE'] = 3
                    resp['MSG'] = "ITEM_LIST中缺少SUBQTY或SUBQTY不可为0"
                    return resp

                # ITEM_LIST SAVE
                sql = f"""INSERT INTO ZM_MC_ORIGN_OUT_SUB(ID,TOP_ID,RSNUM,MATNR,M_CHARG,S_CHARG,BDMNG,FLAG,CREATE_BY,CREATE_DATE,REMARK_1,REMARK_2,REMARK_3)
                VALUES(ZM_MC_ORIGN_OUT_SUB_SEQ.NEXTVAL,'{top_id}','{header['ORDER']}','{sap_matnr}','{row['LOT']}','{item['SUBLOT']}',{item['SUBQTY']},'1','{header.get('USER','')}',NOW(),'{validity_date}','','')
                """
                exec_res, exec_msg = con_dw.exec_n_2(sql)
                if not exec_res:
                    resp['CODE'] = 5
                    resp['MSG'] = f"ZM_MC_ORIGN_OUT_SUB插入失败:{exec_msg}"
                    resp['SQL'] = sql
                    return resp

                # 管制仓库存预入
                sql = f"""INSERT INTO ZM_MC_INVENTORY_ITEM(ID,LGORT,MATNR,M_CHARG,S_CHARG,BDMNG,VFDAT,FLAG,CREATE_BY,CREATE_DATE,ORDER_ID)
                VALUES(ZM_MC_INVENTORY_ITEM_SEQ.NEXTVAL, '9999','{sap_matnr}','{row['LOT']}', '{item['SUBLOT']}',{item['SUBQTY']},'{validity_date}','0','{header.get('USER','')}',NOW(),'{header['ORDER']}')
                """
                exec_res, exec_msg = con_dw.exec_n_2(sql)
                if not exec_res:
                    resp['CODE'] = 5
                    resp['MSG'] = f"ZM_MC_INVENTORY_ITEM插入失败:{exec_msg}"
                    resp['SQL'] = sql
                    return resp

                # 记录操作LOG
                log_item = {}
                log_item['USER'] = header.get('USER', '')
                log_item['MATNR'] = sap_matnr
                log_item['M_CHARG'] = row['LOT']
                log_item['S_CHARG'] = item['SUBLOT']
                log_item['BDMNG'] = item['SUBQTY']
                log_item['VFDAT'] = validity_date

                log_res = update_action_log(
                    con_dw, log_item, action_code='ORIGN_OUT')
                if log_res['CODE']:
                    resp['CODE'] = log_res['CODE']
                    resp['MSG'] = log_res['MSG']
                    resp['SQL'] = log_res['SQL']
                    return resp

    # 先进先出给出OUT清单
    get_material_lot_out_list(con_dw, header['ORDER'])

    con_dw.db.commit()
    return resp


# ------------------------------------2.PDA入/出管制仓-----------------------------------------------------------------------
# PDA入管制仓获取预入明细
def pda_material_in_query(query_data):
    resp = {'CODE': 0, 'MSG': '', 'ITEMS_DATA': []}
    con_dw = conn.HanaConnDW()

    # 预留单号
    order = query_data.get('order', '')
    if not order:
        resp['CODE'] = 1
        resp['MSG'] = f"请输入预留单号"
        return resp

    order = ('00000000000' + order)[-10:]
    # 查询预入管制仓明细
    sql = f"""SELECT A.RSNUM,A.LGORT,A.UMLGO,A.MATNR,A.ZZCNLH,A.MAKTX,A.MEINS,A.M_CHARG,B.S_CHARG,B.BDMNG,B.REMARK_1,A.CREATE_BY,to_char(A.CREATE_DATE ,'YYYY-MM-DD hh24:mi'), 
        CASE C.FLAG WHEN '0' THEN '已出大仓' WHEN '1' THEN '已入管制' ELSE  '已出管制' END 
        FROM ZM_MC_ORIGN_OUT_MAIN A 
        INNER JOIN ZM_MC_ORIGN_OUT_SUB B ON A.ID = B.TOP_ID 
        LEFT JOIN ZM_MC_INVENTORY_ITEM C ON B.S_CHARG = C.S_CHARG 
        WHERE A.RSNUM = '{order}' 
        ORDER BY A.RSNUM,A.MATNR,A.M_CHARG,B.S_CHARG
    """
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"查询不到预留单{order}已扫描预入管制仓明细数据"
        return resp

    for row in results:
        item = {}
        item['RSNUM'] = str(int(xstr(row[0])))
        item['LGORT'] = xstr(row[1])
        item['UMLGO'] = xstr(row[2])
        item['MATNR'] = str(int(xstr(row[3])))
        item['ZZCNLH'] = xstr(row[4])
        item['MAKTX'] = xstr(row[5])
        item['MEINS'] = xstr(row[6])
        item['M_CHARG'] = xstr(row[7])
        item['S_CHARG'] = xstr(row[8])
        item['BDMNG'] = float(row[9])
        item['VFDAT'] = xstr(row[10])
        item['CREATE_BY'] = xstr(row[11])
        item['CREATE_DATE'] = xstr(row[12])
        item['STAT'] = xstr(row[13])

        resp['ITEMS_DATA'].append(item)

    return resp


# PDA入管制仓提交入库动作
def pda_material_in_commit(query_data):
    print(query_data)
    resp = {'CODE': 0, 'MSG': '', 'SQL': ''}
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()
    # 预留单号
    order = query_data.get('order', '')
    user = query_data.get('user', '')
    if not (order and user):
        resp['CODE'] = 1
        resp['MSG'] = f"请提供预留单号,用户名"
        return resp

    order = ('00000000000' + order)[-10:]

    # 判断
    # 判断是否已经出大仓
    sql = f"SELECT * FROM ZM_MC_ORIGN_OUT_MAIN WHERE RSNUM = '{order}' "
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"{order}尚未出大仓,请勿直接入管制库"
        return resp

    # 判断是否已入管制仓
    sql = f"SELECT * FROM ZM_MC_INVENTORY_ITEM WHERE ORDER_ID = '{order}' AND FLAG = '1' "
    results = con_dw.query(sql)
    if results:
        resp['CODE'] = 2
        resp['MSG'] = f"{order}已入管制仓,请勿重复提交入库申请"
        return resp

    # 1.入库
    # 更新库存表:预存-> 实入
    sql = f"UPDATE ZM_MC_INVENTORY_ITEM SET FLAG = '1',UPDATE_DATE = NOW(),UPDATE_BY ='{user}'  WHERE ORDER_ID = '{order}' "
    exec_res, exec_msg = con_dw.exec_n_2(sql)
    if not exec_res:
        resp['CODE'] = 5
        resp['MSG'] = f"ZM_MC_INVENTORY_ITEM更新失败:{exec_msg}"
        resp['SQL'] = sql
        return resp

    # 记录操作LOG
    sql = f"SELECT MATNR,M_CHARG,S_CHARG,BDMNG,VFDAT FROM ZM_MC_INVENTORY_ITEM zmii WHERE ORDER_ID = '{order}' "
    results = con_dw.query(sql)
    for row in results:
        log_item = {}
        log_item['USER'] = user
        log_item['MATNR'] = xstr(row[0])
        log_item['M_CHARG'] = xstr(row[1])
        log_item['S_CHARG'] = xstr(row[2])
        log_item['BDMNG'] = row[3]
        log_item['VFDAT'] = xstr(row[4])

        log_res = update_action_log(con_dw, log_item, action_code='CONTROL_IN')
        if log_res['CODE']:
            resp['CODE'] = log_res['CODE']
            resp['MSG'] = log_res['MSG']
            resp['SQL'] = log_res['SQL']
            return resp

        # 2.修改MES批次表状态ZR_GZC_RECIVE
        sql = f"""INSERT INTO ZR_GZC_RECIVE(INVENTORY_ID,TYPE,FLAG,CHARG,RSNUM,CONTENT,VALUE,UPDATE_USER,UPDATE_TIME)
        VALUES('{log_item['S_CHARG']}', 'CONTROL_IN','1','{log_item['M_CHARG']}','{order}','{order}','{log_item['MATNR']}','{log_item['USER']}',NOW())
        """
        exec_res, exec_msg = con.exec_n_2(sql)
        if not exec_res:
            resp['CODE'] = 5
            resp['MSG'] = f"MES入管制记录表ZR_GZC_RECIVE插入失败:{exec_msg}"
            resp['SQL'] = sql
            return resp

    con_dw.db.commit()
    con.db.commit()
    return resp


# 记录log表
def update_action_log(con_dw, item, action_code):
    resp = {'CODE': 0, 'MSG': '', 'SQL': ''}
    sql = f"SELECT ID FROM ZM_MC_ACTION_LOG WHERE S_CHARG='{item['S_CHARG']}' "
    results = con_dw.query(sql)
    if results:
        id = results[0][0]
        # 更新
        if action_code == 'ORIGN_OUT':
            sql = f"UPDATE ZM_MC_ACTION_LOG SET ORIGN_OUT_BY='{item['USER']}',ORIGN_OUT_DATE=to_char(NOW() ,'YYYY-MM-DD hh24:mi'),UPDATE_DATE=NOW() WHERE ID ={id}  "
        elif action_code == 'CONTROL_IN':
            sql = f"UPDATE ZM_MC_ACTION_LOG SET CONTROL_IN_BY='{item['USER']}',CONTROL_IN_DATE=to_char(NOW() ,'YYYY-MM-DD hh24:mi'),UPDATE_DATE=NOW() WHERE ID ={id}  "
        elif action_code == 'CONTROL_OUT':
            sql = f"UPDATE ZM_MC_ACTION_LOG SET CONTROL_OUT_BY='{item['USER']}',CONTROL_OUT_DATE=to_char(NOW() ,'YYYY-MM-DD hh24:mi'),UPDATE_DATE=NOW() WHERE ID ={id}  "

    else:
        # 插入记录
        if action_code == 'ORIGN_OUT':
            sql = f"""INSERT INTO ZM_MC_ACTION_LOG(ID,MATNR,M_CHARG,S_CHARG,BDMNG,VFDAT,ORIGN_OUT_BY,ORIGN_OUT_DATE,FLAG,UPDATE_DATE) 
            VALUES(ZM_MC_ACTION_LOG_SEQ.NEXTVAL,'{item['MATNR']}','{item['M_CHARG']}','{item['S_CHARG']}',{item['BDMNG']},'{item['VFDAT']}','{item['USER']}',to_char(NOW() ,'YYYY-MM-DD hh24:mi'),'1',NOW())
            """
        elif action_code == 'CONTROL_IN':
            sql = f"""INSERT INTO ZM_MC_ACTION_LOG(ID,MATNR,M_CHARG,S_CHARG,BDMNG,VFDAT,CONTROL_IN_BY,CONTROL_IN_DATE,FLAG,UPDATE_DATE) 
            VALUES(ZM_MC_ACTION_LOG_SEQ.NEXTVAL,'{item['MATNR']}','{item['M_CHARG']}','{item['S_CHARG']}',{item['BDMNG']},'{item['VFDAT']}','{item['USER']}',to_char(NOW() ,'YYYY-MM-DD hh24:mi'),'1',NOW())
            """
        elif action_code == 'CONTROL_OUT':
            sql = f"""INSERT INTO ZM_MC_ACTION_LOG(ID,MATNR,M_CHARG,S_CHARG,BDMNG,VFDAT,CONTROL_OUT_BY,CONTROL_OUT_DATE,FLAG,UPDATE_DATE) 
            VALUES(ZM_MC_ACTION_LOG_SEQ.NEXTVAL,'{item['MATNR']}','{item['M_CHARG']}','{item['S_CHARG']}',{item['BDMNG']},'{item['VFDAT']}','{item['USER']}',to_char(NOW() ,'YYYY-MM-DD hh24:mi'),'1',NOW())
            """

    exec_res, exec_msg = con_dw.exec_n_2(sql)
    if not exec_res:
        resp['CODE'] = 5
        resp['MSG'] = f"ZM_MC_ACTION_LOG维护失败:{exec_msg}"
        resp['SQL'] = sql
        return resp

    return resp


# PDA出管制仓获取预出明细
def pda_material_out_query(query_data):
    resp = {'CODE': 0, 'MSG': '', 'ITEMS_DATA': []}

    con_dw = conn.HanaConnDW()
    # 预留单号
    order = query_data.get('order', '')
    if not order:
        resp['CODE'] = 1
        resp['MSG'] = f"请提供预留单号"
        return resp

    order = ('00000000000' + order)[-10:]

    # 查询预出管制仓
    sql = f""" SELECT A.RSNUM_IN,A.RSNUM_OUT,A.MATNR,B.ZZCNLH,B.MAKTX,B.MEINS,A.M_CHARG_OUT,A.S_CHARG_OUT,A.BDMNG,A.VFDAT,A.CREATE_BY,A.CREATE_DATE
    FROM ZM_MC_IN_OUT_RELATION_ITEM A 
    LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR 
    WHERE A.RSNUM_IN = '{order}' ORDER BY A.RSNUM_IN ,A.MATNR,A.M_CHARG_OUT,A.S_CHARG_OUT
    """
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"查询不到预留单{order}已扫描预出管制仓明细数据"
        return resp

    for row in results:
        item = {}
        item['RSNUM'] = str(int(xstr(row[0])))
        item['RSNUM_NEW'] = xstr(row[1])
        item['MATNR'] = str(int(xstr(row[2])))
        item['ZZCNLH'] = xstr(row[3])
        item['MAKTX'] = xstr(row[4])
        item['MEINS'] = xstr(row[5])
        item['M_CHARG'] = xstr(row[6])
        item['S_CHARG'] = xstr(row[7])
        item['BDMNG'] = float(row[8])
        item['VFDAT'] = xstr(row[9])
        item['CREATE_BY'] = xstr(row[10])
        item['CREATE_DATE'] = xstr(row[11])

        resp['ITEMS_DATA'].append(item)

    return resp


# PDA出管制仓提交出库动作
def pda_material_out_commit(query_data):
    resp = {'CODE': 0, 'MSG': '', 'SQL': ''}
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()
    # 预留单号
    order = query_data.get('order', '')
    user = query_data.get('user', '')
    if not (order and user):
        resp['CODE'] = 1
        resp['MSG'] = f"请提供预留单号,用户名"
        return resp

    order = ('00000000000' + order)[-10:]

    # 判断是否已经出大仓
    sql = f"SELECT * FROM ZM_MC_ORIGN_OUT_MAIN WHERE RSNUM = '{order}' "
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"{order}尚未出大仓,无法从管制库出去"
        return resp

    # 判断是否入管制仓
    sql = f"""SELECT S_CHARG,FLAG FROM ZM_MC_INVENTORY_ITEM WHERE S_CHARG in
    (SELECT S_CHARG_OUT FROM ZM_MC_IN_OUT_RELATION_ITEM zmiori WHERE RSNUM_IN = '{order}')
    AND  FLAG = '0' """
    results = con_dw.query(sql)
    if results:
        s_charg = xstr(results[0][0])
        resp['CODE'] = 2
        resp['MSG'] = f"{order}对应的预出子批次{s_charg}尚未入管制仓,请先入管制仓"
        return resp

    # 判断是否出管制仓
    sql = f"SELECT * FROM ZM_MC_IN_OUT_RELATION_ITEM WHERE RSNUM_IN = '{order}' AND CONTROL_OUT_FLAG = '0' "
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"{order}已出管制仓,请勿重复提交出库申请"
        return resp

    # 1.出库
    # 更新库存表:预存-> 实入
    sql = f"DELETE FROM ZM_MC_INVENTORY_ITEM WHERE S_CHARG IN (SELECT S_CHARG_OUT FROM ZM_MC_IN_OUT_RELATION_ITEM WHERE RSNUM_IN='{order}' AND CONTROL_OUT_FLAG='0')"
    exec_res, exec_msg = con_dw.exec_n_2(sql)
    if not exec_res:
        resp['CODE'] = 5
        resp['MSG'] = f"ZM_MC_INVENTORY_ITEM删除失败:{exec_msg}"
        resp['SQL'] = sql
        return resp

    # 更新出库表
    sql = f"""UPDATE ZM_MC_IN_OUT_RELATION_ITEM SET CONTROL_OUT_BY= '{user}', CONTROL_OUT_DATE=to_char(NOW() ,'YYYY-MM-DD hh24:mi'), CONTROL_OUT_FLAG='1'
    WHERE RSNUM_IN = '{order}'
    """
    exec_res, exec_msg = con_dw.exec_n_2(sql)
    if not exec_res:
        resp['CODE'] = 5
        resp['MSG'] = f"ZM_MC_IN_OUT_RELATION_ITEM出库状态切换失败:{exec_msg}"
        resp['SQL'] = sql
        return resp

    # 记录操作LOG
    sql = f"SELECT MATNR,M_CHARG_OUT,S_CHARG_OUT,BDMNG,VFDAT,RSNUM_OUT FROM ZM_MC_IN_OUT_RELATION_ITEM WHERE RSNUM_IN = '{order}' "
    results = con_dw.query(sql)
    for row in results:
        log_item = {}
        log_item['USER'] = user
        log_item['MATNR'] = xstr(row[0])
        log_item['M_CHARG'] = xstr(row[1])
        log_item['S_CHARG'] = xstr(row[2])
        log_item['BDMNG'] = row[3]
        log_item['VFDAT'] = xstr(row[4])
        log_item['RSNUM_OUT'] = xstr(row[5])

        log_res = update_action_log(
            con_dw, log_item, action_code='CONTROL_OUT')
        if log_res['CODE']:
            resp['CODE'] = log_res['CODE']
            resp['MSG'] = log_res['MSG']
            resp['SQL'] = log_res['SQL']
            return resp

        # 2.修改MES批次表状态ZR_GZC_RECIVE
        sql = f"""INSERT INTO ZR_GZC_RECIVE(INVENTORY_ID,TYPE,FLAG,CHARG,RSNUM,CONTENT,VALUE,UPDATE_USER,UPDATE_TIME)
        VALUES('{log_item['S_CHARG']}', 'CONTROL_OUT','0','{log_item['M_CHARG']}','{log_item['RSNUM_OUT']}','{order}','{log_item['MATNR']}','{log_item['USER']}',NOW())
        """
        exec_res, exec_msg = con.exec_n_2(sql)
        if not exec_res:
            resp['CODE'] = 5
            resp['MSG'] = f"MES出管制记录表ZR_GZC_RECIVE插入失败:{exec_msg}"
            resp['SQL'] = sql
            return resp

    con_dw.db.commit()
    con.db.commit()
    return resp


# ---------------------------------------------------------3.前台终端管控查询------------------------------------
# 查询预入预出明细
def get_material_detail(query_data):
    resp = {'CODE': 0, 'MSG': '', 'ITEMS_DATA1': [], 'ITEMS_DATA2': []}

    con_dw = conn.HanaConnDW()
    # 预留单号
    order = query_data.get('queryOrder', '')
    # 物料号
    matnr = query_data.get('queryMatnr', '')
    # 开始日期/截至日期
    queryStartDate = query_data.get('queryStartDate', '')
    queryEndDate = query_data.get('queryEndDate', '')

    if not (order or queryStartDate or queryEndDate):
        resp['CODE'] = 1
        resp['MSG'] = f"请输入预留单号或者选择查询日期区间"
        return resp

    # BY Order查询
    if order:
        order = ('00000000000' + order)[-10:]

        # 查询预入管制仓明细
        sql = f"""SELECT A.RSNUM,A.LGORT,A.UMLGO,A.MATNR,A.ZZCNLH,A.MAKTX,A.MEINS,A.M_CHARG,B.S_CHARG,B.BDMNG,B.REMARK_1,A.CREATE_BY,to_char(A.CREATE_DATE ,'YYYY-MM-DD hh24:mi'), 
            CASE C.FLAG WHEN '0' THEN '已出大仓' WHEN '1' THEN '已入管制' ELSE  '已出管制' END 
            FROM ZM_MC_ORIGN_OUT_MAIN A 
            INNER JOIN ZM_MC_ORIGN_OUT_SUB B ON A.ID = B.TOP_ID 
            LEFT JOIN ZM_MC_INVENTORY_ITEM C ON B.S_CHARG = C.S_CHARG 
            WHERE A.RSNUM = '{order}' 
        """

        # 物料号
        if matnr:
            matnr = ('00000000000' + matnr)[-18:]
            sql = sql + f" AND A.MATNR = '{matnr}' "

        sql = sql + " ORDER BY A.RSNUM,A.MATNR,A.M_CHARG,B.S_CHARG"
    else:
        # 查询预入管制仓明细
        sql = f"""SELECT A.RSNUM,A.LGORT,A.UMLGO,A.MATNR,A.ZZCNLH,A.MAKTX,A.MEINS,A.M_CHARG,B.S_CHARG,B.BDMNG,B.REMARK_1,A.CREATE_BY,to_char(A.CREATE_DATE ,'YYYY-MM-DD hh24:mi'), 
        CASE C.FLAG WHEN '0' THEN '已出大仓' WHEN '1' THEN '已入管制' ELSE  '已出管制' END 
        FROM ZM_MC_ORIGN_OUT_MAIN A 
        INNER JOIN ZM_MC_ORIGN_OUT_SUB B ON A.ID = B.TOP_ID 
        LEFT JOIN ZM_MC_INVENTORY_ITEM C ON B.S_CHARG = C.S_CHARG 
        WHERE A.CREATE_DATE >= '{queryStartDate}' AND A.CREATE_DATE <= '{queryEndDate}'  
        ORDER BY A.RSNUM,A.MATNR,A.M_CHARG,B.S_CHARG
        """
    print(sql)
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"查询不到预留单{order} {matnr}已扫描预入管制仓明细数据"
        return resp

    for row in results:
        item = {}
        item['RSNUM'] = str(int(xstr(row[0])))
        item['LGORT'] = xstr(row[1])
        item['UMLGO'] = xstr(row[2])
        item['MATNR'] = str(int(xstr(row[3])))
        item['ZZCNLH'] = xstr(row[4])
        item['MAKTX'] = xstr(row[5])
        item['MEINS'] = xstr(row[6])
        item['M_CHARG'] = xstr(row[7])
        item['S_CHARG'] = xstr(row[8])
        item['BDMNG'] = float(row[9])
        item['VFDAT'] = xstr(row[10])
        item['CREATE_BY'] = xstr(row[11])
        item['CREATE_DATE'] = xstr(row[12])
        item['STAT'] = xstr(row[13])

        resp['ITEMS_DATA1'].append(item)

    # 查询预出管制仓
    # By ORDER查询
    if order:
        sql = f""" SELECT A.RSNUM_IN,A.RSNUM_OUT,A.MATNR,B.ZZCNLH,B.MAKTX,B.MEINS,A.M_CHARG_OUT,A.S_CHARG_OUT,A.BDMNG,A.VFDAT,A.CREATE_BY,A.CREATE_DATE
        FROM ZM_MC_IN_OUT_RELATION_ITEM A 
        LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR 
        WHERE A.RSNUM_IN = '{order}' ORDER BY A.RSNUM_IN ,A.MATNR,A.M_CHARG_OUT,A.S_CHARG_OUT
        """
    else:
        # By 日期查询
        sql = f""" SELECT A.RSNUM_IN,A.RSNUM_OUT,A.MATNR,B.ZZCNLH,B.MAKTX,B.MEINS,A.M_CHARG_OUT,A.S_CHARG_OUT,A.BDMNG,A.VFDAT,A.CREATE_BY,A.CREATE_DATE
        FROM ZM_MC_IN_OUT_RELATION_ITEM A 
        LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR 
        WHERE A.CREATE_DATE >= '{queryStartDate}' AND A.CREATE_DATE <= '{queryEndDate}'  ORDER BY A.RSNUM_IN ,A.MATNR,A.M_CHARG_OUT,A.S_CHARG_OUT
        """

    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"查询不到预留单{order}已扫描预出管制仓明细数据"
        return resp

    for row in results:
        item = {}
        item['RSNUM_IN'] = str(int(xstr(row[0])))
        item['RSNUM_OUT'] = xstr(row[1])
        item['MATNR'] = str(int(xstr(row[2])))
        item['ZZCNLH'] = xstr(row[3])
        item['MAKTX'] = xstr(row[4])
        item['MEINS'] = xstr(row[5])
        item['M_CHARG'] = xstr(row[6])
        item['S_CHARG'] = xstr(row[7])
        item['BDMNG'] = float(row[8])
        item['VFDAT'] = xstr(row[9])
        item['CREATE_BY'] = xstr(row[10])
        item['CREATE_DATE'] = xstr(row[11])

        resp['ITEMS_DATA2'].append(item)

    return resp


# 导出管制明细清单
def export_material_entry_detail(header_data):
    print(header_data)
    res = {"ERR_MSG": ""}
    # 预留单号
    order = header_data.get('queryOrder', '')
    # 物料号
    matnr = header_data.get('queryMatnr', '')
    # 开始日期/截至日期
    queryStartDate = header_data.get('queryStartDate', '')
    queryEndDate = header_data.get('queryEndDate', '')
    if not (order or queryStartDate or queryEndDate):
        res['ERR_MSG'] = f"请输入预留单号或者选择查询日期区间"
        return res

    if order:
        sql = f"""
        SELECT A.RSNUM_OUT AS "新预留单号",A.RSNUM_IN AS "原预留单号",to_char(C.CREATE_DATE ,'YYYYMMDD') AS "需求日期",A.BDMNG AS "需求量",A.MATNR AS "SAP物料",B.MAKTX AS "物料描述",B.ZZCNLH AS "厂内料号",
        B.MEINS AS "单位",C.UMLGO AS "目标收货仓",C.LGORT AS "大仓库存地点",A.M_CHARG_OUT AS "SAP批次",A.S_CHARG_OUT AS "供应商批次",A.VFDAT AS "有效期"
        FROM ZM_MC_IN_OUT_RELATION_ITEM A 
        LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR
        LEFT JOIN ZM_MC_ORIGN_OUT_MAIN C ON C.RSNUM = A.RSNUM_IN
        WHERE A.RSNUM_IN = '{order}' 
        UNION 
        SELECT A.RSNUM_OUT AS "新预留单号",'' AS "原预留单号",'合计' AS "需求日期", sum(A.BDMNG) AS "需求量",'' AS "SAP物料",'' AS "物料描述",'' AS "厂内料号",
        '' AS "单位",'' AS "目标收货仓",'' AS "大仓库存地点",'' AS "SAP批次",'' AS "供应商批次",'' AS "有效期"
        FROM ZM_MC_IN_OUT_RELATION_ITEM A 
        LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR
        LEFT JOIN ZM_MC_ORIGN_OUT_MAIN C ON C.RSNUM = A.RSNUM_IN 
        WHERE A.RSNUM_IN = '{order}'
        GROUP BY A.RSNUM_OUT 
        """
    else:
        sql = f"""
        SELECT A.RSNUM_OUT AS "新预留单号",A.RSNUM_IN AS "原预留单号",to_char(C.CREATE_DATE ,'YYYYMMDD') AS "需求日期",A.BDMNG AS "需求量",A.MATNR AS "SAP物料",B.MAKTX AS "物料描述",B.ZZCNLH AS "厂内料号",
        B.MEINS AS "单位",C.UMLGO AS "目标收货仓",C.LGORT AS "大仓库存地点",A.M_CHARG_OUT AS "SAP批次",A.S_CHARG_OUT AS "供应商批次",A.VFDAT AS "有效期"
        FROM ZM_MC_IN_OUT_RELATION_ITEM A 
        LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR
        LEFT JOIN ZM_MC_ORIGN_OUT_MAIN C ON C.RSNUM = A.RSNUM_IN
        WHERE A.CREATE_DATE >= '{queryStartDate}' AND A.CREATE_DATE <= '{queryEndDate}'
        UNION 
        SELECT A.RSNUM_OUT AS "新预留单号",'' AS "原预留单号",'合计' AS "需求日期", sum(A.BDMNG) AS "需求量",'' AS "SAP物料",'' AS "物料描述",'' AS "厂内料号",
        '' AS "单位",'' AS "目标收货仓",'' AS "大仓库存地点",'' AS "SAP批次",'' AS "供应商批次",'' AS "有效期"
        FROM ZM_MC_IN_OUT_RELATION_ITEM A 
        LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR
        LEFT JOIN ZM_MC_ORIGN_OUT_MAIN C ON C.RSNUM = A.RSNUM_IN 
        WHERE A.CREATE_DATE >= '{queryStartDate}' AND A.CREATE_DATE <= '{queryEndDate}'
        GROUP BY A.RSNUM_OUT 
        """

    print(sql)
    file_id = ttx.trans_sql_dw(sql, "管制仓发料清单.xlsx")
    res['HEADER_DATA'] = file_id
    res['SQL'] = sql
    return res


# 查询管制仓库存明细
def get_material_inventory_detail(query_data):
    resp = {'CODE': 0, 'MSG': '', 'ITEMS_DATA1': [], 'ITEMS_DATA2': []}

    con_dw = conn.HanaConnDW()
    # 物料号
    matnr = query_data.get('queryMatnr', '')

    # 查询管制仓库存明细
    sql = f"""SELECT A.LGORT,A.MATNR,B.ZZCNLH,B.MAKTX,B.MEINS,A.M_CHARG,A.S_CHARG,A.BDMNG,A.VFDAT,A.CREATE_BY,A.CREATE_DATE,A.ORDER_ID
        FROM ZM_MC_INVENTORY_ITEM A
        LEFT JOIN VM_SAP_MAT_INFO B ON A.MATNR = B.MATNR 
        WHERE A.FLAG = '1'
    """

    # 物料号
    if matnr:
        matnr = ('00000000000' + matnr)[-18:]
        sql = sql + f" AND A.MATNR = '{matnr}' "

    sql = sql + " ORDER BY MATNR, M_CHARG ,S_CHARG "
    print(sql)
    results = con_dw.query(sql)
    if not results:
        resp['CODE'] = 2
        resp['MSG'] = f"查询不到{matnr}管制仓库存明细数据"
        return resp

    for row in results:
        item = {}
        item['LGORT'] = xstr(row[0])
        item['MATNR'] = xstr(row[1])
        item['ZZCNLH'] = xstr(row[2])
        item['MAKTX'] = xstr(row[3])
        item['MEINS'] = xstr(row[4])
        item['M_CHARG'] = xstr(row[5])
        item['S_CHARG'] = xstr(row[6])
        item['BDMNG'] = float(row[7])
        item['VFDAT'] = xstr(row[8])
        item['CREATE_BY'] = xstr(row[9])
        item['CREATE_DATE'] = xstr(row[10])
        item['ORDER_ID'] = xstr(row[11])

        resp['ITEMS_DATA1'].append(item)

    return resp


# 跳过管制仓实扫, 直接过账到MES线边接收


if __name__ == '__main__':
    data = {"order": "0000286957", "user": '12334'}
    pda_material_in_commit(data)
