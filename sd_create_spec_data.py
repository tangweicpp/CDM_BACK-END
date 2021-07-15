import os
import re
import uuid
import time
import conn_db as conn
import pandas as pd
import trans_sql_to_xl as ttx
import mm_mat_info as mmi
from flask import abort, make_response
from web_api_client import get_data_from_web_api


def xstr(s):
    return '' if s is None else str(s).strip()


def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


def get_so_action(con, header):
    action = "C"
    header_no = "b670dd8d0"
    # return action, header_no

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

        con.exec_n(sql)

    return action, header_no


# 创建销售订单
def create_so(con, upload_id):
    so_data_list = {'SO_DATA': []}

    # header
    sql = f"""SELECT PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY,String_agg(WAFER_SN ,''',''') FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}'
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
        sql = f"""SELECT SAP_PRODUCT_PN,sum(PASSBIN_COUNT+FAILBIN_COUNT),CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN,WAFER_PCS_PRICE,WAFER_DIE_PRICE,ADDRESS_CODE,count(1),REMARK1,String_agg(WAFER_SN ,''','''),PO_DATE,REMAKR4,REMAKR5
            FROM ZM_CDM_PO_ITEM WHERE WAFER_SN IN ('{wafer_sn_list}')
            GROUP BY SAP_PRODUCT_PN,CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN,WAFER_PCS_PRICE,WAFER_DIE_PRICE,ADDRESS_CODE,REMARK1,PO_DATE,REMAKR4,REMAKR5 """

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
            po_date = time.strftime('%Y%m%d')

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
            item['VBELN'] = xstr(row[12])  # 参考退货单号
            item['POSNR'] = xstr(row[13])  # 参考退货行号
            item['INCO1'] = ''
            item['INCO2'] = ''
            item['ZZDZDM'] = address_code

            sql = f'''INSERT INTO ZM_CDM_SO_ITEM(SO_SN,CDM_ITEM_SN,PRD_ID,QTY,CREATE_BY,CREATE_DATE,FLAG,SAP_PRD_ID)
                values('{header['HEAD_NO']}','{item['ITEM_NO']}','{product_pn}','{item['KWMENG']}',
                '{item['BNAME']}',now(),'0','{item['MATNR']}')
                '''
            if not con.exec_n(sql):
                con.db.rollback()

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

                wafer = {}
                wafer['ACTION'] = 'N'
                wafer['ZFAB_DEVICE'] = fab_device
                wafer['ZCUST_DEVICE'] = cust_device
                wafer['ZCUST_LOT'] = lot_id
                wafer['ZCUST_WAFER_ID'] = lot_wafer_id
                wafer['ZGOODDIE_QTY'] = wafer_good_dies
                wafer['ZBADDIE_QTY'] = wafer_ng_dies
                wafer['ZGROSSDIE_QTY'] = wafer_gross_dies

                sql = f"""INSERT INTO ZM_CDM_SO_SUB_ITEM(ITEM_SN,WAFER_SN,CUST_LOT_ID,CUST_WAFER_ID,CUST_LOTWAFER_ID,GOOD_DIES,NG_DIES,FLAG,REMARK1,REMARK2)
                values('{item['ITEM_NO']}','{wafer_sn}','{lot_id}','{wafer_id}','{lot_wafer_id}',
                '{wafer_good_dies}','{wafer_ng_dies}','0','','')
                """
                if not con.exec_n(sql):
                    con.db.rollback()

                item['WAFER_LIST'].append(wafer)

            so_data['ITEM'].append(item)

        so_data_list['SO_DATA'].append(so_data)

    print(so_data_list)
    # 发送数据
    res = get_data_from_web_api("SD017", so_data_list)
    print(res['RES_DATA_D'])
    return_node = res['RES_DATA_D']['RETURN'][0]
    err_msg = return_node.get('MESSAGE')
    if err_msg != "成功":
        abort(make_response({"ERR_MSG": err_msg}))

    so_id = return_node.get('VBELN', '')
    so_item = return_node.get('POSNR', '')

    # 更新订单
    sql = f"update zm_cdm_po_item set so_id = '{so_id}', so_item='{so_item}' where upload_id = '{upload_id}' "
    print(sql)
    con.exec_n(sql)


# 上传US010 BC文件
def import_bc_file(so_file):
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
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg = {"ERR_MSG": f"文件读取失败:{e}"}
        abort(make_response(err_msg))

    items = []
    for index, row in df.iterrows():
        if index == 0:
            continue

        if 'rows' in row[0]:
            continue

        item = {}
        if len(row) != 6:
            err_msg = {"ERR_MSG": f"BC模板和设定的模板列数(6列)不一致"}
            abort(make_response(err_msg))
        item['LOT_ID'] = xstr(row[0])[2:]
        item['INVOICE_ID'] = xstr(row[1])
        item['INTR_DATE'] = xstr(row[2])
        item['FAB_DEVICE'] = xstr(row[3])
        item['QUANTITY'] = xstr(row[4])
        item['COO'] = xstr(row[5])

        items.append(item)

    return {"ERR_MSG": "", "ITEMS": items}


# 提交BC数据
def submit_bc_data(data):
    err_msg = ''
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    header = data['header']
    items = data['items']
    username = header['userName']

    print(header)
    # 判断BC是否已经上传过
    for item in items:
        lot_id = item['LOT_ID']

        sql = f"select * from ZM_CDM_AA_BC_DATA where batchid='{lot_id}' and flag='Y' "
        results = con.query(sql)
        if results:
            # 失效
            sql = f"delete from ZM_CDM_AA_BC_DATA  where batchid='{lot_id}' and flag = 'Y' "
            con.exec_c(sql)
            con_dw.exec_c(sql)

    for item in items:
        lot_id = item['LOT_ID']
        lot_coo = item['COO']
        fab_device = item['FAB_DEVICE']
        intr_date = item['INTR_DATE']
        invoice_id = item['INVOICE_ID']
        quantity = item['QUANTITY']
        wafer_id_str = item.get('WAFER_ID_STR', '')

        sql = f'''INSERT INTO ZM_CDM_AA_BC_DATA(ID,BATCHID,MTRLNUM,DESIGNID,APTINADOCNUMBER,LOTRECDATE,FLAG,CREATEBY,CREATEDATE,INVFLAG,CURRENT_WAFER_QTY,WAFER_ID_STR)
        values(zm_cdm_aa_bc_seq.nextval,'{lot_id}','{fab_device}','{lot_coo}','{invoice_id}','{intr_date}','Y','{username}',NOW(),0,{quantity},'{wafer_id_str}')
        '''
        if not con.exec_n(sql):
            abort(make_response({"ERR_MSG": "BC上传错误"}))

        # map抓取请求发出
        post_map_request(lot_id)

    con.db.commit()
    print(header, items)
    return {'ERR_MSG': err_msg}


# 过滤非BUMP的batch
def is_bump_batch(fab_device):
    con_dw = conn.HanaConnDW()
    sql = f"SELECT DISTINCT KEY1 FROM ZM_CONFIG_TYPE_LIST WHERE CONFIG_TYPE = '2' AND KEY2 = '{fab_device}' "
    results = con_dw.query(sql)
    fab_device_g = xstr(results[0][0]).upper(
    ) if results else fab_device.upper()
    sql = f"SELECT ZZPROCESS FROM VM_SAP_MAT_INFO vsmi WHERE ZZFABXH = '{fab_device_g}' "
    results = con_dw.query(sql)
    if results:
        if "BUMP" in xstr(results[0][0]):
            return True
        else:
            return False
    else:
        return False


# 查询BC数据
def query_bc_data(batid="", non_batch=""):
    res = {'ERR_MSG': "", "ITEMS_DATA": []}
    con = conn.HanaConn()

    # sql
    if batid:
        sql = f""" SELECT BATCHID,APTINADOCNUMBER,LOTRECDATE ,MTRLNUM ,CURRENT_WAFER_QTY,DESIGNID,WAFER_ID_STR,CREATEDATE ,CREATEBY 
        FROM ZM_CDM_AA_BC_DATA WHERE FLAG = 'Y' AND BATCHID = '{batid}' """

    else:
        sql = f""" SELECT BATCHID,APTINADOCNUMBER,LOTRECDATE ,MTRLNUM ,CURRENT_WAFER_QTY,DESIGNID,WAFER_ID_STR,CREATEDATE ,CREATEBY 
        FROM ZM_CDM_AA_BC_DATA WHERE FLAG = 'Y' and CREATEDATE >= '2019-05-01' """

    if non_batch == "true":
        sql = sql + f" AND CURRENT_WAFER_QTY <> 25 "

    sql = sql + " ORDER BY CREATEDATE desc "

    print(sql)
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = f"查询不到{batid} 已上传BC记录"
    else:
        for row in results:
            item = {}
            item['LOT_ID'] = xstr(row[0])
            item['INVOICE_ID'] = xstr(row[1])
            item['INTR_DATE'] = xstr(row[2])
            item['FAB_DEVICE'] = xstr(row[3])
            item['QUANTITY'] = xstr(row[4])
            item['COO'] = xstr(row[5])

            if non_batch == "true":
                if not is_bump_batch(item['FAB_DEVICE']):
                    continue

            if item['QUANTITY'] == '25':
                item['WAFER_ID_STR'] = "#01~#25"
            else:
                item['WAFER_ID_STR'] = xstr(row[6])

            item['CREATE_DATE'] = xstr(row[7])
            item['CREATE_BY'] = xstr(row[8])

            res["ITEMS_DATA"].append(item)

    return res


# 导出BC数据
def export_bc_data():
    res = {'ERR_MSG': ""}

    # sql
    sql = f"SELECT * FROM ZM_CDM_AA_BC_DATA zcabd WHERE CREATEDATE >= '2018-01-01' ORDER BY CREATEDATE desc "
    file_id = ttx.trans_sql(sql, "US010 BC记录(2018年至今).xlsx")
    print("测试文件:", file_id)
    res['FILE_ID'] = file_id
    return res


# 导出FEDS数据
def export_feds_data():
    res = {'ERR_MSG': ""}

    # sql
    sql = f"SELECT * FROM ZM_CDM_AA_FEDS_DATA ORDER BY ID desc "
    file_id = ttx.trans_sql(sql, "US010 FEDS记录.xlsx")
    print("测试文件:", file_id)
    res['FILE_ID'] = file_id
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
def insert_wafer_data(fab_device, lot_id, wafer_qty):
    con = conn.HanaConn()
    gross_die = get_wafer_gross_die(fab_device)
    if wafer_qty:
        wafer_qty = int(wafer_qty)

        for i in range(wafer_qty):
            wafer_id = ('000' + str(i+1))[-2:]
            lot_wafer_id = lot_id + wafer_id
            wafer_sn = get_rand_id(8)

            sql = f''' INSERT INTO ZM_CDM_WAFER_MAP_BIN_DIES(SOURCE_TYPE,WAFER_ID,WAFER_GROSS_DIES,WAFER_GOOD_DIES,WAFER_NG_DIES,FLAG,CREATE_DATE,CREATE_BY,CUST_CODE,LOT_ID,ID,WAFER_NO)  
                VALUES('20','{lot_wafer_id}',{gross_die},{gross_die},0,'1',NOW(),'CDM', 'US010','{lot_id}',zm_cdm_wafer_map_seq.NEXTVAL,'{wafer_id}')
            '''
            print(sql)
            con.exec_c(sql)


# 查询客户机种组名
def get_cust_device_group_name(cust_device):
    con = conn.HanaConnDW()
    sql = f"SELECT DISTINCT KEY1 FROM ZM_CONFIG_TYPE_LIST WHERE CONFIG_TYPE = '1' AND KEY2 = '{cust_device}' "
    results = con.query(sql)
    return xstr(results[0][0]).upper() if results else cust_device.upper()


# 查询FAB机种组名
def get_fab_device_group_name(fab_device):
    con = conn.HanaConnDW()
    sql = f"SELECT DISTINCT KEY1 FROM ZM_CONFIG_TYPE_LIST WHERE CONFIG_TYPE = '2' AND KEY2 = '{fab_device}' "
    results = con.query(sql)
    return xstr(results[0][0]).upper() if results else fab_device.upper()


# 查询物料主数据
def get_mat_master_data(customer_device="", fab_device="", ht_device="", product_no="", sap_product_no="", process="", code="", gross_dies=""):
    res = []
    con = conn.HanaConnDW()

    # 客户机种组
    customer_device_g = get_cust_device_group_name(
        customer_device) if customer_device else ''

    # Fab机种组
    fab_device_g = get_fab_device_group_name(fab_device) if fab_device else ''

    # 查询
    sql = ''' SELECT DISTINCT aa.ZZKHXH,aa.ZZFABXH,aa.ZZHTXH,aa.ZZCNLH,aa.MATNR,aa.ZZPROCESS,aa.ZZEJDM,aa.ZZJYGD,aa.ZZBASESOMO,aa.ZZBZ09,
        aa.ZZLKHZY1,ZZLKHZY2,ZZLKHZY3,aa.ZZLKHZY4,aa.ZZLKHZY5, aa.ZZLCBZ
        FROM VM_SAP_MAT_INFO aa INNER JOIN 
        (SELECT ZZCNLH,max(ERSDA) AS ERSDA FROM VM_SAP_MAT_INFO 
        WHERE ZZCNLH NOT LIKE '%料号%' 
        AND SUBSTRING(ZZCNLH,LENGTH(ZZCNLH)-2,1) <> 'W' 
        AND LENGTH(ZZCNLH) < 16  AND LENGTH(ZZCNLH) > 10   
        AND SUBSTRING(ZZCNLH, LENGTH(ZZCNLH)-2 ,1) <> 'C' 
        AND SUBSTRING(ZZCNLH, LENGTH(ZZCNLH)-2 ,1) <> 'W' and substring(ZZCNLH,1,2) <> '60'
    '''

    sql = sql + \
        f" AND ZZKHXH = '{customer_device_g}' " if customer_device else sql
    sql = sql + \
        f" AND ZZFABXH = '{fab_device_g}' " if fab_device else sql
    sql = sql + \
        f" AND ZZHTXH = '{ht_device}' " if ht_device else sql
    sql = sql + \
        f" AND ZZCNLH = '{product_no}' " if product_no else sql
    sql = sql + \
        f" AND MATNR = '{('000000000000' + sap_product_no)[-18:]}' " if sap_product_no else sql
    sql = sql + \
        f" AND ZZPROCESS = '{process}' " if process else sql
    sql = sql + \
        f" AND ZZEJDM = '{code}' " if code else sql
    sql = sql + \
        f" AND ZZJYGD = '{gross_dies}' " if gross_dies else sql

    sql = sql + \
        "GROUP BY ZZCNLH) bb ON aa.ZZCNLH = bb.ZZCNLH AND aa.ERSDA = bb.ERSDA  "

    results = con.query(sql)
    if not results:
        err_msg = {'ERR_MSG': '', 'ERR_SQL': sql}
        err_msg['ERR_MSG'] = '查不到物料主数据:' + \
            ('<客户机种:' + customer_device + ' 组:' + customer_device_g + '>' if customer_device else '') + \
            ('<FAB机种:' + fab_device + ' 组:' + fab_device_g + '>' if fab_device else '') + \
            ('<Process:' + process + '>' if process else '') + \
            ('<Code:' + code+'>' if code else '')

    for row in results:
        item = {}

        item['SQL'] = sql
        item['ZZKHXH'] = xstr(row[0])
        item['ZZFABXH'] = xstr(row[1])
        item['ZZHTXH'] = xstr(row[2])
        item['ZZCNLH'] = xstr(row[3])
        item['MATNR'] = xstr(row[4]).lstrip('0')
        item['ZZPROCESS'] = xstr(row[5])
        item['ZZEJDM'] = xstr(row[6])
        item['ZZJYGD'] = int(xstr(row[7]))
        item['ZZBASESOMO'] = xstr(row[8])
        item['ZZKHDM'] = xstr(row[9])
        if 'FROZEN' in item['ZZKHDM']:
            continue

        item['ZZLKHZY1'] = xstr(row[10])
        item['ZZLKHZY2'] = xstr(row[11])
        item['ZZLKHZY3'] = xstr(row[12])
        item['ZZLKHZY4'] = xstr(row[13])
        item['ZZLKHZY5'] = xstr(row[14])
        # item['ZZLCBZ'] = xstr(row[15])
        item['ZZLCBZ'] = ""

        res.append(item)

    return res


# 创建map拉取需求
def post_map_request(lot_id):
    sql = f"insert into erptemp.dbo.AA_BC_HISTORY(LOTID,flag,上传日期) values('{lot_id}','0',getdate()) "
    conn.MssConn().exec_c(sql)


# 上传FEDS文件
def import_feds_file(so_file):
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
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg = {"ERR_MSG": f"文件读取失败:{e}"}
        abort(make_response(err_msg))

    items = []
    for index, row in df.iterrows():
        if index == 0:
            continue

        item = {}
        if len(row) != 11:
            err_msg = {"ERR_MSG": f"FEDS模板和设定的模板列数(11列)不一致"}
            abort(make_response(err_msg))

        item['demand_type'] = xstr(row[0])
        item['out_part_id'] = xstr(row[1])
        item['start_part_id'] = xstr(row[2])
        item['start_date'] = row[3]
        item['start_qty'] = row[4]
        item['workweek'] = xstr(row[5])
        item['site_id'] = xstr(row[6])
        item['stage_id'] = xstr(row[7])
        item['ctg'] = xstr(row[8])
        item['pti2'] = xstr(row[9])
        item['comments'] = xstr(row[10])

        mat_data = get_mat_master_data(
            customer_device=item['comments'], fab_device=item['start_part_id'])

        item['product_id'] = ""
        item['ht_pn'] = ""
        for mat in mat_data:
            item['product_id'] = item['product_id'] + mat['ZZCNLH'] + " "
            item['ht_pn'] = item['ht_pn'] + mat['ZZHTXH'] + " "

        items.append(item)

    return {"ERR_MSG": "", "ITEMS": items}


# 提交FEDS数据
def submit_feds_data(data):
    err_msg = ''
    con = conn.HanaConn()
    header = data['header']
    items = data['items']
    username = header['userName']

    # FEDS
    for item in items:
        mat_data = get_mat_master_data(
            customer_device=item['comments'], fab_device=item['start_part_id'])
        if not mat_data:
            abort(make_response(
                {"ERR_MSG": f"客户机种{item['comments']},FAB机种{item['start_part_id']} 找不到物料主数据, 不允许上传, 请联系NPI新建该机种的厂内料号"}))

        if len(mat_data) > 1:
            abort(make_response(
                {"ERR_MSG": f"客户机种{item['comments']},FAB机种{item['start_part_id']} 找到多笔厂内料号, 不允许上传, 请联系NPI确定唯一厂内料号"}))

        product_id = mat_data[0]['ZZCNLH']
        ht_pn = mat_data[0]['ZZHTXH']

        # 删除旧数据
        sql = f"""update ZM_CDM_AA_FEDS_DATA set flag = 'N' where COMMENTS = '{item['comments']}' and START_PART_ID = '{item['start_part_id']}'

        """
        con.exec_n(sql)

        # 插入新数据
        sql = f'''INSERT INTO ZM_CDM_AA_FEDS_DATA(ID,TYPENAME,DEMAND_TYPE,START_PART_ID,OUT_PART_ID,SITE,STAGE, OUT_QTY,WORKWEEK,SITE_ID,STAGE_ID,CTG,PTI2,COMMENTS,FLAG,QTECH_CREATED_BY,QTECH_CREATED_DATE)
        values(zm_cdm_aa_bc_seq.nextval,'FEDS','ORDER_NEW','{item['start_part_id']}','{item['out_part_id']}', '{product_id}','{ht_pn}',{item['start_qty']},'{item['workweek']}','{item['site_id']}','{item['stage_id']}',
        '{item['ctg']}','{item['pti2']}','{item['comments']}','Y','{username}',NOW())
        '''
        print(sql)
        if not con.exec_n(sql):
            abort(make_response({"ERR_MSG": "FEDS上传错误"}))

    con.db.commit()
    print(header, items)
    return {'ERR_MSG': err_msg}


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


# 根据客户机种,FAB机种查询出厂内唯一料号
def get_us010_product_info(cust_device, fab_device):

    mat_data = mmi.get_mat_master_data(
        customer_device=cust_device, fab_device=fab_device)
    if len(mat_data) == 0:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device}和FAB机种{fab_device}关联不到厂内成品料号,请确认是否输入错误"}))

    if len(mat_data) > 1:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device}和FAB机种{fab_device}关联到多笔成品料号,请联系IT,确认物料主数据"}))

    product_id = mat_data[0]['ZZCNLH']
    sap_product_id = mat_data[0]['MATNR']
    process = mat_data[0]['ZZPROCESS']
    gross_dies = mat_data[0]['ZZJYGD']
    ht_pn = mat_data[0]['ZZHTXH']

    return product_id, sap_product_id, process, gross_dies, ht_pn


# 获取物料的销售订单
def get_us010_so_info(cust_device, fab_device, product_id):
    con = conn.HanaConn()

    sql = f"""SELECT a.SO_NO,b.SO_ITEM_SN,a.PO_NO FROM ZM_CDM_SO_HEADER a
        INNER JOIN ZM_CDM_SO_ITEM b
        ON a.SO_SN = b.SO_SN 
        WHERE b.PRD_ID = '{product_id}' and a.SO_NO is not null and b.SO_ITEM_SN is not NULL ORDER BY a.SO_NO desc
    """

    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device}和FAB机种{fab_device},对应成品{product_id}, 查询不到销售订单,请联系内勤维护销售订单"}))

    so_id, so_item, po_id = xstr(results[0][0]), xstr(
        results[0][1]), xstr(results[0][2])
    return so_id, so_item, po_id


# 获取US010 FEDS数据
def get_us010_feds_data(cust_device, fab_device):
    con = conn.HanaConn()

    sql = f"""SELECT * FROM ZM_CDM_AA_FEDS_DATA zcafd WHERE COMMENTS = '{cust_device}' AND START_PART_ID = '{fab_device}'  AND FLAG = 'Y'  ORDER BY id desc"""
    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device}和FAB机种{fab_device}, 查询不到FEDS数据,请联系内勤上传FEDS文件"}))


def get_us010_bc_data(cust_device, fab_device):
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    # sql = f""" SELECT a.BATCHID,a.DESIGNID,a.CURRENT_WAFER_QTY,a.WAFER_ID_STR  FROM ZM_CDM_AA_BC_DATA a WHERE a.MTRLNUM = '{fab_device}' AND a.FLAG = 'Y'
    # AND NOT EXISTS (SELECT 1 FROM ZM_CDM_PO_ITEM b WHERE b.LOT_ID = a.BATCHID AND b.FLAG='1' AND b.add_8 in ('SO','MAP','HTKS')) and  to_char(a.CREATEDATE,'YYYY-MM-DD')  > '2020-01-01'
    #  """
    sql = f""" SELECT distinct a.BATCHID,a.DESIGNID,a.CURRENT_WAFER_QTY,a.WAFER_ID_STR,a.ID  FROM ZM_CDM_AA_BC_DATA a WHERE a.MTRLNUM = '{fab_device}' AND a.FLAG = 'Y' 
    AND NOT EXISTS (SELECT 1 FROM ZM_CDM_PO_ITEM b WHERE b.LOT_ID = a.BATCHID AND b.FLAG='1' ) and to_char(a.CREATEDATE,'YYYY-MM-DD')  > '2020-01-01'
     """
    # print(sql)
    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device}和FAB机种{fab_device}, 查询不到BC数据,请联系内勤上传BC文件"}))

    bc_data = []

    for row in results:
        bc = {}
        bc['BATCHID'] = xstr(row[0])
        bc['DESIGNID'] = xstr(row[1])
        bc['CURRENT_WAFER_QTY'] = row[2]
        bc['WAFER_ID_STR'] = xstr(row[3])
        bc['BATHSN'] = xstr(row[4])

        # 查询是否有库存
        sql = f"SELECT * FROM ZKTMM0001 WHERE ZWAFER_LOT = '{bc['BATCHID']}' AND WERKS = '1200' AND SUBSTRING(MATNR,11,1) = '4' "
        results2 = con_dw.query(sql)
        if not results2:
            continue

        bc_data.append(bc)

    return bc_data


# 获取机种MPN信息
def get_us010_mpn(cust_device):
    con = conn.OracleConn()

    sql = f"SELECT MARKINGCODEFIRST FROM CUSTOMERMPNATTRIBUTES  where PART='{cust_device}' "
    results = con.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device}和, 查询不到MPN数据,请联系NPI维护机种MPN信息,否则打标码数据缺失"}))

    mark_code_first = xstr(results[0][0])
    if not mark_code_first:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device} ,MPN表的打标码数据缺失,请联系NPI维护"}))

    return mark_code_first


# 获取GC转NORMAL订单明细
def query_GC_NORMAL_po(data):
    print(data)
    con = conn.HanaConnDW()
    con_n = conn.HanaConn()
    res = {"ERR_MSG": "", "RET_DATA": []}

    user_name = data.get('userName', '').strip()
    lot_id = data.get('lotID', '').strip()

    print(user_name, lot_id)
    # 查询
    sql = f""" SELECT distinct a.LOT_ID,a.WAFER_ID ,a.PRODUCT_PN ,d.add_5,a.WAFER_SN FROM ZM_CDM_PO_ITEM a
            INNER JOIN VM_SAP_MAT_INFO b ON a.PRODUCT_PN = b.ZZCNLH 
            inner JOIN ZKTMM0001 c ON c.MATNR = b.MATNR AND c.ZWAFER_LOT = a.LOT_ID AND c.ZWAFER_ID = a.LOT_WAFER_ID 
            LEFT JOIN ZM_CDM_PO_ITEM d ON a.LOT_id = d.LOT_ID AND a.WAFER_ID = d.WAFER_ID  AND d.PRODUCT_PN <> a.PRODUCT_PN 
            WHERE a.LOT_ID  ='{lot_id}' AND b.ZZPROCESS = 'WLA' 
            ORDER BY a.LOT_ID ,a.WAFER_ID 
        """
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到该lot可转NORMAL的明细,请确认WLA段是否已经入库"
        return res

    for row in results:
        item = {}
        item['LOT_ID'] = xstr(row[0])
        item['WAFER_ID'] = xstr(row[1])
        item['WLA_PN'] = xstr(row[2])
        item['TURN_PROCESS'] = xstr(row[3])
        item['WAFER_SN'] = xstr(row[4])
        item['USER_NAME'] = user_name

        # 排除新生成的
        sql = f"select * from zm_cdm_po_item where lot_id = '{item['LOT_ID']}' and wafer_id = '{item['WAFER_ID']}' and add_5 = 'TURN_NORMAL' "
        results = con_n.query(sql)
        if results:
            item['TURN_PROCESS'] = 'TURN_NORMAL'

        res['RET_DATA'].append(item)

    return res


# 获取TRUN NORMAL料号
def get_TURN_NORMAL_info(con_dw, wla_pn):
    sql = f"""SELECT DISTINCT b.MATNR,b.ZZCNLH,b.ZZLCBZ,b.ZZHTXH FROM VM_SAP_MAT_INFO A 
    INNER JOIN VM_SAP_MAT_INFO b ON a.ZZKHXH = b.ZZKHXH and a.ZZFABXH = b.ZZFABXH
    WHERE a.ZZCNLH = '{wla_pn}' AND b.ZZPROCESS = 'TURN_NORMAL'
    """
    results = con_dw.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"{wla_pn}找不到对应的TURN_NORMAL料号", "ERR_SQL": sql}))

    if len(results) > 1:
        abort(make_response(
            {"ERR_MSG": f"{wla_pn}找到多笔TURN_NORMAL料号,料号异常", "ERR_SQL": sql}))

    turn_nor_sap_pn = xstr(results[0][0])  # SAP料号
    turn_nor_pn = xstr(results[0][1])  # 厂内料号
    turn_nor_lcbz = xstr(results[0][2])  # 量产/样品订单
    if turn_nor_lcbz == "Y":
        turn_nor_lcbz = "ZOR3"
    elif turn_nor_lcbz == "N":
        turn_nor_lcbz = "ZOR1"
    else:
        turn_nor_lcbz = "ZOR3"

    turn_nor_ht_pn = xstr(results[0][3])  # 厂内机种

    return turn_nor_sap_pn, turn_nor_pn, turn_nor_lcbz, turn_nor_ht_pn


# 生成转NORMAL订单数据
def create_turn_normal_po(po_data):
    res = {"ERR_MSG": "", "RET_DATA": []}
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    print(po_data)

    upload_id = get_rand_id(8)

    # 插入订单数据
    for row in po_data:
        lot_id = row['LOT_ID']
        wafer_id = row['WAFER_ID']
        wafer_sn = row['WAFER_SN']
        user_name = row['USER_NAME']
        wla_pn = row['WLA_PN']

        if row['TURN_PROCESS']:
            abort(make_response(
                {"ERR_MSG": f"{lot_id}{wafer_id}已经转过NORMAL或已经是WLT了,无法再次转订单"}))

        turn_nor_sap_pn, turn_nor_pn, turn_nor_lcbz, turn_nor_ht_pn = get_TURN_NORMAL_info(
            con_dw, wla_pn)

        insert_new_turn_po(con, upload_id, wafer_sn,  turn_nor_sap_pn,
                           turn_nor_pn, turn_nor_lcbz, turn_nor_ht_pn, user_name)

    # 生成SO
    create_so(con, upload_id)

    # 数据提交
    con.db.commit()
    return res


# 插入转NORMAL订单
def insert_new_turn_po(con, upload_id, wafer_sn,  turn_nor_sap_pn, turn_nor_pn, turn_nor_lcbz, turn_nor_ht_pn, user_name):
    # 查询WLA段数据
    sql = f"""INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE, SAP_CUST_CODE,TRAD_CUST_CODE, PO_ID,PO_TYPE ,PO_DATE, BONDED,CUSTOMER_DEVICE,FAB_DEVICE,CUST_FAB_DEVICE_1,
    CUST_FAB_DEVICE_2,HT_PN ,PRODUCT_PN ,SAP_PRODUCT_PN ,LOT_ID,WAFER_ID,LOT_WAFER_ID, PASSBIN_COUNT ,FAILBIN_COUNT,
    MARK_CODE ,ADD_0 ,ADD_1 ,add_2, add_3, add_4,add_5,add_6,add_7,add_8,add_9,add_10,FLAG,FLAG2,FLAG3 ,CREATE_DATE,CREATE_BY,ID,WAFER_SN,WAFER_TIMES,UPLOAD_ID)
    SELECT CUST_CODE, SAP_CUST_CODE,TRAD_CUST_CODE, PO_ID,'{turn_nor_lcbz}',PO_DATE, BONDED,CUSTOMER_DEVICE,FAB_DEVICE,CUST_FAB_DEVICE_1,
    CUST_FAB_DEVICE_2,'{turn_nor_ht_pn}','{turn_nor_pn}','{turn_nor_sap_pn}',LOT_ID,WAFER_ID,REPLACE( LOT_WAFER_ID,'+',''), PASSBIN_COUNT ,FAILBIN_COUNT,
    MARK_CODE ,ADD_0 ,ADD_1 ,add_2, add_3, add_4,'TURN_NORMAL',add_6,add_7,add_8,add_9,add_10,'1','0','1',now(),'{user_name}',1,zm_cdm_wafer_sn_seq_new.nextval,
    WAFER_TIMES||'+','{upload_id}'
    FROM ZM_CDM_PO_ITEM zcpi WHERE WAFER_SN = '{wafer_sn}'
    """
    print(sql)
    con.exec_n(sql)


# 获取US010 PO数据
def query_us010_po(data):
    print(data)
    res = {"ERR_MSG": "", "RET_DATA": []}

    user_name = data.get('userName', '').strip()
    cust_device = data.get('custDevice', '').strip()
    fab_device = data.get('fabDevice', '').strip()

    # print(user_name, cust_device, fab_device)

    # 1.获取物料主数据信息
    product_id, sap_product_id, process, gross_dies, ht_pn = get_us010_product_info(
        cust_device, fab_device)

    # print(product_id, sap_product_id, process, gross_dies, ht_pn)
    if not 'TSV' in process and not 'WLP' in process:
        abort(make_response(
            {"ERR_MSG": f"您输入的客户机种{cust_device}和FAB机种{fab_device}, 对应的成品process:{process},既不属于TSV,也不属于BUMP+WLP , 请联系IT"}))

    # 2.获取销售订单
    so_id, so_item, po_id = get_us010_so_info(
        cust_device, fab_device, product_id)
    # print(so_id, so_item, po_id)

    # 3.获取FEDS
    get_us010_feds_data(cust_device, fab_device)

    # 4.获取BC
    bc_data = get_us010_bc_data(cust_device, fab_device)
    # print(bc_data)

    # 5.判断PROCESS, 确定是否需要map
    if "TSV" in process:
        # 查询map数据
        # pass
        # 获取机种打标码
        mark_code_first = get_us010_mpn(cust_device)
        # print(mark_code_first)
        for row in bc_data:
            wafer_list_str, map_data_list = get_us010_map_data(row['BATCHID'])
            if not map_data_list:
                continue

            # print(row['BATCHID'], wafer_list_str)

            ret_data = {}
            ret_data['map_data'] = map_data_list
            ret_data['lot_id'] = row['BATCHID']
            ret_data['so_id'] = so_id
            ret_data['so_item'] = so_item
            ret_data['po_id'] = po_id
            ret_data['wafer_id'] = wafer_list_str
            ret_data['passbin_cnt'] = gross_dies
            ret_data['failbin_cnt'] = 0
            ret_data['gross_cnt'] = ret_data['passbin_cnt'] + \
                ret_data['failbin_cnt']
            ret_data['add_2'] = row['DESIGNID']
            ret_data['cust_device'] = cust_device
            ret_data['fab_device'] = fab_device
            ret_data['ht_pn'] = ht_pn
            ret_data['sap_product_id'] = sap_product_id
            ret_data['product_id'] = product_id
            ret_data['mark_code'] = ""
            ret_data['create_by'] = user_name
            ret_data['wafer_qty'] = row['CURRENT_WAFER_QTY']
            ret_data['mark_code_first'] = mark_code_first
            ret_data['batch_sn'] = row['BATHSN']

            # print(ret_data)
            res['RET_DATA'].append(ret_data)

    else:
        # 针对满lot的, 长出25片数据
        for row in bc_data:
            # 返回对象
            ret_data = {}
            ret_data['lot_id'] = row['BATCHID']
            ret_data['so_id'] = so_id
            ret_data['so_item'] = so_item
            ret_data['po_id'] = po_id
            ret_data['wafer_id'] = "#1 ~ #25" if row['CURRENT_WAFER_QTY'] == 25 else row['WAFER_ID_STR']
            ret_data['passbin_cnt'] = gross_dies
            ret_data['failbin_cnt'] = 0
            ret_data['gross_cnt'] = ret_data['passbin_cnt'] + \
                ret_data['failbin_cnt']
            ret_data['add_2'] = row['DESIGNID']
            ret_data['cust_device'] = cust_device
            ret_data['fab_device'] = fab_device
            ret_data['ht_pn'] = ht_pn
            ret_data['sap_product_id'] = sap_product_id
            ret_data['product_id'] = product_id
            ret_data['mark_code'] = ""
            ret_data['create_by'] = user_name
            ret_data['wafer_qty'] = row['CURRENT_WAFER_QTY']
            ret_data['batch_sn'] = row['BATHSN']

            # print(ret_data)
            res['RET_DATA'].append(ret_data)

    if not res['RET_DATA']:
        res['ERR_MSG'] = "查询不到可用订单"
    return res


def get_us010_map_data(lot_id):
    con = conn.HanaConn()
    sql = f"SELECT LOT_ID ,WAFER_NO ,WAFER_ID ,WAFER_GOOD_DIES,WAFER_NG_DIES FROM ZM_CDM_WAFER_MAP_BIN_DIES zcwmbd WHERE LOT_ID = '{lot_id}' AND FLAG = '1' order by WAFER_NO"
    results = con.query(sql)
    if not results:
        return None, None

    map_data_list = []
    wafer_list = []
    for row in results:
        map_data = {}
        map_data['lot_wafer_id'] = xstr(row[2])
        map_data['passbin_cnt'] = row[3]
        map_data['failbin_cnt'] = row[4]
        map_data_list.append(map_data)
        wafer_list.append(xstr(row[1]))

    wafer_list_str = ",".join(wafer_list)

    return wafer_list_str, map_data_list


# 生成订单数据
def create_us010_po(po_data):
    res = {"ERR_MSG": "", "RET_DATA": []}
    con = conn.HanaConn()
    con_or = conn.OracleConn()

    # print(po_data)
    upload_id = get_rand_id(8)

    for row in po_data:
        if "map_data" in row:
            for wafer_obj in row["map_data"]:
                save_us010_po_2(con, upload_id, row, wafer_obj, con_or)
        else:
            po_item = {}
            po_item['wafer_id_str'] = row['wafer_id']
            wafer_id_list = get_wafer_id_list(po_item)

            if len(wafer_id_list) != row['wafer_qty']:
                res['ERR_MSG'] = f"{row['lot_id']}:晶圆ID未维护,请联系内勤补充片号(非满25需要维护具体片号)"
                return res

            print(wafer_id_list)
            for wafer_id in wafer_id_list:
                save_us010_po(con, upload_id, row, wafer_id)

    # 数据提交
    con.db.commit()
    return res


# 保存订单数据
def save_us010_po(con, upload_id, row_data, wafer_id):
    lot_wafer_id = row_data['lot_id'] + wafer_id

    sql = f"""INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,PO_ID,PO_TYPE, CUSTOMER_DEVICE ,FAB_DEVICE,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID, WAFER_ID, LOT_WAFER_ID,PASSBIN_COUNT, FAILBIN_COUNT, MARK_CODE, add_2,SO_ID ,SO_ITEM , FLAG , FLAG2,FLAG3 ,CREATE_DATE,
    CREATE_BY,UPLOAD_ID, WAFER_SN, ID, ADD_8,ADD_10) 
    values('US010','0000200115','{row_data['po_id']}','ZOR3','{row_data['cust_device']}','{row_data['fab_device']}','{row_data['ht_pn']}','{row_data['product_id']}','{row_data['sap_product_id']}','{row_data['lot_id']}','{wafer_id}','{lot_wafer_id}',{row_data['passbin_cnt']},
    {row_data['failbin_cnt']},'{row_data['mark_code']}','{row_data['add_2']}','{row_data['so_id']}','{row_data['so_item']}','1','0','0',NOW(),'{row_data['create_by']}','{upload_id}',zm_cdm_wafer_sn_seq_new.nextval,1,'SO','{row_data['batch_sn']}')
    """

    print(sql)
    con.exec_n(sql)


def get_us010_tsv_mark_code(mpn_code, lot_wafer_id, con_or):
    sql = f"SELECT ONMarkingCodeSeq.QTSeq('{lot_wafer_id}') from dual "
    results = con_or.query(sql)
    if not results:
        abort(make_response(
            {"ERR_MSG": f"晶圆:{lot_wafer_id}无法获取到打标码"}))

    mark_code = mpn_code + xstr(results[0][0])
    if len(mark_code) != 10:
        abort(make_response(
            {"ERR_MSG": f"晶圆:{lot_wafer_id}打标码{mark_code}, 长度不正确,正确的是10位"}))

    return mark_code


# 保存订单数据
def save_us010_po_2(con, upload_id, row_data, wafer_obj, con_or):
    lot_wafer_id = wafer_obj['lot_wafer_id']
    wafer_id = lot_wafer_id[-4:-2]
    passbin_cnt = wafer_obj['passbin_cnt']
    failbin_cnt = wafer_obj['failbin_cnt']
    mark_code = get_us010_tsv_mark_code(
        row_data['mark_code_first'], lot_wafer_id, con_or)

    sql = f"""INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE,SAP_CUST_CODE,PO_ID,PO_TYPE, CUSTOMER_DEVICE ,FAB_DEVICE,HT_PN,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID, WAFER_ID, LOT_WAFER_ID,PASSBIN_COUNT, FAILBIN_COUNT, MARK_CODE, add_2,SO_ID ,SO_ITEM , FLAG , FLAG2,FLAG3 ,CREATE_DATE,
    CREATE_BY,UPLOAD_ID, WAFER_SN, ID, ADD_8,ADD_10,WAFER_TIMES) 
    values('US010','0000200115','{row_data['po_id']}','ZOR3','{row_data['cust_device']}','{row_data['fab_device']}','{row_data['ht_pn']}','{row_data['product_id']}','{row_data['sap_product_id']}','{row_data['lot_id']}','{wafer_id}','{lot_wafer_id}',{passbin_cnt},
    {failbin_cnt},'{mark_code}','{row_data['add_2']}','{row_data['so_id']}','{row_data['so_item']}','1','0','1',NOW(),'{row_data['create_by']}','{upload_id}',zm_cdm_wafer_sn_seq_new.nextval,1,'SO','{row_data['batch_sn']}','')
    """

    print(sql)
    con.exec_n(sql)


# -----------------------------------------------------------------------------------------特殊对照表维护--------------------------------------

# 获取维护数据
def get_common_file(header_data):
    if header_data['mt_type'] == "MT01":
        res = get_MT_01_Data(header_data)
    elif header_data['mt_type'] == "MT02":
        res = get_MT_02_Data(header_data)
    elif header_data['mt_type'] == "MT03":
        res = get_MT_03_Data(header_data)
    elif header_data['mt_type'] == "MT04":
        res = get_MT_04_Data(header_data)

    elif header_data['mt_type'] == "MT05":
        res = get_MT_05_Data(header_data)
    return res


# 获取维护数据
def get_MT_01_Data(data):
    print(data)
    res = {"ERR_MSG": "", "ITEMS_DATA": [], "TOTAL_QTY": 0}
    con = conn.HanaConn()

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'BLINE',t1.value,'' ) ) AS BLINE,
            MAX(map( t1.SUB_ID,'CODE',t1.value,'' ) ) AS CODE,
            MAX(map( t1.SUB_ID,'STATUS',t1.value,'' ) ) AS STATUS,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT01' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'BLINE',t1.value,'' ) ) AS BLINE,
            MAX(map( t1.SUB_ID,'CODE',t1.value,'' ) ) AS CODE,
            MAX(map( t1.SUB_ID,'STATUS',t1.value,'' ) ) AS STATUS,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT01'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "查询不到数据"}))

    # 总数
    res['TOTAL_QTY'] = len(results)

    # 当前页数量
    limit = int(data['page_size'])
    if data['current_page'] == '1':
        offset = 0
    else:
        offset = (int(data['current_page']) - 1) * limit

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'BLINE',t1.value,'' ) ) AS BLINE,
            MAX(map( t1.SUB_ID,'CODE',t1.value,'' ) ) AS CODE,
            MAX(map( t1.SUB_ID,'STATUS',t1.value,'' ) ) AS STATUS,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT01' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'BLINE',t1.value,'' ) ) AS BLINE,
            MAX(map( t1.SUB_ID,'CODE',t1.value,'' ) ) AS CODE,
            MAX(map( t1.SUB_ID,'STATUS',t1.value,'' ) ) AS STATUS,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT01'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """

    results2 = con.query(sql)
    for row in results2:
        mt_data = {}
        mt_data['DEVICE'] = xstr(row[0])
        mt_data['BLINE'] = xstr(row[1])
        mt_data['CODE'] = xstr(row[2])
        mt_data['STATUS'] = xstr(row[3])
        mt_data['CREATE_DATE'] = xstr(row[4])
        mt_data['MT_ID'] = row[5]
        res["ITEMS_DATA"].append(mt_data)

    return res


def get_MT_02_Data(data):
    print(data)
    res = {"ERR_MSG": "", "ITEMS_DATA": [], "TOTAL_QTY": 0}
    con = conn.HanaConn()

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'NCMR',t1.value,'' ) ) AS NCMR,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT02' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'NCMR',t1.value,'' ) ) AS NCMR,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT02'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "查询不到数据"}))

    # 总数
    res['TOTAL_QTY'] = len(results)

    # 当前页数量
    limit = int(data['page_size'])
    if data['current_page'] == '1':
        offset = 0
    else:
        offset = (int(data['current_page']) - 1) * limit

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'NCMR',t1.value,'' ) ) AS NCMR,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT02' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'NCMR',t1.value,'' ) ) AS NCMR,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT02'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """

    results2 = con.query(sql)
    for row in results2:
        mt_data = {}
        mt_data['WAFER_ID'] = xstr(row[0])
        mt_data['NCMR'] = xstr(row[1])
        mt_data['CREATE_DATE'] = xstr(row[2])
        mt_data['MT_ID'] = row[3]
        res["ITEMS_DATA"].append(mt_data)

    return res


def get_MT_03_Data(data):
    print(data)
    res = {"ERR_MSG": "", "ITEMS_DATA": [], "TOTAL_QTY": 0}
    con = conn.HanaConn()

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'MARKING_CODE',t1.value,'' ) ) AS MARKING_CODE,
            MAX(map( t1.SUB_ID ,'DEVICE_NAME',t1.value,'' ) ) AS DEVICE_NAME,
            MAX(map( t1.SUB_ID ,'PRODUCT_12NC',t1.value,'' ) ) AS PRODUCT_12NC,
            MAX(map( t1.SUB_ID ,'PMC',t1.value,'' ) ) AS PMC,
            MAX(map( t1.SUB_ID ,'ORIG',t1.value,'' ) ) AS ORIG,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PROVENANCE',t1.value,'' ) ) AS PROVENANCE,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_01',t1.value,'' ) ) AS EU010_ATTR_01,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_02',t1.value,'' ) ) AS EU010_ATTR_02,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT03' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'MARKING_CODE',t1.value,'' ) ) AS MARKING_CODE,
            MAX(map( t1.SUB_ID ,'DEVICE_NAME',t1.value,'' ) ) AS DEVICE_NAME,
            MAX(map( t1.SUB_ID ,'PRODUCT_12NC',t1.value,'' ) ) AS PRODUCT_12NC,
            MAX(map( t1.SUB_ID ,'PMC',t1.value,'' ) ) AS PMC,
            MAX(map( t1.SUB_ID ,'ORIG',t1.value,'' ) ) AS ORIG,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PROVENANCE',t1.value,'' ) ) AS PROVENANCE,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_01',t1.value,'' ) ) AS EU010_ATTR_01,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_02',t1.value,'' ) ) AS EU010_ATTR_02,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT03'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "查询不到数据"}))

    # 总数
    res['TOTAL_QTY'] = len(results)

    # 当前页数量
    limit = int(data['page_size'])
    if data['current_page'] == '1':
        offset = 0
    else:
        offset = (int(data['current_page']) - 1) * limit

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'MARKING_CODE',t1.value,'' ) ) AS MARKING_CODE,
            MAX(map( t1.SUB_ID ,'DEVICE_NAME',t1.value,'' ) ) AS DEVICE_NAME,
            MAX(map( t1.SUB_ID ,'PRODUCT_12NC',t1.value,'' ) ) AS PRODUCT_12NC,
            MAX(map( t1.SUB_ID ,'PMC',t1.value,'' ) ) AS PMC,
            MAX(map( t1.SUB_ID ,'ORIG',t1.value,'' ) ) AS ORIG,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PROVENANCE',t1.value,'' ) ) AS PROVENANCE,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_01',t1.value,'' ) ) AS EU010_ATTR_01,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_02',t1.value,'' ) ) AS EU010_ATTR_02,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT03' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'MARKING_CODE',t1.value,'' ) ) AS MARKING_CODE,
            MAX(map( t1.SUB_ID ,'DEVICE_NAME',t1.value,'' ) ) AS DEVICE_NAME,
            MAX(map( t1.SUB_ID ,'PRODUCT_12NC',t1.value,'' ) ) AS PRODUCT_12NC,
            MAX(map( t1.SUB_ID ,'PMC',t1.value,'' ) ) AS PMC,
            MAX(map( t1.SUB_ID ,'ORIG',t1.value,'' ) ) AS ORIG,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PROVENANCE',t1.value,'' ) ) AS PROVENANCE,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_01',t1.value,'' ) ) AS EU010_ATTR_01,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_02',t1.value,'' ) ) AS EU010_ATTR_02,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT03'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """

    results2 = con.query(sql)
    for row in results2:
        mt_data = {}
        mt_data['DEVICE'] = xstr(row[0])
        mt_data['MARKING_CODE'] = xstr(row[1])
        mt_data['DEVICE_NAME'] = xstr(row[2])
        mt_data['PRODUCT_12NC'] = xstr(row[3])
        mt_data['PMC'] = xstr(row[4])
        mt_data['ORIG'] = xstr(row[5])
        mt_data['PACKAGE'] = xstr(row[6])
        mt_data['PROVENANCE'] = xstr(row[7])
        mt_data['EU010_ATTR_01'] = xstr(row[8])
        mt_data['EU010_ATTR_02'] = xstr(row[9])
        mt_data['CREATE_DATE'] = xstr(row[10])
        mt_data['MT_ID'] = row[11]
        res["ITEMS_DATA"].append(mt_data)

    return res


def get_MT_04_Data(data):
    print(data)
    res = {"ERR_MSG": "", "ITEMS_DATA": [], "TOTAL_QTY": 0}
    con = conn.HanaConn()

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT04' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT04'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "查询不到数据"}))

    # 总数
    res['TOTAL_QTY'] = len(results)

    # 当前页数量
    limit = int(data['page_size'])
    if data['current_page'] == '1':
        offset = 0
    else:
        offset = (int(data['current_page']) - 1) * limit

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT04' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT04'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """

    results2 = con.query(sql)
    for row in results2:
        mt_data = {}
        mt_data['DEVICE'] = xstr(row[0])
        mt_data['PACKAGE'] = xstr(row[1])
        mt_data['CREATE_DATE'] = xstr(row[2])
        mt_data['MT_ID'] = row[3]
        res["ITEMS_DATA"].append(mt_data)

    return res


# US010 MPN信息
def get_MT_05_Data(data):
    print(data)
    res = {"ERR_MSG": "", "ITEMS_DATA": [], "TOTAL_QTY": 0}
    con = conn.HanaConn()

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'LOC',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'LEAD_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'ECAT',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MSL',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'TEMP',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'HALIDE_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PBF_DIE_ATTACH',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MPQ_QTY',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PACKAGING_TYPE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PKG_GRP_CD',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'UL_LISTED_FLAG',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MARKINGCODEFIRST',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'IPN',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT05' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'LOC',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'LEAD_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'ECAT',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MSL',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'TEMP',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'HALIDE_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PBF_DIE_ATTACH',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MPQ_QTY',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PACKAGING_TYPE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PKG_GRP_CD',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'UL_LISTED_FLAG',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MARKINGCODEFIRST',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'IPN',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT05'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc
        """
    results = con.query(sql)
    if not results:
        abort(make_response({"ERR_MSG": "查询不到数据"}))

    # 总数
    res['TOTAL_QTY'] = len(results)

    # 当前页数量
    limit = int(data['page_size'])
    if data['current_page'] == '1':
        offset = 0
    else:
        offset = (int(data['current_page']) - 1) * limit

    if data['mt_attr1']:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'LOC',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'LEAD_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'ECAT',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MSL',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'TEMP',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'HALIDE_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PBF_DIE_ATTACH',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MPQ_QTY',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PACKAGING_TYPE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PKG_GRP_CD',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'UL_LISTED_FLAG',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MARKINGCODEFIRST',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'IPN',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT05' and t1.MAIN_ID = '{data['mt_attr1']}'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """
    else:
        sql = f"""SELECT 
            t1.MAIN_ID ,
            MAX(map( t1.SUB_ID ,'LOC',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'LEAD_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'ECAT',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MSL',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'TEMP',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'HALIDE_FREE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PBF_DIE_ATTACH',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MPQ_QTY',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PACKAGING_TYPE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'PKG_GRP_CD',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'UL_LISTED_FLAG',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'MARKINGCODEFIRST',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'IPN',t1.value,'' ) ) AS PACKAGE,
            to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT05'
            group by t1.MAIN_ID,to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi'),t1.MT_ID 
            ORDER BY to_char(t1.UPDATE_DATE,'YYYY-MM-DD hh24:mi') desc LIMIT {limit} OFFSET {offset}
        """

    results2 = con.query(sql)
    for row in results2:
        mt_data = {}
        mt_data['DEVICE'] = xstr(row[0])
        mt_data['LOC'] = xstr(row[1])
        mt_data['LEAD_FREE'] = xstr(row[2])
        mt_data['ECAT'] = xstr(row[3])
        mt_data['MSL'] = xstr(row[4])
        mt_data['TEMP'] = xstr(row[5])
        mt_data['HALIDE_FREE'] = xstr(row[6])
        mt_data['PBF_DIE_ATTACH'] = xstr(row[7])
        mt_data['MPQ_QTY'] = xstr(row[8])
        mt_data['PACKAGING_TYPE'] = xstr(row[9])
        mt_data['PKG_GRP_CD'] = xstr(row[10])
        mt_data['UL_LISTED_FLAG'] = xstr(row[11])
        mt_data['MARKINGCODEFIRST'] = xstr(row[12])
        mt_data['IPN'] = xstr(row[13])
        mt_data['CREATE_DATE'] = xstr(row[14])
        mt_data['MT_ID'] = row[15]
        res["ITEMS_DATA"].append(mt_data)

    return res


# 上传特殊文件
def upload_common_file(header_data, header_file):
    if header_data['type'] == "MT01":
        res = upload_MT01_file(header_data, header_file)
    elif header_data['type'] == "MT02":
        res = upload_MT02_file(header_data, header_file)
    elif header_data['type'] == "MT03":
        res = upload_MT03_file(header_data, header_file)
    elif header_data['type'] == "MT04":
        res = upload_MT04_file(header_data, header_file)

    elif header_data['type'] == "MT05":
        res = upload_MT05_file(header_data, header_file)

    return res


# US337阴极线维护
def upload_MT01_file(header_data, header_file):
    print(header_data, header_file)
    err_msg = {"ERR_MSG": ""}
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    con = conn.HanaConn()

    # 文件名
    doc_file_name = header_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        header_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg["ERR_MSG"] = f"文件读取失败:{e}"
        return err_msg
    success_cnt = 0
    for index, row in df.iterrows():
        if index == 0:
            continue

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        item = {}
        if len(row) != 4:
            err_msg["ERR_MSG"] = f"模板和设定的模板列数(4列)不一致"
            return err_msg

        item['DEVICE'] = xstr(row[0])
        item['BLINE'] = xstr(row[1])
        item['CODE'] = xstr(row[2])
        item['STATUS'] = xstr(row[3])

        if not item['DEVICE']:
            continue

        in_keys = {'BLINE': '阴极线', 'CODE': 'CODE', 'STATUS': 'STATUS'}
        # sql插入
        for key in in_keys:
            sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{item['DEVICE']}','{key}','{item[key]}','MT01','{header_data['user_name']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
            exec_status, exec_msg = con.exec_n_2(sql)
            if not exec_status:
                print(exec_msg)
                err_msg["ERR_MSG"] = str(exec_msg)
                err_msg["ERR_MSG"] = f"客户机种{item['DEVICE']}的数据已经存在,不可重复维护:" + \
                    err_msg["ERR_MSG"]
                return err_msg

        success_cnt = success_cnt + 1

    con.db.commit()
    if not success_cnt:
        err_msg["ERR_MSG"] = "文件没有数据,请检查文件"
        return err_msg

    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"

    return err_msg


# US337 NCMR维护
def upload_MT02_file(header_data, header_file):
    print(header_data, header_file)
    err_msg = {"ERR_MSG": ""}
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    con = conn.HanaConn()

    # 文件名
    doc_file_name = header_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        header_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg["ERR_MSG"] = f"文件读取失败:{e}"
        return err_msg
    success_cnt = 0
    for index, row in df.iterrows():
        if index == 0:
            continue

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        item = {}
        if len(row) != 2:
            err_msg["ERR_MSG"] = f"模板和设定的模板列数(2列)不一致"
            return err_msg

        item['WAFER_ID'] = xstr(row[0])
        item['NCMR'] = xstr(row[1])

        if not item['WAFER_ID']:
            continue

        in_keys = {'NCMR': ''}
        # sql插入
        for key in in_keys:
            mt_c_val = xstr(item[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT02' and MAIN_ID='{item['WAFER_ID']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{header_data['user_name']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                    # 更新mes
                    sql = f"DELETE FROM ZH_MES_REFERENCE WHERE ID = '37_WAFER' AND KEY1='{item['WAFER_ID']}' AND PROPERTY_NAME='NCMR'  "
                    con.exec_n(sql)

                    sql = f"""INSERT INTO ZH_MES_REFERENCE(ID,KEY1,KEY2,KEY3,PROPERTY_NAME,PROPERTY_VALUE,VALUE_FLAG,CREATED_BY ,CREATED_TIME)
                            values('37_WAFER','{item['WAFER_ID']}','NULL','NULL','NCMR','{mt_c_val}','0','{header_data['user_name']}',to_char(now(),'YYYY-MM-DD')) """
                    con.exec_n(sql)

                else:
                    continue
            else:
                # 新增
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{item['WAFER_ID']}','{key}','{item[key]}','MT02','{header_data['user_name']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"片号{item['WAFER_ID']}的数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

                # 更新mes
                sql = f"DELETE FROM ZH_MES_REFERENCE WHERE ID = '37_WAFER' AND KEY1='{item['WAFER_ID']}' AND PROPERTY_NAME='NCMR'  "
                con.exec_n(sql)

                sql = f"""INSERT INTO ZH_MES_REFERENCE(ID,KEY1,KEY2,KEY3,PROPERTY_NAME,PROPERTY_VALUE,VALUE_FLAG,CREATED_BY ,CREATED_TIME)
                        values('37_WAFER','{item['WAFER_ID']}','NULL','NULL','NCMR','{mt_c_val}','0','{header_data['user_name']}',to_char(now(),'YYYY-MM-DD')) """
                con.exec_n(sql)

        success_cnt = success_cnt + 1

    con.db.commit()
    if not success_cnt:
        err_msg["ERR_MSG"] = "文件没有数据,请检查文件"
        return err_msg

    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"

    return err_msg


# EU010 机种信息维护
def upload_MT03_file(header_data, header_file):
    print(header_data, header_file)
    err_msg = {"ERR_MSG": ""}
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    con = conn.HanaConn()

    # 文件名
    doc_file_name = header_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        header_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg["ERR_MSG"] = f"文件读取失败:{e}"
        return err_msg
    success_cnt = 0
    for index, row in df.iterrows():
        if index == 0:
            continue

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        item = {}
        if len(row) != 10:
            err_msg["ERR_MSG"] = f"模板和设定的模板列数(10列)不一致"
            return err_msg

        item['DEVICE'] = xstr(row[0])
        item['MARKING_CODE'] = xstr(row[1])
        item['DEVICE_NAME'] = xstr(row[2])
        item['PRODUCT_12NC'] = xstr(row[3])
        item['PMC'] = xstr(row[4])
        item['ORIG'] = xstr(row[5])
        item['PACKAGE'] = xstr(row[6])
        item['PROVENANCE'] = xstr(row[7])
        item['EU010_ATTR_01'] = xstr(row[8])
        item['EU010_ATTR_02'] = xstr(row[9])

        if not item['DEVICE']:
            continue

        in_keys = {'MARKING_CODE': '', 'DEVICE_NAME': '', 'PRODUCT_12NC': '', 'PMC': '',
                   'ORIG': '', 'PACKAGE': '', 'PROVENANCE': '', 'EU010_ATTR_01': '', 'EU010_ATTR_02': ''}
        # sql插入
        for key in in_keys:
            mt_c_val = xstr(item[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT03' and MAIN_ID='{item['DEVICE']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{header_data['user_name']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                else:
                    continue
            else:
                # 新增
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{item['DEVICE']}','{key}','{item[key]}','MT03','{header_data['user_name']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"机种{item['DEVICE']}的数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

        success_cnt = success_cnt + 1

    con.db.commit()
    if not success_cnt:
        err_msg["ERR_MSG"] = "文件没有数据,请检查文件"
        return err_msg

    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"

    return err_msg


# AC70 机种信息维护
def upload_MT04_file(header_data, header_file):
    print(header_data, header_file)
    err_msg = {"ERR_MSG": ""}
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    con = conn.HanaConn()

    # 文件名
    doc_file_name = header_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        header_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg["ERR_MSG"] = f"文件读取失败:{e}"
        return err_msg
    success_cnt = 0
    for index, row in df.iterrows():
        if index == 0:
            continue

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        item = {}
        if len(row) != 2:
            err_msg["ERR_MSG"] = f"模板和设定的模板列数(2列)不一致"
            return err_msg

        item['DEVICE'] = xstr(row[0])
        item['PACKAGE'] = xstr(row[1])

        if not item['DEVICE']:
            continue

        in_keys = {'PACKAGE': ''}
        # sql插入
        for key in in_keys:
            mt_c_val = xstr(item[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT04' and MAIN_ID='{item['DEVICE']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{header_data['user_name']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                else:
                    continue
            else:
                # 新增
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{item['DEVICE']}','{key}','{item[key]}','MT04','{header_data['user_name']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"机种{item['DEVICE']}的数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

        success_cnt = success_cnt + 1

    con.db.commit()
    if not success_cnt:
        err_msg["ERR_MSG"] = "文件没有数据,请检查文件"
        return err_msg

    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"

    return err_msg


# US010 MPN机种信息维护
def upload_MT05_file(header_data, header_file):
    print(header_data, header_file)
    err_msg = {"ERR_MSG": ""}
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    con = conn.HanaConn()

    # 文件名
    doc_file_name = header_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        header_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    # 解析文件
    try:
        df = pd.read_excel(
            doc_path, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        err_msg["ERR_MSG"] = f"文件读取失败:{e}"
        return err_msg
    success_cnt = 0
    for index, row in df.iterrows():
        if index == 0:
            continue

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        item = {}
        if len(row) != 14:
            err_msg["ERR_MSG"] = f"模板和设定的模板列数(14列)不一致"
            return err_msg

        item['DEVICE'] = xstr(row[0])
        item['LOC'] = xstr(row[1])
        item['LEAD_FREE'] = xstr(row[2])
        item['ECAT'] = xstr(row[3])
        item['MSL'] = xstr(row[4])
        item['TEMP'] = xstr(row[5])
        item['HALIDE_FREE'] = xstr(row[6])
        item['PBF_DIE_ATTACH'] = xstr(row[7])
        item['MPQ_QTY'] = xstr(row[8])
        item['PACKAGING_TYPE'] = xstr(row[9])
        item['PKG_GRP_CD'] = xstr(row[10])
        item['UL_LISTED_FLAG'] = xstr(row[11])
        item['MARKINGCODEFIRST'] = xstr(row[12])
        item['IPN'] = xstr(row[13])

        if not item['DEVICE']:
            continue

        in_keys = {'LOC': '', 'LEAD_FREE': '', 'ECAT': '', 'MSL': '', 'TEMP': '', 'HALIDE_FREE': '', 'PBF_DIE_ATTACH': '',
                   'MPQ_QTY': '', 'PACKAGING_TYPE': '', 'PKG_GRP_CD': '', 'UL_LISTED_FLAG': '', 'MARKINGCODEFIRST': '', 'IPN': ''}
        # sql插入
        for key in in_keys:
            mt_c_val = xstr(item[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT04' and MAIN_ID='{item['DEVICE']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{header_data['user_name']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                else:
                    continue
            else:
                # 新增
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{item['DEVICE']}','{key}','{item[key]}','MT05','{header_data['user_name']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"机种{item['DEVICE']}的数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

        success_cnt = success_cnt + 1

    con.db.commit()
    if not success_cnt:
        err_msg["ERR_MSG"] = "文件没有数据,请检查文件"
        return err_msg

    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"

    return err_msg


# 新增维护数据
def new_MT_Data(mt_data):
    if mt_data['header']['type'] == "MT01":
        # 阴极线
        res = new_MT_01_data(mt_data)

    elif mt_data['header']['type'] == "MT02":
        # NCMR
        res = new_MT_02_data(mt_data)

    elif mt_data['header']['type'] == "MT03":
        # EU010
        res = new_MT_03_data(mt_data)

    elif mt_data['header']['type'] == "MT04":
        # NCMR
        res = new_MT_04_data(mt_data)

    elif mt_data['header']['type'] == "MT05":
        # MPN
        res = new_MT_05_data(mt_data)

    return res


# 新增阴极线数据
def new_MT_01_data(mt_data):
    print(mt_data)

    err_msg = {"ERR_MSG": ""}
    con = conn.HanaConn()
    item = mt_data['items']

    in_keys = {'BLINE': '阴极线', 'CODE': 'CODE', 'STATUS': 'STATUS'}
    success_cnt = 0
    for row in item:
        print(row)
        if not row.get('DEVICE'):
            err_msg["ERR_MSG"] = f"客户机种必须填写"
            return err_msg

        row['DEVICE'] = row['DEVICE'].strip()

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        # sql插入
        for key in in_keys:
            if not key in row:
                continue
            sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{row['DEVICE']}','{key}','{row[key].strip()}','MT01','{mt_data['header']['userName']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
            print(sql)
            exec_status, exec_msg = con.exec_n_2(sql)
            if not exec_status:
                print(exec_msg)
                err_msg["ERR_MSG"] = str(exec_msg)
                err_msg["ERR_MSG"] = f"客户机种{item['DEVICE']}的数据已经存在,不可重复维护:" + \
                    err_msg["ERR_MSG"]
                return err_msg
        success_cnt = success_cnt + 1

    con.db.commit()
    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"
    return err_msg


# NCMR数据新增
def new_MT_02_data(mt_data):
    print(mt_data)

    err_msg = {"ERR_MSG": ""}
    con = conn.HanaConn()
    item = mt_data['items']

    in_keys = {'NCMR': ''}
    success_cnt = 0
    for row in item:
        print(row)

        if not row.get('WAFER_ID'):
            err_msg["ERR_MSG"] = f"片号必须填写"
            return err_msg

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        # sql插入
        for key in in_keys:
            if not key in row:
                continue
            mt_c_val = xstr(row[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT02' and MAIN_ID='{row['WAFER_ID']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{mt_data['header']['userName']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                    # 更新mes
                    sql = f"DELETE FROM ZH_MES_REFERENCE WHERE ID = '37_WAFER' AND KEY1='{row['WAFER_ID']}' AND PROPERTY_NAME='NCMR'  "
                    con.exec_n(sql)

                    sql = f"""INSERT INTO ZH_MES_REFERENCE(ID,KEY1,KEY2,KEY3,PROPERTY_NAME,PROPERTY_VALUE,VALUE_FLAG,CREATED_BY ,CREATED_TIME)
                            values('37_WAFER','{row['WAFER_ID']}','NULL','NULL','NCMR','{mt_c_val}','0','{mt_data['header']['userName']}',to_char(now(),'YYYY-MM-DD')) """
                    con.exec_n(sql)

                else:
                    continue
            else:
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{row['WAFER_ID']}','{key}','{row[key].strip()}','MT02','{mt_data['header']['userName']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                print(sql)
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"片号{item['WAFER_ID']}的NCMR数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

                # 更新mes
                sql = f"DELETE FROM ZH_MES_REFERENCE WHERE ID = '37_WAFER' AND KEY1='{row['WAFER_ID']}' AND PROPERTY_NAME='NCMR'  "
                con.exec_n(sql)

                sql = f"""INSERT INTO ZH_MES_REFERENCE(ID,KEY1,KEY2,KEY3,PROPERTY_NAME,PROPERTY_VALUE,VALUE_FLAG,CREATED_BY ,CREATED_TIME)
                        values('37_WAFER','{row['WAFER_ID']}','NULL','NULL','NCMR','{mt_c_val}','0','{mt_data['header']['userName']}',to_char(now(),'YYYY-MM-DD')) """
                con.exec_n(sql)

        success_cnt = success_cnt + 1

    con.db.commit()
    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"
    return err_msg


# AC70机种信息维护
def new_MT_03_data(mt_data):
    print(mt_data)

    err_msg = {"ERR_MSG": ""}
    con = conn.HanaConn()
    item = mt_data['items']

    in_keys = {'MARKING_CODE': '',	'DEVICE_NAME': '', 'PRODUCT_12NC': '', 'PMC': '',
               'ORIG': '', 'PACKAGE': '', 'PROVENANCE': '', 'EU010_ATTR_01': '', 'EU010_ATTR_02': ''}
    success_cnt = 0
    for row in item:
        print(row)
        if not row.get('DEVICE'):
            err_msg["ERR_MSG"] = f"客户机种必须填写"
            return err_msg
        row['DEVICE'] = row['DEVICE'].strip()

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        # sql插入
        for key in in_keys:
            if not key in row:
                continue

            mt_c_val = xstr(row[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT03' and MAIN_ID='{row['DEVICE']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{mt_data['header']['userName']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                else:
                    continue
            else:
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{row['DEVICE']}','{key}','{row[key].strip()}','MT03','{mt_data['header']['userName']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                print(sql)
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"机种{item['DEVICE']}的相关数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

        success_cnt = success_cnt + 1

    con.db.commit()
    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"
    return err_msg


# AC70机种信息维护
def new_MT_04_data(mt_data):
    print(mt_data)

    err_msg = {"ERR_MSG": ""}
    con = conn.HanaConn()
    item = mt_data['items']

    in_keys = {'PACKAGE': ''}
    success_cnt = 0
    for row in item:

        print(row)
        if not row.get('DEVICE') and row.get('PACKAGE'):
            err_msg["ERR_MSG"] = f"客户机种和PACKAGE必须填写"
            return err_msg

        row['DEVICE'] = row['DEVICE'].strip()

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        # sql插入
        for key in in_keys:
            if not key in row:
                continue
            mt_c_val = xstr(row[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT04' and MAIN_ID='{row['DEVICE']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{mt_data['header']['userName']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                else:
                    continue
            else:
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{row['DEVICE']}','{key}','{row[key].strip()}','MT04','{mt_data['header']['userName']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                print(sql)
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"机种{row['DEVICE']}的PACKAGE数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

        success_cnt = success_cnt + 1

    con.db.commit()
    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"
    return err_msg


# MPN维护
def new_MT_05_data(mt_data):
    print(mt_data)

    err_msg = {"ERR_MSG": ""}
    con = conn.HanaConn()
    item = mt_data['items']

    in_keys = {'LOC': '', 'LEAD_FREE': '', 'ECAT': '', 'MSL': '', 'TEMP': '', 'HALIDE_FREE': '', 'PBF_DIE_ATTACH': '',
               'MPQ_QTY': '', 'PACKAGING_TYPE': '', 'PKG_GRP_CD': '', 'UL_LISTED_FLAG': '', 'MARKINGCODEFIRST': '', 'IPN': ''}
    success_cnt = 0
    for row in item:

        print(row)
        if not row.get('DEVICE'):
            err_msg["ERR_MSG"] = f"客户机种必须填写"
            return err_msg

        mt_id = con.query(
            "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
        # sql插入
        for key in in_keys:
            if not key in row:
                continue
            mt_c_val = xstr(row[key])

            # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
            sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT04' and MAIN_ID='{row['DEVICE']}' and SUB_ID='{key}' "
            results_r = con.query(sql)
            if results_r:
                mt_e_val = xstr(results_r[0][0])
                mt_id = results_r[0][1]
                if mt_c_val != mt_e_val:
                    sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{mt_data['header']['userName']}' where MT_ID = {mt_id} and SUB_ID='{key}' "
                    con.exec_n(sql)

                else:
                    continue
            else:
                sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{row['DEVICE']}','{key}','{row[key].strip()}','MT05','{mt_data['header']['userName']}',NOW(),'{in_keys[key]}',{mt_id},NOW()) "
                print(sql)
                exec_status, exec_msg = con.exec_n_2(sql)
                if not exec_status:
                    print(exec_msg)
                    err_msg["ERR_MSG"] = str(exec_msg)
                    err_msg["ERR_MSG"] = f"机种{row['DEVICE']}的PACKAGE数据已经存在,不可重复维护:" + \
                        err_msg["ERR_MSG"]
                    return err_msg

        success_cnt = success_cnt + 1

    con.db.commit()
    err_msg["EXEC_MSG"] = f"成功维护{success_cnt}笔数据"
    return err_msg


# 删除维护数据
def delete_MT_data(mt_data):
    print(mt_data)
    err_msg = {"ERR_MSG": ""}
    con = conn.HanaConn()
    success_cnt = 0
    del_reason = mt_data['header']['mtDelReason']
    del_by = mt_data['header']['userName']

    for row in mt_data['items']:
        mt_id = row['MT_ID']
        print(mt_id, del_reason)

        # 备份
        sql = f"UPDATE ZM_CDM_COMMON_MAINTAIN_INFO SET UPDATE_BY ='{del_by}', UPDATE_DATE =now(),REMARK ='{del_reason}' WHERE MT_ID = {mt_id} "
        con.exec_n(sql)
        sql = f"INSERT INTO ZM_CDM_COMMON_MAINTAIN_INFO_DELETE SELECT * FROM ZM_CDM_COMMON_MAINTAIN_INFO WHERE MT_ID = {mt_id}"
        con.exec_n(sql)

        # 删除
        sql = f"DELETE FROM ZM_CDM_COMMON_MAINTAIN_INFO WHERE MT_ID = {mt_id}"
        con.exec_n(sql)

        # NCMR
        if mt_data['header']['type'] == "MT02":
            sql = f"DELETE FROM ZH_MES_REFERENCE WHERE ID = '37_WAFER' AND PROPERTY_NAME='NCMR' AND KEY1='{row['WAFER_ID']}' "
            print(sql)
            con.exec_n(sql)

        # 删除计数
        success_cnt = success_cnt + 1

    con.db.commit()

    err_msg["EXEC_MSG"] = f"成功删除{success_cnt}笔数据"
    return err_msg


if __name__ == "__main__":
    data = [{'LOT_ID': '671GUB', 'TURN_PROCESS': '', 'USER_NAME': '07885',
             'WAFER_ID': '19', 'WAFER_SN': '792513', 'WLA_PN': '18X01053N000CF'}]
    create_turn_normal_po(data)
