from web_api_client import get_data_from_web_api
import conn_db
from flask import abort, make_response


def xstr(s):
    return '' if s is None else str(s).strip()


# 查询客户主数据
def get_customer_master_data(cust_code):
    conn = conn_db.HanaConnDW()
    cust_data = {}
    sql = f"SELECT DISTINCT PARTNER FROM VM_SAP_PO_CUSTOMER WHERE ZZYKHH = '{cust_code}' "
    results = conn.query(sql)
    if not results:
        err_msg = {"ERR_MSG": f'查询不到{cust_code}的SAP客户主数据'}
        abort(make_response(err_msg))

    cust_data['sap_cust_code'] = xstr(results[0][0]).lstrip('0')
    return cust_data


# 查询客户机种组名
def get_cust_device_group_name(cust_device):
    conn = conn_db.HanaConnDW()
    sql = f"SELECT DISTINCT KEY1 FROM ZM_CONFIG_TYPE_LIST WHERE CONFIG_TYPE = '1' AND KEY2 = '{cust_device}' "
    results = conn.query(sql)
    return xstr(results[0][0]).upper() if results else cust_device.upper()


# 查询FAB机种组名
def get_fab_device_group_name(fab_device):
    conn = conn_db.HanaConnDW()
    sql = f"SELECT DISTINCT KEY1 FROM ZM_CONFIG_TYPE_LIST WHERE CONFIG_TYPE = '2' AND KEY2 = '{fab_device}' "
    results = conn.query(sql)
    return xstr(results[0][0]).upper() if results else fab_device.upper()


# 查询物料主数据
def get_mat_master_data(customer_device="", fab_device="", ht_device="", product_no="", sap_product_no="", process="", code="", gross_dies=""):
    res = []
    conn = conn_db.HanaConnDW()

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
        " AND ZZBZ09 <> 'FROZEN'  GROUP BY ZZCNLH) bb ON aa.ZZCNLH = bb.ZZCNLH AND aa.ERSDA = bb.ERSDA  "

    # print(sql)
    results = conn.query(sql)
    if not results:
        err_msg = {'ERR_MSG': '', 'ERR_SQL': sql}
        err_msg['ERR_MSG'] = '查不到物料主数据:' + \
            ('<客户机种:' + customer_device + ' 组:' + customer_device_g + '>' if customer_device else '') + \
            ('<FAB机种:' + fab_device + ' 组:' + fab_device_g + '>' if fab_device else '') + \
            ('<Process:' + process + '>' if process else '') + \
            ('<Code:' + code+'>' if code else '')

        abort(make_response(err_msg))

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
        item['ZZKHDM'] = xstr(row[9]) # 料号禁用
        item['CHILDPN'] = get_bom_child_part(item['MATNR'])
        # if 'FROZEN' in item['ZZKHDM']:
        #     continue

        item['ZZLKHZY1'] = xstr(row[10])
        item['ZZLKHZY2'] = xstr(row[11])
        item['ZZLKHZY3'] = xstr(row[12])
        item['ZZLKHZY4'] = xstr(row[13])
        item['ZZLKHZY5'] = xstr(row[14])
        item['ZZLCBZ'] = xstr(row[15])
        # item['ZZLCBZ'] = ""

        res.append(item)

    return res



# 查询物料主数据包括料号已经禁用的
def get_mat_master_data_all(customer_device="", fab_device="", ht_device="", product_no="", sap_product_no="", process="", code="", gross_dies=""):
    res = []
    conn = conn_db.HanaConnDW()

    # 客户机种组
    customer_device_g = get_cust_device_group_name(
        customer_device) if customer_device else ''

    # Fab机种组
    fab_device_g = get_fab_device_group_name(fab_device) if fab_device else ''

    # 查询
    sql = ''' SELECT DISTINCT aa.ZZKHXH,aa.ZZFABXH,aa.ZZHTXH,aa.ZZCNLH,aa.MATNR,aa.ZZPROCESS,aa.ZZEJDM,aa.ZZJYGD,aa.ZZBASESOMO,aa.ZZBZ09,
        aa.ZZLKHZY1,ZZLKHZY2,ZZLKHZY3,aa.ZZLKHZY4,aa.ZZLKHZY5, aa.ZZLCBZ,aa.ZZKHDM
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
        " GROUP BY ZZCNLH) bb ON aa.ZZCNLH = bb.ZZCNLH AND aa.ERSDA = bb.ERSDA  "

    # print(sql)
    results = conn.query(sql)
    if not results:
        err_msg = {'ERR_MSG': '', 'ERR_SQL': sql}
        err_msg['ERR_MSG'] = '查不到物料主数据:' + \
            ('<客户机种:' + customer_device + ' 组:' + customer_device_g + '>' if customer_device else '') + \
            ('<FAB机种:' + fab_device + ' 组:' + fab_device_g + '>' if fab_device else '') + \
            ('<Process:' + process + '>' if process else '') + \
            ('<Code:' + code+'>' if code else '')

        abort(make_response(err_msg))

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
        item['ZZKHDM'] = xstr(row[9]) # 料号禁用
        item['CHILDPN'] = get_bom_child_part(item['MATNR'])
        # if 'FROZEN' in item['ZZKHDM']:
        #     continue

        item['ZZLKHZY1'] = xstr(row[10])
        item['ZZLKHZY2'] = xstr(row[11])
        item['ZZLKHZY3'] = xstr(row[12])
        item['ZZLKHZY4'] = xstr(row[13])
        item['ZZLKHZY5'] = xstr(row[14])
        item['ZZLCBZ'] = xstr(row[15])
        item['ZZCUSTOMER'] = xstr(row[16])

        res.append(item)

    return res


# 根据成品查询BOM里的关联晶圆料号
def get_bom_child_part(sap_product_name):
    con_dw = conn_db.HanaConnDW()
    sap_product_name = ("00000000000000000000000000" + sap_product_name)[-18:]

    sql = f"""SELECT string_agg(b.IDNRK,''',''') FROM VM_SAP_V_MAT a
        INNER JOIN VM_SAP_V_ITEM b on a.STLNR = b.STLNR
        INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = b.IDNRK
        WHERE a.MATNR = '{sap_product_name}'
        AND c.MTART IN ('Z019','Z013','Z015')
        AND a.WERKS = '1200'
        """
    results = con_dw.query(sql)
    if results:
        return xstr(results[0][0]).replace("'", "''")
    else:
        return ""


# 查询CSP物料主数据
def get_mat_master_data_csp(customer_device="", fab_device="", ht_device="", product_no="", sap_product_no="", process="", code="", gross_dies=""):
    res = []
    conn = conn_db.HanaConnDW()

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
        AND ZZHTXH LIKE '%C'
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
        " AND ZZBZ09 <> 'FROZEN'  GROUP BY ZZCNLH) bb ON aa.ZZCNLH = bb.ZZCNLH AND aa.ERSDA = bb.ERSDA  "

    results = conn.query(sql)
    if not results:
        err_msg = {'ERR_MSG': '', 'ERR_SQL': sql}
        err_msg['ERR_MSG'] = '查不到物料主数据:' + \
            ('<客户机种:' + customer_device + ' 组:' + customer_device_g + '>' if customer_device else '') + \
            ('<FAB机种:' + fab_device + ' 组:' + fab_device_g + '>' if fab_device else '') + \
            ('<Process:' + process + '>' if process else '') + \
            ('<Code:' + code+'>' if code else '')

        abort(make_response(err_msg))

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
        # if 'FROZEN' in item['ZZKHDM']:
        #     continue

        item['ZZLKHZY1'] = xstr(row[10])
        item['ZZLKHZY2'] = xstr(row[11])
        item['ZZLKHZY3'] = xstr(row[12])
        item['ZZLKHZY4'] = xstr(row[13])
        item['ZZLKHZY5'] = xstr(row[14])
        item['ZZLCBZ'] = xstr(row[15])
        # item['ZZLCBZ'] = ""

        res.append(item)

    return res


# DC料号
def get_mat_master_data_dc(customer_device="", fab_device="", ht_device="", product_no="", sap_product_no="", process="", code="", gross_dies=""):
    res = []
    conn = conn_db.HanaConnDW()

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
        " AND ZZBZ09 <> 'FROZEN'  GROUP BY ZZCNLH) bb ON aa.ZZCNLH = bb.ZZCNLH AND aa.ERSDA = bb.ERSDA  "

    results = conn.query(sql)
    if not results:
        err_msg = {'ERR_MSG': '', 'ERR_SQL': sql}
        err_msg['ERR_MSG'] = '查不到物料主数据:' + \
            ('<客户机种:' + customer_device + ' 组:' + customer_device_g + '>' if customer_device else '') + \
            ('<FAB机种:' + fab_device + ' 组:' + fab_device_g + '>' if fab_device else '') + \
            ('<Process:' + process + '>' if process else '') + \
            ('<Code:' + code+'>' if code else '')

        abort(make_response(err_msg))

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
        item['ZZLCBZ'] = xstr(row[15])
        # item['ZZLCBZ'] = ""

        res.append(item)

    return res


# 查询物料主数据
def get_mat_data(customer_device="", fab_device="", ht_device="", product_no="", sap_product_no="", process="", code="", gross_dies=""):
    res = []
    conn = conn_db.HanaConnDW()

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
        " AND ZZBZ09 <> 'FROZEN'  GROUP BY ZZCNLH) bb ON aa.ZZCNLH = bb.ZZCNLH AND aa.ERSDA = bb.ERSDA  "

    results = conn.query(sql)
    if not results:
        return None

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
        item['ZZLCBZ'] = xstr(row[15])

        res.append(item)

    return res


# 查询物料库存库存
def get_material_inventory(material_no, material_lot_id, factory="1200"):
    res = {"ERR_MSG": ""}
    if not (material_no and material_lot_id):
        res["ERR_MSG"] = "物料编号和物料批次号不可为空"
        return res

    req_data = {"ITEM": {"MATNR": material_no,
                         "ZWAFER_LOT": material_lot_id, "WERKS": factory}}
    res = get_data_from_web_api("MM108", req_data)
    return res
