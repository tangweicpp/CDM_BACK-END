from logging import logThreads
import conn_db as conn
from flask import abort
from flask import make_response
import pp_set_wafer_id as pp_sw
import rfc


# 字符串转换
def xstr(s):
    return '' if s is None else str(s).strip()


# 工单数据获取
def get_mo_data(mo_query):
    print(mo_query)
    ret = {'err_desc': '', 'header': {}, 'items': []}

    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()

    # 表头数据
    mo_header = get_mo_header_data(con, con_dw, mo_query)
    # 明细数据
    mo_items = get_mo_items_data(con, con_dw, mo_header)
    ret['header'] = mo_header
    ret['items'] = mo_items
    return ret


# 根据npi量产标识获取工单类型
def get_mo_type_by_product_lcbz(con_dw, mo_query):
    product_name = mo_query.get('product_name', '')

    if mo_query.get('product_name_type') == 'P1':
        # 客户机种
        sql = f"SELECT DISTINCT ZZLCBZ FROM MARA WHERE ZZKHXH = '{product_name}' and trim(ZZLCBZ) <> '' "
        results = con_dw.query(sql)
        if results:
            if results[0][0] == "Y":
                mo_query['mo_type'] = "YP02"
            if results[0][0] == "N":
                mo_query['mo_type'] = "YP01"

    elif mo_query.get('product_name_type') == 'P2':
        # 厂内机种
        sql = f"SELECT DISTINCT ZZLCBZ FROM MARA WHERE ZZHTXH = '{product_name}'  and trim(ZZLCBZ) <> '' "
        results = con_dw.query(sql)
        if results:
            if results[0][0] == "Y":
                mo_query['mo_type'] = "YP02"
            if results[0][0] == "N":
                mo_query['mo_type'] = "YP01"

    elif mo_query.get('product_name_type') == 'P3':
        # 成品料号
        sql = f"SELECT DISTINCT ZZLCBZ FROM MARA WHERE ZZCNLH = '{product_name}' and trim(ZZLCBZ) <> '' "
        results = con_dw.query(sql)
        if results:
            if results[0][0] == "Y":
                mo_query['mo_type'] = "YP02"
            if results[0][0] == "N":
                mo_query['mo_type'] = "YP01"


# 工单表头数据
def get_mo_header_data(con, con_dw, mo_query):
    mo_header = {}
    # 预设工单类型
    # if not mo_query['mo_type'] or mo_query['mo_type'] in ("YP01", "YP02"):
    if not mo_query['mo_type']:
        get_mo_type_by_product_lcbz(con_dw, mo_query)

    mo_header['mo_type'] = mo_query['mo_type']
    # 客户代码
    cust_code = mo_query.get('cust_code', '')
    # 工单类型
    mo_type = mo_query.get('mo_type', '')
    # 物料类型
    product_name_type = mo_query.get('product_name_type', '')
    # 物料名
    product_name = mo_query.get('product_name', '')

    # 查询
    sql = f"""SELECT DISTINCT CUST_CODE,PO_TYPE,PRODUCT_PN,CUSTOMER_DEVICE,
    HT_PN,SUBSTRING(SAP_PRODUCT_PN, LENGTH(SAP_PRODUCT_PN)-7)
    FROM ZM_CDM_PO_ITEM WHERE FLAG='1' and FLAG2 = '0'
    AND TRIM(SAP_PRODUCT_PN) <> '' AND TRIM(PRODUCT_PN) <> ''
    AND TRIM(CUSTOMER_DEVICE) <> '' AND TRIM(HT_PN) <> ''
    """
    # 工单类型对应
    # 样品工单=> 样品订单,免费订单
    if mo_type == 'YP01':
        sql = sql + f" AND PO_TYPE in ('ZOR1','ZOR4') "

    # 量产工单=> 量产订单,免费订单
    elif mo_type == 'YP02':
        sql = sql + f" AND PO_TYPE in ('ZOR3','ZOR4') "

    # 厂内重工工单
    elif mo_type == 'YP03':
        sql = sql + f" AND PO_TYPE in ('YP03') "

    # RMA重工工单
    elif mo_type == 'YP04':
        sql = sql + f" AND PO_TYPE in ('ZOR5','ZOR6','YP04') "

    # 硅基工单 CSP工单 DC工单
    elif mo_type[:3] == 'YP1':
        sql = sql + f" AND PO_TYPE = '{mo_type}' "

    # 其他有效订单
    else:
        sql = sql + f" AND SUBSTRING(PO_TYPE,1,3) = 'ZOR' "

    # 机种查询
    sql = sql + \
        f" AND CUST_CODE = '{cust_code}' " if cust_code else sql
    sql = sql + \
        f" AND CUSTOMER_DEVICE = '{product_name}' " if product_name_type == 'P1' else sql
    sql = sql + \
        f" AND HT_PN = '{product_name}' " if product_name_type == 'P2' else sql
    sql = sql + \
        f" AND PRODUCT_PN = '{product_name}' " if product_name_type == 'P3' else sql

    sql = sql + " ORDER BY PO_TYPE DESC"

    results = con.query(sql)
    if not results:
        abort(make_response(
            {'err_desc': '查询不到订单数据,请确保订单已经上传; 或工单类型选择错误', 'err_sql': sql}))

    # if len(results) > 1:
    #     abort(make_response(
    #         {'err_desc': '查询到订单一对多客户代码\成品料号的异常,请输入准确的客户代码+成品料号', 'err_sql': sql}))

    mo_header['cust_code'] = xstr(results[0][0])
    mo_header['po_type'] = xstr(results[0][1])
    mo_header['product_name'] = xstr(results[0][2])
    mo_header['cust_pn'] = xstr(results[0][3])
    mo_header['ht_pn'] = xstr(results[0][4])
    mo_header['sap_product_name'] = xstr(results[0][5])
    # 获取成品物料的属性
    get_mo_header_product_attrs(con_dw, mo_header)
    get_mo_header_product_bom(con_dw, mo_header)

    return mo_header


# 获取成品物料的属性
def get_mo_header_product_attrs(con_dw, mo_header):
    con_or = conn.OracleConn()

    sap_product_name = mo_header['sap_product_name']
    ht_pn = mo_header['ht_pn']

    # 物料主数据
    sql = f"SELECT DISTINCT ZZDBM,ZZPROCESSKEY,ZZBASESOMO,ZZLCBZ,ZZPROCESS,ZZBZ09,ZZJYGD,ZZFHFS FROM VM_SAP_MAT_INFO WHERE SUBSTRING(MATNR,LENGTH(MATNR)-7) = '{sap_product_name}' "
    results = con_dw.query(sql)
    mo_header['mark_code_flag'] = xstr(results[0][0]) if results else ''
    mo_header['process_key'] = xstr(results[0][1]) if results else ''
    mo_header['base_so'] = xstr(results[0][2]) if results else ''
    mo_header['lcbz'] = xstr(results[0][3]) if results else ''
    mo_header['process'] = xstr(results[0][4]) if results else ''
    mo_header['frozen'] = xstr(results[0][5]) if results else ''
    mo_header['grossdies'] = xstr(int(results[0][6])) if results else ''
    mo_header['fhfs'] = xstr(results[0][7]) if results else ''

    # 打标规则
    sql = f"SELECT REMARK FROM TBL_MARKINGCODE_REP WHERE HT_PN = '{ht_pn}' "
    results = con_or.query(sql)
    # if mo_header['mark_code_flag'] == "Y" and not results:
    #     abort(make_response(
    #         {"err_desc": f"{ht_pn}的物料SAP维护需要打标码,但还没有申请打标规则,无法下线,请联系NPI确认"}))
    # elif mo_header['mark_code_flag'] != "Y" and results:
    #     abort(make_response(
    #         {"err_desc": f"{ht_pn}的物料SAP维护不需要打标码,但申请打标规则,无法下线,请联系NPI确认"}))

    mo_header['mark_code_rule'] = xstr(results[0][0]) if results else ''


# 获取成品物料的BOM以及BOM的主晶圆
def get_mo_header_product_bom(con_dw, mo_header):
    # 跳过非客户订单
    if mo_header['po_type'][:3] != 'ZOR':
        mo_header['primary_mat'] = ''
        return False

    if mo_header['po_type'] in ('ZOR6', 'ZOR5'):
        mo_header['primary_mat'] = ''
        return False

    mo_header['primary_mat'] = ''
    sap_product_name = mo_header['sap_product_name']
    sap_product_name = ('00000000000000000' + sap_product_name)[-18:]
    if mo_header['po_type'][:3] == "ZOR":
        mo_header['primary_mat'] = rfc.get_bom_unit(sap_product_name)
        print("BOM组件", mo_header['primary_mat'])
        if not mo_header['primary_mat']:
            abort(make_response(
                {'err_desc': f'成品料号:{sap_product_name} =>BOM未建立或者BOM里不包含晶圆(半成品)'}))

        return True

        # # 晶圆,半成品,成品
        # sql = f"""SELECT string_agg(b.IDNRK,''',''') FROM VM_SAP_V_MAT a
        # INNER JOIN VM_SAP_V_ITEM b on a.STLNR = b.STLNR
        # INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = b.IDNRK
        # WHERE SUBSTRING(a.MATNR,LENGTH(a.MATNR)-7) = '{sap_product_name}'
        # AND c.MTART IN ('Z019','Z013','Z015')
        # AND a.WERKS = '1200'
        # """
        # results = con_dw.query(sql)
        # if results:
        #     mo_header['primary_mat'] = xstr(results[0][0])
        #     if not mo_header['primary_mat']:
        #         abort(make_response(
        #             {'err_desc': f'成品料号:{sap_product_name} =>BOM未建立或者BOM里不包含晶圆(半成品)'}))

        #     return True

        # if not mo_header['primary_mat']:
        #     abort(make_response(
        #         {'err_desc': f'成品料号:{sap_product_name} =>BOM未建立或者BOM里不包含晶圆(半成品)'}))

        # return True


# 获取US337 AER-PPR状态
def get_US337_AER_PPR_STATUS(con, wafer_sn):
    sql = f"""SELECT * FROM ZM_CDM_PO_ITEM zcpi WHERE CUST_CODE = 'US337' and add_28 IS NOT NULL AND (instr(add_28,'PPR') > 0 OR instr(add_4,'AER')>0)
        AND WAFER_SN = '{wafer_sn}'
    """
    results = con.query(sql)
    if results:
        return True
    else:
        return False


# 获取工单CT
def get_mo_CT(con, customder_device, special_flag):
    # 默认CT=15天
    ct_default_days = 15
    sql = f"SELECT HTKS_CT, FIXED , SPECIAL FROM ZM_CUSTDEVICE_CT A WHERE CUST_DEVICE = '{customder_device}' "
    results = con.query(sql)
    if not results:
        return ct_default_days

    htks_ct_days = results[0][0]
    fixed_days = results[0][1]
    special_days = results[0][2]

    if special_flag:
        ct_default_days = htks_ct_days + fixed_days + special_days
    else:
        ct_default_days = htks_ct_days + fixed_days

    return ct_default_days


# 工单明细数据
def get_mo_items_data(con, con_dw, mo_header):
    mo_items = []

    # 特殊标识
    SPECIAL_FLAG = False

    # 分批标志1
    if mo_header['cust_code'] in ('US337'):
        mo_sub_lot = 'ADD_4'
    elif mo_header['cust_code'] in ('US008', '70', 'HK099', 'HK006', 'SH296', 'AC70', 'JS195', 'DA69', 'AT51', 'SZ280', 'TW039', 'BJ218'):
        mo_sub_lot = 'ADD_1'
    elif mo_header['cust_code'] in ('SG005', 'US026', 'ZJ48'):
        mo_sub_lot = 'ADD_3'
    elif mo_header['cust_code'] in ('AT71'):
        mo_sub_lot = 'ADD_6'
    elif mo_header['cust_code'] in ('AT34'):
        mo_sub_lot = 'ADD_5'

    else:
        mo_sub_lot = "''"

    # 分批标志2
    if mo_header['cust_code'] in ('AT71'):
        mo_sub_lot_2 = 'ADD_8'
    else:
        mo_sub_lot_2 = "''"

    # 查询LOT层级数据
    sql = f"""SELECT  LOT_ID,COUNT(1),SUM(PASSBIN_COUNT+FAILBIN_COUNT),SO_ID,SO_ITEM,PO_ID,{mo_sub_lot},string_agg(WAFER_SN,''','''),{mo_sub_lot_2},BONDED FROM ZM_CDM_PO_ITEM
        WHERE  FLAG = '1' AND FLAG2 = '0'
        AND PO_TYPE = '{mo_header['po_type']}'
        AND CUST_CODE = '{mo_header['cust_code']}'
        AND CUSTOMER_DEVICE= '{mo_header['cust_pn']}'
        AND HT_PN= '{mo_header['ht_pn']}'
        AND PRODUCT_PN = '{mo_header['product_name']}'
        AND TRIM(LOT_ID) <> ''
        GROUP BY LOT_ID,SO_ID,SO_ITEM,PO_ID,{mo_sub_lot},{mo_sub_lot_2},BONDED ORDER BY LOT_ID
    """

    results = con.query(sql)
    for row in results:
        lot_item = {}

        lot_item['lotID'] = xstr(row[0])
        lot_item['waferQty'] = row[1]
        lot_item['moWaferQty'] = row[1]
        lot_item['grossDies'] = xstr(row[2])  # 订单总数量
        lot_item['grossDies'] = 0
        lot_item['soID'] = xstr(row[3])
        lot_item['soItem'] = xstr(row[4])
        lot_item['poID'] = xstr(row[5])
        # 分割lot
        lot_item['subLotID'] = xstr(row[6])
        # 聚合id
        waferIDStrListAgg = xstr(row[7])

        # 保税非保
        if xstr(row[9]) == 'B':
            lot_item['bonded'] = "非保税"
        elif xstr(row[9]) == 'A':
            lot_item['bonded'] = "保税"
        elif xstr(row[9]) == 'Y':
            lot_item['bonded'] = "保税"
        elif xstr(row[9]) == 'N':
            lot_item['bonded'] = "非保税"
        else:
            lot_item['bonded'] = "未知"

        lot_item['poItem'] = lot_item['soItem'] if lot_item['soItem'] else '10'

        # 实物lot
        if mo_header['cust_code'] in ('US008', '70', 'HK099', 'HK006', 'SH296', 'JS195', 'DA69', 'AT51', 'SZ280', 'TW039', 'BJ218'):
            lot_item['fabLotID'] = xstr(row[6])
        elif mo_header['cust_code'] in ('AT71'):
            lot_item['fabLotID'] = xstr(row[8]) if xstr(
                row[8]) else lot_item['lotID']
            lot_item['subLotID'] = xstr(row[8])
        else:
            lot_item['fabLotID'] = ''

        lot_item['waferList'] = []
        lot_item['waferSNList'] = []
        lot_item['waferIDStrList'] = []
        lot_item['lotBin'] = ""
        lot_item['inventoryGrossDies'] = 0  # 单工单总数量

        # 获取特权lot
        lot_item['lotProprity'], lot_item['waferPartNo'] = get_priority_lot_id(con,
                                                                               lot_item['lotID'])

        # 查询wafer层级数据
        sql = f""" SELECT LOT_ID,WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,WAFER_SN,LOT_WAFER_ID,MARK_CODE,ID,REMARK1 
        FROM ZM_CDM_PO_ITEM WHERE WAFER_SN IN ('{waferIDStrListAgg}') ORDER BY LOT_WAFER_ID 
        """
        results2 = con.query(sql)
        for row2 in results2:
            wafer_item = {}
            wafer_item['lotID'] = xstr(row2[0])
            wafer_item['waferBin'] = "G+B"
            wafer_item['waferID'] = xstr(row2[1])
            wafer_item['goodDies'] = row2[2]
            wafer_item['zzmylx'] = ""
            wafer_item['poGoodDies'] = row2[2]
            wafer_item['ngDies'] = row2[3]
            wafer_item['waferSN'] = xstr(row2[4])

            if mo_header['cust_code'] in ("US337"):
                if get_US337_AER_PPR_STATUS(con, wafer_item['waferSN']):
                    SPECIAL_FLAG = True

            wafer_item['lotWaferID'] = xstr(row2[5])
            print(wafer_item['lotWaferID'])
            wafer_item['grossDies'] = wafer_item['goodDies'] + \
                wafer_item['ngDies']
            wafer_item['markCode'] = xstr(row2[6])
            wafer_item['realID'] = 'N' if xstr(row2[7]) == '0' else 'Y'
            wafer_item['primary_mat'] = xstr(row2[8])  # wafer扣料

            wafer_item['fabLotID'] = lot_item['fabLotID']
            wafer_item['productGrossDies'] = mo_header['grossdies']

            # 库存检查
            check_wafer_inventory(con_dw, mo_header, lot_item, wafer_item)
            wafer_item['waferPartNo'] = wafer_item['invPartID']  # 扣账料号
            wafer_item['bomPartID'] = wafer_item['bomPartID'][-8:]
            wafer_item['invPartID'] = wafer_item['invPartID'][-8:]

            # map更新
            update_wafer_map(con, mo_header, lot_item, wafer_item)
            wafer_item['grossDies'] = wafer_item['goodDies'] + \
                wafer_item['ngDies']

            lot_item['grossDies'] = int(
                lot_item['grossDies']) + int(wafer_item['grossDies'])

            # 保存当前行查询到的数据
            wafer_item['queryGoodDies'] = wafer_item['goodDies']

            # 累计
            lot_item['waferList'].append(wafer_item)
            lot_item['waferSNList'].append(wafer_item['waferSN'])
            lot_item['waferIDStrList'].append(wafer_item['waferID'])
            lot_item['inventoryGrossDies'] = lot_item['inventoryGrossDies'] + \
                int(wafer_item['inventoryDies'])

        lot_item['waferIDStrList'] = ','.join(lot_item['waferIDStrList'])

        # 工单总库存状态
        if int(lot_item['inventoryGrossDies']) >= int(lot_item['grossDies']):
            lot_item['moInvStatus'] = "Y"
        else:
            lot_item['moInvStatus'] = "N"

        mo_items.append(lot_item)

    # 获取CT
    mo_header['CT_DAYS'] = int(
        get_mo_CT(con_dw, mo_header['cust_pn'], SPECIAL_FLAG))

    return mo_items


# 根据厂内机种判断是继承还是厂外
def get_map_incoming_flag(con, mo_header):
    if not mo_header.get('MAP_FROM'):
        ht_pn = mo_header['ht_pn']
        sql = f"""  select v1.DEVICE_NAME
            FROM VIEW_PROD_MAP_DEF v1 
            INNER JOIN VIEW_PROD_MAP_COMBINE_OP_DEF v2
            ON v2.PROD_MAP_DEF_BO = v1.ID
            AND v2.MAP_RULE_BO LIKE '%_INCOMING'
            WHERE DEVICE_NAME = '{ht_pn}'
            AND NOT EXISTS(SELECT 1 
            FROM ZD_PROD_MAP_INHERIT_DEF zd 
            WHERE zd.DEVICE_NAME = v1.DEVICE_NAME
            AND zd.OPERATION = v2.OPERATION_NAME)
            """
        results = con.query(sql)
        if results:
            # 厂外
            mo_header['MAP_FROM'] = "CDM"

        sql = f""" SELECT v1.DEVICE_NAME
        FROM VIEW_PROD_MAP_DEF v1 
        INNER JOIN VIEW_PROD_MAP_COMBINE_OP_DEF v2
        ON v2.PROD_MAP_DEF_BO = v1.ID
        AND v2.MAP_RULE_BO LIKE '%_INCOMING'
        WHERE DEVICE_NAME ='{ht_pn}'
        AND EXISTS(SELECT 1 
        FROM ZD_PROD_MAP_INHERIT_DEF zd 
        WHERE zd.DEVICE_NAME = v1.DEVICE_NAME
        AND zd.OPERATION = v2.OPERATION_NAME)
        """
        results = con.query(sql)
        if results:
            # 厂内
            mo_header['MAP_FROM'] = "MES"


# 判断是否跳过map卡控
def check_need_map_check(con, data):
    sql = f"SELECT * FROM ZM_CDM_PRIORITY_LOT WHERE PRIORITY_LEVEL = '2' AND REMARK1 = 'MAP_CHECK' AND LOT_ID = '{data}' "
    results = con.query(sql)
    if results:
        return True
    else:
        return False


# 获取BUMPING段LOT
def get_SH296_BP_LOT(con, lot_id, wafer_no, lot_wafer_id):
    # sql = f"SELECT DISTINCT LOT_ID FROM ZM_CDM_PO_ITEM zcpi WHERE instr('{lot_id}', LOT_ID) > 0 AND FLAG2 = '1' AND HT_PN LIKE '%B' AND CUST_CODE  IN ('AA08','SH296') "

    sql = f"SELECT LOT_WAFER_ID,CREATE_DATE,PO_DATE FROM ZM_CDM_PO_ITEM zcpi WHERE instr('{lot_id}', LOT_ID) > 0 AND FLAG2 = '1' AND HT_PN LIKE '%B' AND CUST_CODE  IN ('AA08','SH296') AND WAFER_ID = '{wafer_no}' ORDER BY PO_DATE desc"

    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    else:
        return lot_wafer_id


# 更新map,检查map
def update_wafer_map(con, mo_header, lot_item, wafer_item):
    print(mo_header)
    wafer_item['mapFlag'] = False
    mo_header['MAP_FROM'] = ""
    wafer_item['mapMessage'] = ""
    m_lot_id = wafer_item['lotID']  # map LOT ID
    m_wafer_id = wafer_item['lotWaferID']  # map WAFER ID
    m_wafer_no = wafer_item['waferID']  # map WAFER NO
    chk_gross_dies_flag = False  # 良品 + 不良品 = GROSSDIE检查

    # 重工不需要
    if mo_header['mo_type'] not in ('YP01', 'YP02'):
        wafer_item['mapMessage'] = f"map无需更新"
        wafer_item['mapFlag'] = True
        return False

    # 跳过卡控的厂内机种
    if check_need_map_check(con, mo_header['ht_pn']):
        wafer_item['mapMessage'] = f"机种跳过卡控,map无需更新"
        wafer_item['mapFlag'] = True
        return False

    # 小于GROSS DIE数量则不更新map
    if int(wafer_item['goodDies']) < int(mo_header['grossdies']):
        wafer_item['mapMessage'] = f"客户指定良品投单,map无需更新"
        wafer_item['mapFlag'] = True
        return False

    # US337无效卡控
    if mo_header['cust_code'] in ('US337'):
        wafer_item['mapMessage'] = f"map无需更新"
        wafer_item['mapFlag'] = True
        return False

    # 检查良品+ 不良品= GROSS DIE
    if mo_header['cust_code'] in ('KR001', 'KR009'):
        mo_header['MAP_FROM'] = "CDM"
        chk_gross_dies_flag = True

    elif mo_header['cust_code'] in ('US010') and 'TSV' in mo_header['process'] and mo_header['po_type'] == 'ZOR1':
        mo_header['MAP_FROM'] = "CDM"
        chk_gross_dies_flag = True

    # FC:只取良品
    elif 'FC' in mo_header['process']:
        if mo_header['cust_code'] in ('AT71', 'JS195', 'TW039'):
            m_lot_id = wafer_item['fabLotID'] if wafer_item['fabLotID'] else wafer_item['lotID']
            m_wafer_id = m_lot_id + m_wafer_no
        elif mo_header['cust_code'] in ('US008'):
            m_lot_id = wafer_item['fabLotID'] if wafer_item['fabLotID'] else wafer_item['lotID']
            # m_lot_id = m_lot_id[:(m_lot_id.find('.')+2)]
            m_wafer_id = m_lot_id + m_wafer_no

        elif mo_header['cust_code'] in ('SH296'):
            m_lot_id = wafer_item['fabLotID'] if wafer_item['fabLotID'] else wafer_item['lotID']
            m_wafer_id = m_lot_id + m_wafer_no
            m_wafer_id = get_SH296_BP_LOT(
                con, m_lot_id, m_wafer_no, m_wafer_id)

    else:
        wafer_item['mapMessage'] = f"map无需更新"
        wafer_item['mapFlag'] = True
        return False

    # 需要找map
    if not m_lot_id:
        wafer_item['mapMessage'] = f"map LOT不能为空"
        wafer_item['mapFlag'] = False
        return False

    # 开始找map
    # 1.判断是取厂内map还是厂外map
    get_map_incoming_flag(con, mo_header)
    if not mo_header['MAP_FROM']:
        wafer_item['mapMessage'] = f"map来源未定义好,请联系NPI"
        wafer_item['mapFlag'] = False
        return False

    # 查询
    sql = f"""SELECT WAFER_GOOD_DIES,WAFER_NG_DIES,WAFER_ID 
      FROM ZM_CDM_WAFER_MAP_BIN_DIES WHERE (WAFER_ID in ('{m_wafer_id}') or (LOT_ID ='{m_lot_id}' AND WAFER_NO = '{m_wafer_no}'))  
      AND FLAG = '1' AND CREATE_BY = '{mo_header['MAP_FROM']}' ORDER BY CREATE_DATE desc """
    print(sql)
    results = con.query(sql)
    if not results:
        if mo_header['MAP_FROM'] == "MES":
            # 查询厂内map
            sql = f"SELECT sum(DIE_QTY),0,'{m_wafer_id}' FROM zr_customer_item_grade WHERE CUSTOMER_ITEM_BO LIKE '{m_wafer_id}%' AND GOOD_BAD = 'G' "
            print(sql)
            results = con.query(sql)
            if not results or not results[0][0]:
                wafer_item['mapMessage'] = f"map资料未找到:{mo_header['MAP_FROM']}"
                wafer_item['mapFlag'] = False
                return False

    if not results:
        wafer_item['mapMessage'] = f"map资料未找到:{mo_header['MAP_FROM']}"
        wafer_item['mapFlag'] = False
        return False

    if chk_gross_dies_flag:
        # map数据
        map_passbin_cnt = results[0][0]
        map_failbin_cnt = results[0][1]
        map_wafer_id = xstr(results[0][2])
        map_gross_cnt = map_passbin_cnt + map_failbin_cnt

        # 订单数据
        po_gross_dies = wafer_item['goodDies'] + wafer_item['ngDies']
        if map_gross_cnt != po_gross_dies:
            wafer_item['mapMessage'] = f"map的grossdies:{map_gross_cnt},订单grossdies:{po_gross_dies},不一致"
            wafer_item['mapFlag'] = False
            return False

    else:
        # map数据
        map_passbin_cnt = int(results[0][0])
        map_failbin_cnt = 0
        # wafer_item['inventoryDies'] = map_passbin_cnt

    # 需要更新
    sql = f"SELECT * FROM ZM_CDM_PO_ITEM WHERE LOT_WAFER_ID = '{wafer_item['lotWaferID']}' AND FLAG2 = '1' AND (HT_PN LIKE '%FC' OR HT_PN LIKE '%FT') "
    results = con.query(sql)
    if not results:
        sql = f"UPDATE ZM_CDM_PO_ITEM SET PASSBIN_COUNT = {map_passbin_cnt}, FAILBIN_COUNT = {map_failbin_cnt},UPDATE_DATE=NOW() WHERE WAFER_SN = '{wafer_item['waferSN']}'  "
        if not con.exec_c(sql):
            wafer_item['mapMessage'] = f"map的更新失败"
            wafer_item['mapFlag'] = False
            return False

        wafer_item['goodDies'] = map_passbin_cnt
        wafer_item['ngDies'] = map_failbin_cnt
        wafer_item['mapMessage'] = f"map已更新:{mo_header['MAP_FROM']}"
        wafer_item['mapFlag'] = True
    else:
        wafer_item['mapMessage'] = f"map已找到:{mo_header['MAP_FROM']}"
        wafer_item['mapFlag'] = True

    # US010样品TSV订单更新打标码
    if mo_header['cust_code'] in ('US010') and 'TSV' in mo_header['process'] and mo_header['po_type'] == 'ZOR1' and 'WERKS' in wafer_item:
        wafer_item['markCode'] = update_us010_tsv_mark_code(
            wafer_item['lotID'], map_wafer_id, lot_item['poID'], wafer_item['waferSN'])
        update_us010_tsv_inv_wafer_id(wafer_item, map_wafer_id)

    return True


# US010 TSV更新打标码
def update_us010_tsv_mark_code(lot_id, wafer_id, ebr_no, wafer_sn):
    con_or = conn.OracleConn()
    con = conn.HanaConn()
    sql = f"SELECT ONSTMarkingCodeSeq.QTSEQ('{wafer_id}','{lot_id}') FROM dual"
    results = con_or.query(sql)
    if not results:
        abort(make_response({'err_desc': f'US010=> {wafer_id} :TSV样品打标码获取失败'}))

    mark_code = xstr(results[0][0])
    new_mark_code = ebr_no + mark_code[-4:]
    sql = f"update OnST_MarkHistory set QBOXNUMBER= '{new_mark_code}' where CONTAINERNAME = '{wafer_id}' "
    con_or.exec_c(sql)

    sql = f"update ZM_CDM_PO_ITEM set mark_code ='{new_mark_code}',update_date=NOW() where wafer_sn = '{wafer_sn}' "
    con.exec_c(sql)

    return new_mark_code


def update_us010_tsv_inv_wafer_id(wafer_item, map_wafer_id):
    # 更新订单片号
    con = conn.HanaConn()
    sql = f"update ZM_CDM_PO_ITEM set lot_wafer_id ='{map_wafer_id}',update_date=NOW() where wafer_sn = '{wafer_item['waferSN']}'"
    con.exec_c(sql)
    wafer_item['lotWaferID'] = map_wafer_id
    set_obj = {}
    set_obj['WERKS'] = wafer_item['WERKS']
    set_obj['MATNR'] = wafer_item['MATNR']
    set_obj['CHARG'] = wafer_item['CHARG']
    set_obj['ZWAFER_LOT'] = wafer_item['ZWAFER_LOT']
    set_obj['ZSEQ'] = wafer_item['ZSEQ']
    set_obj['ZWAFER_ID'] = map_wafer_id
    wafer_item['ZWAFER_ID'] = map_wafer_id
    pp_sw.submit_request(set_obj)


# 库存检查
def check_wafer_inventory(con_dw, mo_header, lot_item, wafer_item):
    # 查询晶圆(主材)库存
    if mo_header['primary_mat']:
        matnr = wafer_item['primary_mat'] if wafer_item['primary_mat'] else mo_header['primary_mat']

        if lot_item['fabLotID']:
            zwafer_lot = lot_item['fabLotID']
            zwafer_lot_2 = wafer_item['lotID']

        else:
            zwafer_lot = wafer_item['lotID']
            zwafer_lot_2 = wafer_item['lotID']

        if lot_item['fabLotID']:
            zwafer_id = lot_item['fabLotID'] + wafer_item['waferID']
            zwafer_id_2 = wafer_item['lotWaferID']
        else:
            zwafer_id = wafer_item['lotWaferID']
            zwafer_id_2 = wafer_item['lotWaferID']

        # 查询库存
        wafer_inv = get_wafer_inventory(
            con_dw, matnr, zwafer_lot, zwafer_id, wafer_item, zwafer_lot_2, zwafer_id_2)

        # 库存检查
        if not wafer_inv:
            wafer_item['bomPartID'] = matnr
            wafer_item['invPartID'] = ""
            wafer_item['inventoryGrDies'] = 0  # 库存dies
            wafer_item['inventoryGIDies'] = 0  # 耗用dies
            wafer_item['inventoryDies'] = 0  # 可用dies
            wafer_item['inventoryStatus'] = "未入库,或没有片号,库存不足"
            wafer_item['inventoryDesc'] = "未入库,或没有片号"
            wafer_item['inventoryGoodDies'] = 0
            wafer_item['inventoryFlag'] = False
        else:
            wafer_item['bomPartID'] = matnr
            wafer_item['invPartID'] = wafer_item['MATNR']
            # 库存dies
            wafer_item['inventoryGrDies'] = wafer_item['ZGROSS_DIE_QTY']
            # 耗用dies
            wafer_item['inventoryGIDies'] = wafer_item['ZDIE_QTY_GI']
            # 可用dies
            wafer_item['inventoryDies'] = wafer_item['ZDIE_QTY_RM']
            # 良品dies
            wafer_item['inventoryGoodDies'] = wafer_item['ZDIE_QTY_GOOD_RM']

            # 数量检查
            if wafer_item['grossDies'] <= wafer_item['inventoryDies']:
                wafer_item['inventoryStatus'] = "库存充足"
                wafer_item['inventoryDesc'] = "库存充足"
                wafer_item['inventoryFlag'] = True
            else:
                wafer_item['inventoryStatus'] = "库存不足"
                wafer_item['inventoryDesc'] = "库存不足"
                wafer_item['inventoryFlag'] = False
    else:
        wafer_item['bomPartID'] = mo_header['sap_product_name']
        wafer_item['invPartID'] = mo_header['sap_product_name']
        wafer_item['inventoryGrDies'] = wafer_item['grossDies']
        wafer_item['inventoryGIDies'] = 0
        wafer_item['inventoryDies'] = wafer_item['grossDies']
        wafer_item['inventoryStatus'] = "库存充足"
        wafer_item['inventoryDesc'] = "库存充足"
        wafer_item['inventoryFlag'] = True
        # 良品dies
        wafer_item['inventoryGoodDies'] = wafer_item['inventoryDies']


# 获取WAFER库存明细
def get_wafer_inventory(con_dw, matnr, zwafer_lot, zwafer_id, wafer_item, zwafer_lot_2, zwafer_id_2):
    if len(matnr) == 8:
        matnr = ("0000000000000" + matnr)[-18:]

    sql = f"""SELECT t1.WERKS,t1.LGORT,t1.MATNR,t1.CHARG,t1.ZSEQ ,t1.ZWAFER_LOT,t1.ZWAFER_ID,t1.ZDIE_QTY,t1.ZDIE_QTY_GI,t1.ZDIE_QTY -t1.ZDIE_QTY_GI,t2.ZOUT_BOX,t2.ZSTOCK_TYPE,t1.ZGROSS_DIE_QTY,t1.ZBIN_NO 
    FROM ZKTMM0001 t1 
    left JOIN ZTMM0001 t2 ON t2.MATNR = t1.MATNR AND t2.WERKS = t1.WERKS AND t2.CHARG = t1.CHARG 
    WHERE t1.WERKS = '1200' AND t1.ZWAFER_LOT in ('{zwafer_lot}','{zwafer_lot_2}') AND t1.ZWAFER_ID in ('{zwafer_id}','{zwafer_id_2}') AND t1.MATNR IN ('{matnr}') AND (t1.ZDIE_QTY -t1.ZDIE_QTY_GI) > 0
    AND TRIM(t1.CHARG) <> '' ORDER BY t1.ZBIN_NO DESC ,t1.CHARG 
    """

    results = con_dw.query(sql)
    if not results:
        return False

    wafer_item['WERKS'] = xstr(results[0][0])
    wafer_item['LGORT'] = xstr(results[0][1])
    wafer_item['MATNR'] = xstr(results[0][2])
    wafer_item['CHARG'] = xstr(results[0][3])
    wafer_item['ZSEQ'] = xstr(results[0][4])
    wafer_item['ZWAFER_LOT'] = xstr(results[0][5])
    wafer_item['ZWAFER_ID'] = xstr(results[0][6])
    wafer_item['ZGROSS_DIE_QTY'] = int(results[0][7])
    wafer_item['ZDIE_QTY_GI'] = int(results[0][8])
    wafer_item['ZDIE_QTY_RM'] = int(results[0][9])
    wafer_item['ZWAFER_GROSS_DIE'] = int(results[0][12])
    wafer_item['WAFER_INV_ITEMS'] = []
    # 库存明细
    for row in results:
        wafer_inv_item = {}
        wafer_inv_item['WERKS'] = xstr(row[0])
        wafer_inv_item['LGORT'] = xstr(row[1])
        wafer_inv_item['MATNR'] = xstr(row[2])
        wafer_inv_item['CHARG'] = xstr(row[3])
        wafer_inv_item['ZSEQ'] = xstr(row[4])
        wafer_inv_item['ZWAFER_LOT'] = xstr(row[5])
        wafer_inv_item['ZWAFER_ID'] = xstr(row[6])
        wafer_inv_item['ZGROSS_DIE_QTY'] = int(row[7])
        wafer_inv_item['ZDIE_QTY_GI'] = int(row[8])
        wafer_inv_item['ZDIE_QTY_RM'] = int(row[9])
        wafer_inv_item['ZOUT_BOX'] = xstr(row[10])
        wafer_inv_item['ZBIN_NO'] = xstr(row[13])
        wafer_item['WAFER_INV_ITEMS'].append(wafer_inv_item)

    # 库存可用总数
    sql = f"""select sum(a.ZDIE_QTY-a.ZDIE_QTY_GI) from  ZKTMM0001 a 
    where  a.WERKS = '1200' AND a.ZWAFER_LOT in ('{zwafer_lot}','{zwafer_lot_2}')
    AND a.ZWAFER_ID in ('{zwafer_id}','{zwafer_id_2}') AND a.MATNR IN ('{matnr}')
    AND TRIM(a.CHARG) <> '' 
    """
    results = con_dw.query(sql)
    if results:
        if xstr(results[0][0]):
            wafer_item['ZDIE_QTY_RM'] = int(xstr(results[0][0]))

    # 库存GOOD数量
    sql = f"""select sum(a.ZDIE_QTY-a.ZDIE_QTY_GI) from  ZKTMM0001 a 
    left join ZTMM0001 b on a.CHARG = b.CHARG and b.MATNR = a.MATNR
    where  a.WERKS = '1200' AND a.ZWAFER_LOT in ('{zwafer_lot}','{zwafer_lot_2}')
    AND a.ZWAFER_ID in ('{zwafer_id}','{zwafer_id_2}') AND a.MATNR IN ('{matnr}')
    AND TRIM(a.CHARG) <> '' AND b.ZSTOCK_TYPE in ('G','')
    """
    results = con_dw.query(sql)
    if not results:
        wafer_item['ZDIE_QTY_GOOD_RM'] = wafer_item['ZDIE_QTY_RM']
    else:
        if xstr(results[0][0]):
            wafer_item['ZDIE_QTY_GOOD_RM'] = int(results[0][0])

        else:
            wafer_item['ZDIE_QTY_GOOD_RM'] = 0

    # 库存NG数量
    sql = f"""select sum(a.ZDIE_QTY-a.ZDIE_QTY_GI) from  ZKTMM0001 a 
    left join ZTMM0001 b on a.CHARG = b.CHARG and b.MATNR = a.MATNR
    where  a.WERKS = '1200' AND a.ZWAFER_LOT in ('{zwafer_lot}','{zwafer_lot_2}')
    AND a.ZWAFER_ID in ('{zwafer_id}','{zwafer_id_2}') AND a.MATNR IN ('{matnr}')
    AND TRIM(a.CHARG) <> '' AND b.ZSTOCK_TYPE = 'B'
    """
    results = con_dw.query(sql)
    if not results:
        wafer_item['ZDIE_QTY_GOOD_RM'] = wafer_item['ZDIE_QTY_RM']
    else:
        if xstr(results[0][0]):
            wafer_item['ZDIE_QTY_NG_RM'] = int(results[0][0])

        else:
            wafer_item['ZDIE_QTY_NG_RM'] = 0

    # 晶圆贸易类型-保税非保
    # sql = f""" select DISTINCT b.ZZMYLX from ZKTMM0001 a 
    # LEFT JOIN ZTMM0001 b ON a.CHARG = b.CHARG 
    # where  a.WERKS = '1200' AND a.ZWAFER_LOT in ('{zwafer_lot}','{zwafer_lot_2}')
    # AND a.ZWAFER_ID in ('{zwafer_id}','{zwafer_id_2}') AND a.MATNR IN ('{matnr}')
    # """
    # results = con_dw.query(sql)
    # if len(results) == 1:
    #     wafer_item['zzmylx'] = xstr(results[0][0])
    #     if wafer_item['zzmylx'] == '0':
    #         wafer_item['zzmylx'] = 'A'
    #     elif wafer_item['zzmylx'] == '1':
    #         wafer_item['zzmylx'] = 'B'
    # 晶圆贸易类型-保税非保
    if wafer_item['LGORT'][1:2] == '9':
        # 保税
        wafer_item['zzmylx'] = 'A'
    elif wafer_item['LGORT'][1:2] == '0':
        # 非保
        wafer_item['zzmylx'] = 'B'
    else:
        # 未知类型
        wafer_item['zzmylx'] = ''

    return True


# 获取特权lot
def get_priority_lot_id(con, lot_id):
    sql = f"SELECT PRIORITY_LEVEL,REMARK1  FROM ZM_CDM_PRIORITY_LOT WHERE LOT_ID = '{lot_id}' AND FLAG = '1' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0]), xstr(results[0][1])

    return '0', ''


if __name__ == '__main__':
    data = {'cust_code': 'SH296', 'mo_type': 'YP01',
            'product_name_type': 'P2', 'product_name': 'XAA08021FT'}
    get_mo_data(data)
