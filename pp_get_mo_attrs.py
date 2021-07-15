import conn_db
import mm_mat_info as mmi


def xstr(s):
    return '' if s is None else str(s).strip()


# ------------------------------------------------------------------------------------------
# 获取工单层级属性
def get_mo_header_level_attributes(conn, conn_or, mo_id, process_key):
    attrs = {}

    mo_header = get_mo_header(conn, mo_id)
    if mo_header["ERR_MSG"]:
        return mo_header["ERR_MSG"]

    # 公共属性
    attrs['BONDED'] = 'Y' if mo_id[:1] == 'A' else 'N'
    attrs['CUST_REWORK'] = 'Y' if mo_id[1:2] == 'R' else 'N'
    attrs['ERP_CREATE_DATE_STRING'] = mo_header['CREATE_DATE']
    attrs['MARKING_CODE_DC'] = mo_header['MARKING_CODE_DC']
    attrs['ERP_CREATE_DATE'] = get_mo_date_code(
        conn, conn_or, mo_header, attrs['ERP_CREATE_DATE_STRING'])
    attrs['SHIP_CUSTOMER'] = mo_header['CUST_CODE']
    attrs['CUST_PART_NUM1'] = mo_header['CUSTOMER_DEVICE']
    attrs['HT_PN'] = mo_header['HT_PN']
    if process_key:
        attrs['ZZPROCESSKEY'] = process_key

    get_mo_header_attrs_COMMON(attrs, conn, conn_or, mo_header)

    if mo_header['CUST_CODE'] in ('AB18') and attrs['MARKING_CODE_DC']:
        attrs['ERP_CREATE_DATE'] = attrs['MARKING_CODE_DC']

    if mo_header['CUST_CODE'] in ('ZJ48', 'ZJ35'):
        get_mo_header_attrs_57(attrs, conn, conn_or, mo_header)

    if mo_header['CUST_CODE'] == 'GD55':
        get_mo_header_attrs_HD(attrs, conn, conn_or, mo_header)

    if mo_header['CUST_CODE'] == 'GD108':
        get_mo_header_attrs_GD108(attrs, conn, conn_or, mo_header)

    if mo_header['PRODUCT_PN'][-3:] == 'CCF':
        get_mo_header_attrs_GLASS(attrs, conn, conn_or, mo_header)

    if mo_header['PRODUCT_PN'][-2:] == 'FC':
        get_mo_header_attrs_FC(attrs, conn, conn_or, mo_header)

    if mo_header['CUST_CODE'] == 'US337':
        err_msg = get_mo_header_attrs_37(attrs, conn, conn_or, mo_header)
        if err_msg:
            return err_msg

    if mo_header['CUST_CODE'] in ('AA', 'US010'):
        get_mo_header_attrs_AA(attrs, conn, conn_or, mo_header)

    if mo_header['CUST_CODE'] in ('EU010', 'AC70', 'HK037', 'HK075'):
        get_mo_header_attrs_EU010(attrs, conn, conn_or, mo_header)

    if mo_header['CUST_CODE'] in ('US008', '70', 'HK099', 'HK006'):
        err_msg = get_mo_header_attrs_68(attrs, conn, conn_or, mo_header)
        if err_msg:
            return err_msg

    if mo_header['CUST_CODE'] in ('AT34'):
        err_msg = get_mo_header_attrs_AT34(attrs, conn, conn_or, mo_header)
        if err_msg:
            return err_msg

    if mo_header['CUST_CODE'] in ("US337", "EU010", "HK075"):
        err_msg = get_mo_header_attrs_yjx(attrs, conn, conn_or, mo_header)
        if err_msg:
            return err_msg

    # GD55 CSP工单增加流水
    if mo_header['CUST_CODE'] in ("GD55") and mo_header['PO_TYPE'] == "YP13":
        err_msg = get_GD55_Lot_dc_seq(attrs, conn)

    attrs_error = check_mo_header_attrs(attrs)
    if attrs_error:
        return attrs_error

    return attrs


# GD55 CSP LOT DC流水
def get_GD55_Lot_dc_seq(attrs, conn):
    zzbase_str = attrs['ERP_CREATE_DATE']
    if len(zzbase_str) != 4:
        return "GD55 CSP DATECODE获取异常"

    sql = f"SELECT IFNULL(MAX(ZZSEQ)+1,1)  FROM ZM_CDM_COMMON_SEQ WHERE ZZTYPE = 'GD55_CSP_LOT_DC_SEQ' AND ZZBASE = '{zzbase_str}' "
    results = conn.query(sql)
    if results:
        cur_seq = results[0][0]
        attrs['GD55_CSP_LOT_SEQ'] = zzbase_str + \
            "-F" + ("0000" + str(cur_seq))[-3:]

        print(attrs['GD55_CSP_LOT_SEQ'])
        # 插入新记录
        sql = f" INSERT INTO ZM_CDM_COMMON_SEQ(ZZTYPE,ZZBASE,ZZKEY,ZZSEQ,ZZTIME) VALUES('GD55_CSP_LOT_DC_SEQ','{zzbase_str}','{attrs['GD55_CSP_LOT_SEQ']}',{cur_seq},NOW()) "
        err_status, err_msg = conn.exec_n_2(sql)
        if not err_status:
            return f"GD55 CSP LOT流水插入异常:{err_msg}"
        else:
            return ""
    else:
        return "GD55 CSP LOT流水获取异常"


# 阴极线
def get_mo_header_attrs_yjx(attrs, conn, conn_or, mo_header):
    # CODE37
    attrs['CODE37_STAUTS'] = "NULL"
    attrs['CODE37_BLINE'] = "NULL"
    attrs['CODE37_CODE'] = "NULL"

    cust_device = get_first_time_device(
        mo_header['MO_WAFER_SN'], conn, conn_or)

    if not cust_device:
        return '找不到一次订单的客户机种'

    sql = f"SELECT distinct  status,bline,code  FROM CODE37 WHERE DEVICE = '{cust_device}' "
    results = conn_or.query(sql)

    sql2 = f"""SELECT
            MAX(map( t1.SUB_ID,'STATUS',t1.value,'' ) ) AS STATUS,
            MAX(map( t1.SUB_ID ,'BLINE',t1.value,'' ) ) AS BLINE,
            MAX(map( t1.SUB_ID,'CODE',t1.value,'' ) ) AS CODE
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME = 'MT01' AND t1.MAIN_ID = '{cust_device}'
            group by
            t1.MAIN_ID
    """
    results2 = conn.query(sql2)

    if results:
        attrs['CODE37_STAUTS'] = xstr(results[0][0])
        attrs['CODE37_BLINE'] = xstr(results[0][1])
        attrs['CODE37_CODE'] = xstr(results[0][2])

        if xstr(results2[0][0]) != attrs['CODE37_STAUTS'] or xstr(results2[0][1]) != attrs['CODE37_BLINE'] or xstr(results2[0][2]) != attrs['CODE37_CODE']:
            return f'CODE37阴极线数据异常'

    else:
        if results2:
            attrs['CODE37_STAUTS'] = xstr(results2[0][0])
            attrs['CODE37_BLINE'] = xstr(results2[0][1])
            attrs['CODE37_CODE'] = xstr(results2[0][2])
        else:
            if mo_header['MO_TYPE'] in ('YP01', 'YP02') and cust_device not in ('JHR3010A2B-B', 'JHR3019C1-1B', 'JHR3019E1-B', 'RCLAMP2261ZA.F.P1'):
                con_dw = conn_db.HanaConnDW()
                sql = f"SELECT * FROM VM_SAP_MAT_INFO WHERE ZZKHXH = '{cust_device}' AND ZZPROCESS LIKE '%SSP%' "
                results2 = con_dw.query(sql)
                if results2:
                    return f'一次订单机种:{cust_device}没有维护CODE37阴极线'

            else:
                return ''


# 检查工单表头层级属性
def check_mo_header_attrs(attrs):
    if not attrs.get('ERP_CREATE_DATE_STRING'):
        return "ERP_CREATE_DATE_STRING属性不可为空,请联系IT"

    if not attrs.get('ERP_CREATE_DATE'):
        return "ERP_CREATE_DATE属性不可为空,请联系IT"

    if not attrs.get('CUST_PART_NUM1'):
        return "CUST_PART_NUM1属性不可为空,请联系IT"

    if attrs['MARKING_CODE_DC'] and check_mo_mark_code(ht_pn=attrs['HT_PN']):
        if attrs['ERP_CREATE_DATE'] != attrs['MARKING_CODE_DC']:
            if not attrs.get('SERVICE_CODE'):
                return f"工单年周{attrs['ERP_CREATE_DATE']}和打标码年周{attrs['MARKING_CODE_DC']}不一致,请联系IT"
        print("年周检查通过")

    return ''


def get_mo_header_attrs_68(attrs, conn, conn_or, mo_header):
    sql = f"select address_code from zm_cdm_po_item where wafer_sn = '{mo_header['MO_WAFER_SN']}'  "
    results = conn.query(sql)
    if results:
        attrs['JOBID'] = xstr(results[0][0])

    if not attrs.get('JOBID') and mo_header['PO_TYPE'][:3] == 'ZOR':
        return "US008必须维护出货地址"

    return ""


def get_mo_header_attrs_AT34(attrs, conn, conn_or, mo_header):
    attrs['JOBID'] = mo_header['ADD_5']
    if not attrs['JOBID']:
        return "AT34必须维护JOBID"

    return ""


# 物料主数据
def get_sap_mat_info(matnr):
    con_dw = conn_db.HanaConnDW()

    matnr = ("000000000000" + matnr)[-18:]

    sql = f"""SELECT DISTINCT ZZKHXH,ZZFABXH,ZZHTXH,ZZCNLH,MATNR,ZZPROCESS,ZZEJDM,ZZJYGD,ZZBASESOMO,ZZBZ09,
        ZZLKHZY1,ZZLKHZY2,ZZLKHZY3,ZZLKHZY4,ZZLKHZY5,ZZLCBZ FROM VM_SAP_MAT_INFO WHERE MATNR = '{matnr}'
    """
    results = con_dw.query(sql)
    if not results:
        return None

    mat_info = {}
    mat_info['ZZPROCESS'] = xstr(results[0][5])
    return mat_info


# 判断是否是工单时触发
def check_mo_mark_code(ht_pn):
    con = conn_db.OracleConn()
    sql = f"SELECT * FROM TBL_MARKINGCODE_REP WHERE HT_PN = '{ht_pn}' AND CUST_PN = '工单创建时产生' "
    results = con.query(sql)
    if results:
        return True
    else:
        return False


# 获取工单表头信息
def get_mo_header(conn, mo_id):
    res = {"ERR_MSG": ""}
    sql = f""" SELECT aa.MO_ID,aa.CUST_CODE,aa.CUSTOMER_DEVICE,aa.PRODUCT_PN,TO_CHAR(aa.CREATE_DATE,'YYYY-MM-DD') AS CREATE_DATE,
    bb.LOT_WAFER_ID,bb.LOT_ID,aa.HT_PN,bb.WAFER_SN,aa.REMARK1,aa.MO_TYPE,aa.SAP_PRODUCT_PN,cc.ADD_23,cc.ADD_4,cc.add_17,cc.PO_TYPE,
    cc.ADD_5
    FROM ZM_CDM_MO_HEADER  aa
    INNER JOIN ZM_CDM_MO_ITEM bb ON aa.MO_ID = bb.MO_ID
    INNER JOIN ZM_CDM_PO_ITEM cc ON cc.WAFER_SN = bb.WAFER_SN
    WHERE aa.MO_ID ='{mo_id}'  """

    results = conn.query(sql)
    if not results:
        res["ERR_MSG"] = "工单号不存在"
        return res

    res['MO_ID'] = xstr(results[0][0])
    res['CUST_CODE'] = xstr(results[0][1])
    res['CUSTOMER_DEVICE'] = xstr(results[0][2])
    res['PRODUCT_PN'] = xstr(results[0][3])
    res['CREATE_DATE'] = xstr(results[0][4])
    res['MO_WAFER_ID'] = xstr(results[0][5])
    res['MO_LOT_ID'] = xstr(results[0][6])
    res['HT_PN'] = xstr(results[0][7])
    res['MO_WAFER_SN'] = xstr(results[0][8])
    res['REMAKR1'] = xstr(results[0][9])
    res['MO_TYPE'] = xstr(results[0][10])
    res['SAP_PRODUCT_PN'] = xstr(results[0][11])
    res['MARKING_CODE_DC'] = xstr(results[0][12])
    res['ADD_4'] = xstr(results[0][13])
    res['ADD_17'] = xstr(results[0][14])
    res['PO_TYPE'] = xstr(results[0][15])
    res['ADD_5'] = xstr(results[0][16])

    # 物料主数据
    mat_info = get_sap_mat_info(res['SAP_PRODUCT_PN'])
    if mat_info:
        res['ZZPROCESS'] = mat_info['ZZPROCESS']
    else:
        res['ZZPROCESS'] = ""

    return res


# 获取FC工单创建日期
def get_fc_mo_create_date(conn, conn_or, mo_wafer_id):
    mo_wafer_id = mo_wafer_id.replace('+', '')
    mo_date = ""
    sql = f""" SELECT TO_CHAR(b.CREATE_DATE,'YYYY-MM-DD') FROM ZM_CDM_MO_ITEM a
    INNER JOIN ZM_CDM_MO_HEADER B ON a.MO_ID  = b.MO_ID
    WHERE replace(a.LOT_WAFER_ID,'+','') = '{mo_wafer_id}'
    AND a.FLAG = '1' AND b.FLAG = '1'
    AND b.PRODUCT_PN like '%FC' ORDER BY B.CREATE_DATE desc
    """
    results = conn.query(sql)
    if results:
        mo_date = xstr(results[0][0])
    else:
        sql = f""" SELECT TO_CHAR(b.ERPCREATEDATE,'YYYY-MM-DD') FROM IB_WAFERLIST a
        INNER JOIN IB_WOHISTORY B
        ON a.ORDERNAME = b.ORDERNAME
        WHERE REPLACE (a.WAFERID,'+','')= '{mo_wafer_id}'
        AND b.PRODUCT LIKE '%FC'
        """
        results = conn_or.query(sql)
        if len(results) == 1:
            mo_date = xstr(results[0][0])

    return mo_date


# 获取FC段打标码
def get_fc_mo_mark_code(mo_wafer_id):
    conn = conn_db.HanaConn()
    conn_or = conn_db.OracleConn()
    mo_wafer_id = mo_wafer_id.replace('+', '')
    fc_mark_code = ""
    sql = f""" SELECT a.mark_code FROM ZM_CDM_MO_ITEM a
    INNER JOIN ZM_CDM_MO_HEADER B ON a.MO_ID  = b.MO_ID
    WHERE replace(a.LOT_WAFER_ID,'+','') = '{mo_wafer_id}'
    AND a.FLAG = '1' AND b.FLAG = '1'
    AND b.PRODUCT_PN like '%FC'
     """
    results = conn.query(sql)
    if len(results) == 1:
        fc_mark_code = xstr(results[0][0])
    else:
        sql = f""" SELECT a.MARKINGCODE FROM IB_WAFERLIST a
        INNER JOIN IB_WOHISTORY B
        ON a.ORDERNAME = b.ORDERNAME
        WHERE REPLACE (a.WAFERID,'+','')= '{mo_wafer_id}'
        AND b.PRODUCT LIKE '%FC'
        """
        results = conn_or.query(sql)
        if len(results) == 1:
            fc_mark_code = xstr(results[0][0])

    return fc_mark_code


# 获取首次工单创建日期
def get_wafer_first_mo_create_date(conn, conn_or, mo_wafer_id):
    mo_date = ""
    mo_wafer_id = mo_wafer_id.replace('+', '')

    # 查询老系统
    sql = f"""SELECT  TO_CHAR(b.ERPCREATEDATE,'YYYY-MM-DD') FROM IB_WAFERLIST a
        INNER JOIN IB_WOHISTORY B
        ON a.ORDERNAME = b.ORDERNAME
        WHERE a.WAFERID = '{mo_wafer_id}'
    """
    results = conn_or.query(sql)
    if results:
        mo_date = xstr(results[0][0])
    else:
        # 查询新系统
        sql = f"""SELECT  TO_CHAR(a.CREATE_DATE,'YYYY-MM-DD') FROM ZM_CDM_MO_HEADER a
            INNER JOIN ZM_CDM_MO_ITEM b
            ON a.MO_ID = b.MO_ID WHERE a.FLAG = '1'
            AND REPLACE(b.LOT_WAFER_ID,'+','')  = '{mo_wafer_id}'
            ORDER BY a.CREATE_DATE
        """
        results = conn.query(sql)
        if results:
            mo_date = xstr(results[0][0])

    return mo_date


# 获取工单创建年周DC
def get_mo_date_code(conn, conn_or, mo_header, mo_date):
    mo_date_code = ''

    # FT DC=> 取对应wafer FC的DC
    if mo_header['PRODUCT_PN'][-2:] == 'FT' and mo_header['ZZPROCESS'] != "FC+FT":
        mo_date = get_fc_mo_create_date(
            conn, conn_or, mo_header['MO_WAFER_ID'])
        if mo_date:
            mo_date_code = get_dc(mo_date, 7)

    else:
        if mo_header['CUST_CODE'] == 'US026' or mo_header['CUST_CODE'] == 'SG005':
            if mo_date:
                mo_date_code = get_dc(mo_date, 4)

        elif mo_header['CUST_CODE'] in ('AA', 'US010'):
            if mo_header['MO_ID'][1:2] == 'R':
                mo_date = get_wafer_first_mo_create_date(
                    conn, conn_or, mo_header['MO_WAFER_ID'])
                if mo_date:
                    mo_date_code = get_dc(mo_date, 6)

            else:
                if mo_date:
                    mo_date_code = str(int(get_dc(mo_date, 6)) - 1)

        # elif mo_header['CUST_CODE'] in ('GD55', 'US337'):
        elif mo_header['CUST_CODE'] in ('US337'):
            if mo_date:
                mo_date_code = get_dc(mo_date, 1)

        elif mo_header['CUST_CODE'] in ('GD55'):
            if mo_date:
                mo_date_code = str(int(get_dc(mo_date, 7)) - 1)

        else:
            if mo_date:
                mo_date_code = get_dc(mo_date, 7)

    return mo_date_code


# 37标签符号转换
def get_37_label_symbol(mo_header):
    if "RCLAMP" in mo_header["CUSTOMER_DEVICE"]:
        label1 = mo_header["CUSTOMER_DEVICE"].replace(
            "RCLAMP", "RCLAMP{R}").replace(".P2", "")
        label2 = "RailClamp{R}"

    elif "UCLAMP" in mo_header["CUSTOMER_DEVICE"]:
        label1 = mo_header["CUSTOMER_DEVICE"].replace(
            "UCLAMP", "UCLAMP{R}").replace(".P2", "")
        label2 = "MicroClamp{TM}"

    elif "ECLAMP" in mo_header["CUSTOMER_DEVICE"]:
        label1 = mo_header["CUSTOMER_DEVICE"].replace(
            "ECLAMP", "ECLAMP{TM}").replace(".P2", "")
        label2 = "EMIClamp{TM}"

    elif "TCLAMP" in mo_header["CUSTOMER_DEVICE"]:
        label1 = mo_header["CUSTOMER_DEVICE"].replace(
            "TCLAMP", "TCLAMP{TM}").replace(".P2", "")
        label2 = "TransClamp{TM}"

    elif "HCLAMP" in mo_header["CUSTOMER_DEVICE"]:
        label1 = mo_header["CUSTOMER_DEVICE"].replace(
            "HCLAMP", "HCLAMP{TM}").replace(".P2", "")
        label2 = ""

    elif "PCLAMP" in mo_header["CUSTOMER_DEVICE"]:
        label1 = mo_header["CUSTOMER_DEVICE"].replace(
            "PCLAMP", "PCLAMP{TM}").replace(".P2", "")
        label2 = ""

    elif "HS" in mo_header["CUSTOMER_DEVICE"]:
        label1 = mo_header["CUSTOMER_DEVICE"].replace(
            "HS", "HS").replace(".P2", "")

        label2 = "HotSwitch{TM}"
    else:
        label1 = ""
        label2 = ""

    return label1, label2


def get_mo_header_attrs_COMMON(attrs, conn, conn_or, mo_header):
    sql = f"SELECT distinct PKG_TYPE FROM TBLTSVNPIPRODUCT where QTECHPTNO2 = '{mo_header['PRODUCT_PN']}' "
    results = conn_or.query(sql)
    if results:
        attrs['PACKAGE'] = xstr(results[0][0])


def get_mo_header_attrs_FC(attrs, conn, conn_or, mo_header):
    sql = f"SELECT distinct STRUCKSTR3,XIANGSU FROM TBLTSVNPIPRODUCT t WHERE QTECHPTNO2 = '{mo_header['PRODUCT_PN']}' "
    results = conn_or.query(sql)
    if results:
        attrs['FC_BD'] = xstr(results[0][0])
        attrs['FC_GRADE'] = xstr(results[0][1])


def get_mo_header_attrs_HD(attrs, conn, conn_or, mo_header):
    pass
    # ->add_0
    # sql = f"SELECT add_0 FROM ZM_CDM_PO_ITEM WHERE WAFER_SN= '{mo_header['MO_WAFER_SN']}' "
    # results = conn.query(sql)
    # if results:
    #     # sql_ora = f"SELECT CUST_PN FROM hw_pn where "
    #     if xstr(results[0][0]) == 'SYH650UIC':
    #         attrs['HW_PN'] = '39070489'


def get_mo_header_attrs_57(attrs, conn, conn_or, mo_header):
    sql = f"SELECT add_4 FROM ZM_CDM_PO_ITEM WHERE WAFER_SN= '{mo_header['MO_WAFER_SN']}' "
    results = conn.query(sql)
    if results:
        # sql_ora = f"SELECT CUST_PN FROM hw_pn where "
        if xstr(results[0][0]) == 'SYH650UIC':
            attrs['HW_PN'] = '39070489'


def get_mo_header_attrs_37(attrs, conn, conn_or, mo_header):
    # 37 ORM_DEVICE
    orm_device = ('HCLAMP1831ZBTFT', 'RCLAMP5021ZATFT',
                  'HCLAMP1831ZBTFT', 'RCLAMP5021ZATFT')

    if mo_header['CUSTOMER_DEVICE'] in orm_device:
        sql = f"SELECT lot_num FROM CUST_LOT_NUM where lot = '{mo_header['LOT_ID']}' "
        results = conn_or.query(sql)
        if results:
            attrs['ORM_DEVICE'] = "ORM_NUM:" + xstr(results[0][0])

    # ★ 37 标签特殊字符: 37_CUST_DEVICE1, 37_CUST_DEVICE2
    attrs['37_CUST_DEVICE1'], attrs['37_CUST_DEVICE2'] = get_37_label_symbol(
        mo_header)

    # ★  37 客户指定重工回写JOBID+R, 只维护一次
    attrs['37_RETURN'] = mo_header['REMAKR1']
    if attrs['37_RETURN'] == 'Y':
        if mo_header['MO_ID'][1:2] != 'R':
            return "客户指定重工的工单前缀第二位必须为R"

        sql = f"UPDATE ZM_CDM_PO_ITEM SET add_4 = add_4 || 'R' WHERE MO_ID = '{mo_header['MO_ID']}' AND SUBSTRING(add_4,LENGTH (add_4),1) <> 'R' "
        conn.exec_n(sql)

    # ★★★ 37 JOB ID, SERVICE_CODE => SSP: AS TS     FC: BS AS TS
    sql = f"select add_4,add_17 from zm_cdm_po_item where wafer_sn = '{mo_header['MO_WAFER_SN']}'  "
    results = conn.query(sql)
    attrs['JOBID'] = xstr(results[0][0])
    attrs['SERVICE_CODE'] = xstr(results[0][1])

    # 客户订单检查JOBID
    if mo_header['PO_TYPE'][:3] == "ZOR":
        if not attrs['JOBID']:
            return "JOBID不可为空"

        if not attrs['SERVICE_CODE'] in ("AS", "TS", "BS"):
            return f"US337 SERVICE_CODE:{attrs['SERVICE_CODE']} 错误 =>必须是 AS,TS,BS 之一"

    # TEST段 JOB DC :2段JOB开工单的DC 只能开一次, 厂内重工除外
    attrs['REFLOW_WARNING'] = ""
    if (mo_header['PO_TYPE'][:3] == "ZOR" or attrs['37_RETURN'] == 'Y') and attrs['SERVICE_CODE'] == "TS":
        test_job_id = attrs['JOBID']
        test_job_dc = get_dc(mo_header['CREATE_DATE'], 1)
        attrs['ERP_CREATE_DATE'] = test_job_dc

        sql = f"SELECT * FROM ZH_MES_REFERENCE WHERE KEY1 = '{test_job_id}' AND PROPERTY_NAME = '37JOB_DC' "
        results = conn.query(sql)
        if results:
            return "此JOB已开立过工单，请确认同JOB其他WAFER的工单信息; 37测试段JOB必须开在一张工单"

        # 第一次插入TEST JOB DC记录
        sql = f""" INSERT INTO ZH_MES_REFERENCE(ID,KEY1,KEY2,KEY3,PROPERTY_NAME,PROPERTY_VALUE,VALUE_FLAG,CREATED_BY ,CREATED_TIME)
                values('37JOB_DC','{test_job_id}','{mo_header['MO_ID']}','NULL','37JOB_DC','{test_job_dc}','0','07885',to_char(now(),'YYYY-MM-DD')) """
        conn.exec_n(sql)

        # 工单标记TEST JOB
        sql = f"""UPDATE ZM_CDM_MO_HEADER SET MO_PRIORITY='TEST_JOB' WHERE MO_ID = '{mo_header['MO_ID']}' """
        conn.exec_n(sql)

        # 测试段机种:前50批提示
        sql = f" SELECT count(*) FROM ZM_CDM_MO_HEADER WHERE CUSTOMER_DEVICE = '{mo_header['CUSTOMER_DEVICE']}' AND FLAG = '1' "
        results = conn.query(sql)
        part_cnt = results[0][0]
        if part_cnt <= 50:
            attrs['REFLOW_WARNING'] = "需要抽样500颗做reflow*3次，请通知测试IPQC"
        else:
            attrs['REFLOW_WARNING'] = ""

    return ''


def get_first_time_device(wafer_sn, conn, conn_or):
    sql = f"SELECT LOT_WAFER_ID FROM ZM_CDM_PO_ITEM zcpi WHERE WAFER_SN = '{wafer_sn}' "
    results = conn.query(sql)
    lot_wafer_id = xstr(results[0][0])

    sql = f'''SELECT mpn_desc FROM MAPPINGDATATEST m
            INNER JOIN CUSTOMEROITBL_TEST ct
            ON to_char(ct.id) = m.FILENAME
            AND ct.SOURCE_BATCH_ID = m.LOTID
            WHERE m.SUBSTRATEID = '{lot_wafer_id.replace('+','')}'
        '''

    results = conn_or.query(sql)
    if results:
        cust_device = xstr(results[0][0])
    else:
        sql = f"SELECT CUSTOMER_DEVICE FROM ZM_CDM_PO_ITEM zcpi WHERE LOT_WAFER_ID = '{lot_wafer_id}' order by create_date "
        results2 = conn.query(sql)
        if results2:
            cust_device = xstr(results2[0][0])
        else:
            return ''

    return cust_device


def get_mo_header_attrs_AA(attrs, conn, conn_or, mo_header):
    sql = f'''SELECT a.MSL,b.NUMBEROFHOURS,a.TEMP,a.PBF_DIE_ATTACH,a.ecat,a.LEAD_FREE,a.ipn,a.LOC
        FROM CUSTOMERMPNAttributes a
        left JOIN CUSTOMERMSLevelTBL b ON a.MSL = b.MS_LEVEL
        WHERE a.PART = '{mo_header['CUSTOMER_DEVICE']}'
    '''
    results = conn_or.query(sql)
    if results:
        attrs['AAMPN_DEVICE'] = mo_header['CUSTOMER_DEVICE']
        attrs['AAMPN_MSL'] = xstr(results[0][0])
        attrs['AAMSL_HOURS'] = xstr(results[0][1])
        attrs['AAMPN_TEMP'] = xstr(results[0][2])
        attrs['AAMPN_PBF'] = xstr(results[0][3])
        attrs['AAMPN_ECAT'] = xstr(results[0][4])
        attrs['AAMPN_UL'] = xstr(results[0][5])
        attrs['AA_IPN'] = xstr(results[0][6])
        attrs['AA_LOC'] = xstr(results[0][7])
    else:
        sql = f""" SELECT
            MAX(map( t1.SUB_ID ,'MSL',t1.value,'' ) ) AS MSL,
            MAX(map( t1.SUB_ID ,'PMC',t1.value,'' ) ) AS PMC,
            MAX(map( t1.SUB_ID ,'TEMP',t1.value,'' ) ) AS TEMP,
            MAX(map( t1.SUB_ID ,'PBF_DIE_ATTACH',t1.value,'' ) ) AS PBF_DIE_ATTACH,
            MAX(map( t1.SUB_ID ,'ECAT',t1.value,'' ) ) AS ECAT,
            MAX(map( t1.SUB_ID ,'LEAD_FREE',t1.value,'' ) ) AS LEAD_FREE,
            MAX(map( t1.SUB_ID ,'IPN',t1.value,'' ) ) AS IPN,
            MAX(map( t1.SUB_ID ,'LOC',t1.value,'' ) ) AS LOC,
            t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME in ('MT05') and t1.MAIN_ID = '{mo_header['CUSTOMER_DEVICE']}'
            group by t1.MAIN_ID,t1.MT_ID
        """
        results = conn.query(sql)
        if results:
            attrs['AAMPN_DEVICE'] = mo_header['CUSTOMER_DEVICE']
            attrs['AAMPN_MSL'] = xstr(results[0][0])

            sql = f"select NUMBEROFHOURS from CUSTOMERMSLevelTBL where MS_LEVEL = '{attrs['AAMPN_MSL']}' "
            results2 = conn_or.query(sql)
            if results2:
                attrs['AAMSL_HOURS'] = results2[0][0]
            else:
                attrs['AAMSL_HOURS'] = ""
            attrs['AAMPN_TEMP'] = xstr(results[0][2])
            attrs['AAMPN_PBF'] = xstr(results[0][3])
            attrs['AAMPN_ECAT'] = xstr(results[0][4])
            attrs['AAMPN_UL'] = xstr(results[0][5])
            attrs['AA_IPN'] = xstr(results[0][6])
            attrs['AA_LOC'] = xstr(results[0][7])


def get_mo_header_attrs_GLASS(attrs, conn, conn_or, mo_header):
    sql = f"SELECT customerptno3,customerptno4,customerptno5,customerptno6 FROM tbltsvnpiproduct where qtechptno2 = '{mo_header['PRODUCT_PN']}' "
    results = conn_or.query(sql)
    if results:
        attrs['GLASS_NAME'] = xstr(results[0][0]).replace(
            '一步清洗', 'One steps').replace('二步清洗', 'Two steps')
        attrs['GLASS_CV'] = xstr(results[0][1])
        attrs['GLASS_ID'] = xstr(results[0][2])
        attrs['GLASS_TYPE'] = xstr(results[0][3]).replace('康宁', 'KN')


def get_mo_header_attrs_GD108(attrs, conn, conn_or, mo_header):
    sql = f"SELECT DISTINCT CUST_PART,BIN1_DEVICE, BIN2_DEVICE from CUST_BIN_DEVICE WHERE CUST_PART = '{mo_header['CUSTOMER_DEVICE']}'"
    results = conn_or.query(sql)
    if results:
        attrs['BIN1_DEVICE'] = xstr(results[0][1])
        attrs['BIN2_DEVICE'] = xstr(results[0][2])


def get_mo_header_attrs_EU010(attrs, conn, conn_or, mo_header):
    sql = f"SELECT distinct PRODUCT_12NC,PMC,MARKING_CODE,PACKAGE,ORIG,PROVENANCE FROM EU010_REFERENCE WHERE CUST_DEVICE = '{mo_header['CUSTOMER_DEVICE']}' "
    results = conn_or.query(sql)
    if results:
        attrs['EU010_12NC'] = xstr(results[0][0])
        attrs['EU010_PMC'] = xstr(results[0][1])
        attrs['EU010_MAKLING_CODE'] = xstr(results[0][2])
        attrs['EU010_PACKAGE'] = xstr(results[0][3])
        attrs['EU010_ORIG'] = xstr(results[0][4])
        attrs['EU010_PROVENANCE'] = xstr(results[0][5])
    else:
        sql = f""" SELECT
            MAX(map( t1.SUB_ID ,'PRODUCT_12NC',t1.value,'' ) ) AS PRODUCT_12NC,
            MAX(map( t1.SUB_ID ,'PMC',t1.value,'' ) ) AS PMC,
            MAX(map( t1.SUB_ID ,'MARKING_CODE',t1.value,'' ) ) AS MARKING_CODE,
            MAX(map( t1.SUB_ID ,'PACKAGE',t1.value,'' ) ) AS PACKAGE,
            MAX(map( t1.SUB_ID ,'ORIG',t1.value,'' ) ) AS ORIG,
            MAX(map( t1.SUB_ID ,'PROVENANCE',t1.value,'' ) ) AS PROVENANCE,
            MAX(map( t1.SUB_ID ,'DEVICE_NAME',t1.value,'' ) ) AS DEVICE_NAME,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_01',t1.value,'' ) ) AS EU010_ATTR_01,
            MAX(map( t1.SUB_ID ,'EU010_ATTR_02',t1.value,'' ) ) AS EU010_ATTR_02,
            t1.MT_ID
            FROM
            ZM_CDM_COMMON_MAINTAIN_INFO t1
            where t1.GROUP_NAME in ('MT03','MT04') and t1.MAIN_ID = '{mo_header['CUSTOMER_DEVICE']}'
            group by t1.MAIN_ID,t1.MT_ID
        """
        results = conn.query(sql)
        if results:
            attrs['EU010_12NC'] = xstr(results[0][0])
            attrs['EU010_PMC'] = xstr(results[0][1])
            attrs['EU010_MAKLING_CODE'] = xstr(results[0][2])
            attrs['EU010_PACKAGE'] = xstr(results[0][3])
            attrs['EU010_ORIG'] = xstr(results[0][4])
            attrs['EU010_PROVENANCE'] = xstr(results[0][5])


# 更新订单表打标码
def update_po_item_mark_code(con, wafer_sn, mark_code):
    sql = f"UPDATE ZM_CDM_PO_ITEM SET MARK_CODE = '{mark_code}',UPDATE_DATE=NOW() WHERE WAFER_SN = '{wafer_sn}' "
    con.exec_n(sql)


# 获取wafer层级属性
def get_mo_wafer_level_attributes(conn, mo_wafer_sn):
    po_data = get_wafer_po_data(conn, mo_wafer_sn)
    if not po_data:
        err_msg = "无法获取WAFER的订单数据"
        return err_msg

    attrs = {}

    # 公共属性
    attrs['COMP_CODE'] = 'HTKS'
    attrs['SHIP_TO'] = po_data.get('CUST_CODE', '')
    attrs['WAFER_CUST_PART_NUM1'] = po_data.get('CUSTOMER_DEVICE', '')
    attrs['CUSTOMER_PO'] = po_data.get('PO_ID', '')
    attrs['INDATE'] = po_data['CREATE_DATE']
    attrs['PODATE'] = attrs['INDATE'][:10]

    # FT的工单查询FC的打标码
    attrs['MARKING_CODE'] = po_data.get('MARK_CODE', '')
    if po_data['PRODUCT_PN'][-2:] == "FT" and not attrs['MARKING_CODE']:
        attrs['MARKING_CODE'] = get_fc_mo_mark_code(po_data['LOT_WAFER_ID'])

    # FT无条件继承FC段
    if po_data['CUST_CODE'] in ('AA08', 'SH296', 'AT71', 'SC057') and po_data['PRODUCT_PN'][-2:] == "FT":
        fc_mark_code = get_fc_mo_mark_code(po_data['LOT_WAFER_ID'])
        if fc_mark_code:
            attrs['MARKING_CODE'] = fc_mark_code
            update_po_item_mark_code(conn, mo_wafer_sn, fc_mark_code)

    attrs['GC_SEC_CODE2'] = po_data.get('ADD_0', '')
    attrs['SHIP_COMMENT'] = po_data.get('ADD_1', '')
    attrs['PROBE_SHIP_PART_TYPE'] = po_data.get('ADD_2', '')
    attrs['CUST_INVOICENUMBER'] = po_data.get(
        'ADD_3', '').replace('裂片', 'Lobes')
    attrs['CUST_FROMSITE'] = po_data.get('ADD_4', '')
    attrs['ASSYEMBLY_FACILITY'] = po_data.get('ADD_6', '')
    attrs['ZX_SHIP_TO'] = po_data.get('ADD_7', '')
    attrs['CUST_PART_NUM2'] = po_data.get('FAB_DEVICE', '')
    attrs['WAFER_SN'] = mo_wafer_sn

    attrs['WO_B'] = po_data.get('PO_ID', '')
    attrs['WO_C'] = po_data.get('CUST_CODE', '')
    attrs['WO_D'] = po_data.get('ADDRESS_CODE', '')
    attrs['WO_E'] = po_data.get('FAB_DEVICE', '')
    attrs['WO_F'] = po_data.get('CUSTOMER_DEVICE', '')
    attrs['WO_G'] = po_data.get('ADD_0', '')
    attrs['WO_H'] = po_data.get('PO_H', '')
    attrs['WO_I'] = po_data.get('PO_DATE', '')
    attrs['WO_J'] = po_data.get('LOT_ID', '')
    attrs['WO_K'] = po_data.get('WAFER_ID', '')
    attrs['WO_L'] = po_data.get('PASSBIN_CNT', '')
    attrs['WO_M'] = po_data.get('PASSBIN_CNT', '')
    attrs['WO_N'] = po_data.get('HT_PN', '')
    attrs['WO_O'] = po_data.get('ADD_1', '')
    attrs['WO_P'] = po_data.get('ADD_2', '')
    attrs['WO_Q'] = po_data.get('ADD_3', '')
    attrs['WO_R'] = po_data.get('ADD_4', '')
    attrs['WO_S'] = po_data.get('ADD_5', '')
    attrs['WO_T'] = po_data.get('ADD_6', '')
    attrs['WO_U'] = po_data.get('ADD_7', '')
    attrs['WO_V'] = po_data.get('ADD_8', '')
    attrs['WO_W'] = po_data.get('ADD_9', '')
    attrs['WO_X'] = po_data.get('ADD_10', '')
    attrs['WO_Y'] = po_data.get('ADD_11', '')
    attrs['WO_Z'] = po_data.get('ADD_12', '')

    if po_data['CUST_CODE'] in ('AT71', 'SZ280'):
        attrs['REAL_WAFER_ID'] = attrs['WO_W']
    
    if po_data['CUST_CODE'] in ('JS195'):
        attrs['REAL_WAFER_ID'] = attrs['WO_O']

    if po_data['CUST_CODE'] in ('AT34'):
        attrs['WO_AA'] = po_data.get('ADD_13', '')
        attrs['WO_AB'] = po_data.get('ADD_14', '')
        attrs['WO_AC'] = po_data.get('ADD_15', '')
        attrs['WO_AD'] = po_data.get('ADD_16', '')
        attrs['WO_AE'] = po_data.get('ADD_17', '')
        attrs['WO_AF'] = po_data.get('ADD_18', '')
        attrs['WO_AG'] = po_data.get('ADD_19', '')
        attrs['WO_AH'] = po_data.get('ADD_20', '')
        attrs['WO_AI'] = po_data.get('ADD_21', '')
        attrs['WO_AJ'] = po_data.get('ADD_22', '')
        attrs['WO_AK'] = po_data.get('ADD_23', '')
        attrs['WO_AL'] = po_data.get('ADD_24', '')
        attrs['WO_AM'] = po_data.get('ADD_25', '')

    # 特定客户属性
    if po_data['CUST_CODE'] in ('HK109', 'SH40', 'ZJ139'):
        err_msg = get_mo_wafer_attrs_GC(attrs, po_data)
        if err_msg:
            return err_msg

    if po_data['CUST_CODE'] in ('SH48'):
        attrs['CUST_PART_NUM2'] = attrs['WO_O']

    if po_data['CUST_CODE'] in ('BJ49'):
        get_mo_wafer_attrs_SX(attrs, po_data)

    if po_data['CUST_CODE'] in ('US337'):
        get_mo_wafer_attrs_37(conn, attrs, po_data)

    if po_data['CUST_CODE'] in ('AT51'):
        err_msg = get_mo_wafer_attrs_EQ(attrs, po_data)
        if err_msg:
            return err_msg

    if po_data['CUST_CODE'] in ('SH07', 'XD36', 'SC060', 'BJ139', 'JS161', 'FJ030', 'AT93', 'XD66', 'HK010', 'FJ030', 'CQ015', 'GD188'):
        get_mo_wafer_attrs_SH07(attrs, po_data)

    if po_data['CUST_CODE'] in ('ZJ48', 'ZJ35'):
        get_mo_wafer_attrs_57(attrs, po_data)

    if po_data['CUST_CODE'] in ('AA', 'US010'):
        err_msg = get_mo_wafer_attrs_AA(attrs, po_data)
        if err_msg:
            return err_msg

    if po_data['CUST_CODE'] in ('AT71'):
        err_msg = get_mo_wafer_attrs_AT71(attrs, po_data)
        if err_msg:
            return err_msg

    if po_data['CUST_CODE'] in ('US008', '70', 'HK099', 'HK006'):
        get_mo_wafer_attrs_68(attrs, po_data)

    if po_data['CUST_CODE'] in ('GD55'):
        err_msg = get_mo_wafer_attrs_GD55(attrs, po_data)
        if err_msg:
            return err_msg

    # 检查打标码
    if po_data['PO_TYPE'][:3] != 'YP1':
        err_msg = check_wafer_mark_code(
            po_data['HT_PN'], po_data['LOT_WAFER_ID'], attrs['MARKING_CODE'])
        if err_msg:
            return err_msg

    return attrs


def get_mo_wafer_attrs_GD55(attrs, po_data):
    con = conn_db.HanaConn()
    lot_wafer_id = po_data['LOT_WAFER_ID'].replace('+', '')

    # 二段测试程序抓一段
    sql = f"SELECT ADD_2 FROM ZM_CDM_PO_ITEM WHERE LOT_WAFER_ID like '{lot_wafer_id}%' AND ADD_0 LIKE '02%' "
    results = con.query(sql)
    if not results and po_data['PO_TYPE'][:3] != 'YP1' and po_data['HT_PN'] != 'YHD013B' and po_data['HT_PN'] != 'YHD031B':
        err_msg = f"WAFER_ID:{lot_wafer_id},找不到一次订单的测试程序"
        return err_msg

    if results:
        attrs['PROBE_SHIP_PART_TYPE'] = xstr(results[0][0])
    else:
        attrs['PROBE_SHIP_PART_TYPE'] = ''
    return ''


def check_wafer_mark_code(ht_pn, wafer_id, mark_code):
    err_msg = ''

    con_or = conn_db.OracleConn()

    # 检查1: NPI维护规则对比
    sql = f"SELECT REMARK FROM TBL_MARKINGCODE_REP  WHERE HT_PN = '{ht_pn}' "
    results = con_or.query(sql)
    if results:
        mark_rule_code = xstr(results[0][0]).replace('\\\\', '\\')
        mark_code = mark_code.replace('\\\\', '\\')
        print(mark_rule_code, mark_code)

        # 位数检查
        if len(mark_rule_code) != len(mark_code):
            err_msg = f"打标位数错误,NPI设定位数:{len(mark_rule_code)}, 当前位数:{len(mark_code)}"
            return err_msg

        # 字符检查
        for i in range(len(mark_rule_code)):
            if mark_rule_code[i] == '*':
                continue

            if mark_rule_code[i] != mark_code[i]:
                err_msg = f"{wafer_id}:第{i+1}位打标字符错误,NPI设定固定字符:{mark_rule_code[i]}, 当前异常字符:{mark_code[i]}"
                print(mark_rule_code[i], mark_code[i], err_msg)

                return err_msg

    # 检查2:特殊客户打标码重复控制

    # 检查通过
    return err_msg


# 获取wafer的PO信息
def get_wafer_po_data(conn, wafer_sn):
    sql = f'''SELECT ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,ADD_6,ADD_7,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,
    ADD_14,ADD_15,PO_ID,PO_DATE,MARK_CODE,CUSTOMER_DEVICE,CUST_CODE,FAB_DEVICE,LOT_ID,CUST_FAB_DEVICE_1,HT_PN,
    LOT_WAFER_ID,ADD_30,ADD_28,ADD_29,WAFER_ID,PO_TYPE,ADDRESS_CODE,TO_char(CREATE_DATE,'YYYY-MM-DD hh:mm:ss') ,
    PO_H,passbin_count,PRODUCT_PN,WAFER_SN,CREATE_BY,ADD_16,ADD_17,ADD_18,ADD_19,ADD_20,ADD_21,ADD_22,ADD_23,ADD_24,ADD_25,ADD_26,ADD_27
    FROM ZM_CDM_PO_ITEM WHERE WAFER_SN = '{wafer_sn}' '''

    results = conn.query(sql)
    if not results:
        return None
    po_data = {}
    po_data['ADD_0'] = xstr(results[0][0])
    po_data['ADD_1'] = xstr(results[0][1])
    po_data['ADD_2'] = xstr(results[0][2])
    po_data['ADD_3'] = xstr(results[0][3])
    po_data['ADD_4'] = xstr(results[0][4])
    po_data['ADD_5'] = xstr(results[0][5])
    po_data['ADD_6'] = xstr(results[0][6])
    po_data['ADD_7'] = xstr(results[0][7])
    po_data['ADD_8'] = xstr(results[0][8])
    po_data['ADD_9'] = xstr(results[0][9])
    po_data['ADD_10'] = xstr(results[0][10])
    po_data['ADD_11'] = xstr(results[0][11])
    po_data['ADD_12'] = xstr(results[0][12])
    po_data['ADD_13'] = xstr(results[0][13])
    po_data['ADD_14'] = xstr(results[0][14])
    po_data['ADD_15'] = xstr(results[0][15])
    po_data['PO_ID'] = xstr(results[0][16])
    po_data['PO_DATE'] = xstr(results[0][17])
    po_data['MARK_CODE'] = xstr(results[0][18])
    po_data['CUSTOMER_DEVICE'] = xstr(results[0][19])
    po_data['CUST_CODE'] = xstr(results[0][20])
    po_data['FAB_DEVICE'] = xstr(results[0][21])
    po_data['LOT_ID'] = xstr(results[0][22])
    po_data['CUST_FAB_DEVICE_1'] = xstr(results[0][23])
    po_data['HT_PN'] = xstr(results[0][24])
    po_data['LOT_WAFER_ID'] = xstr(results[0][25])
    po_data['ADD_30'] = xstr(results[0][26])
    po_data['ADD_28'] = xstr(results[0][27])
    po_data['ADD_29'] = xstr(results[0][28])
    po_data['WAFER_ID'] = xstr(results[0][29])
    po_data['PO_TYPE'] = xstr(results[0][30])
    po_data['ADDRESS_CODE'] = xstr(results[0][31])
    po_data['CREATE_DATE'] = xstr(results[0][32])
    po_data['PO_H'] = xstr(results[0][33])
    po_data['PASSBIN_CNT'] = xstr(results[0][34])
    po_data['PRODUCT_PN'] = xstr(results[0][35])
    po_data['WAFER_SN'] = xstr(results[0][36])
    po_data['CREATE_BY'] = xstr(results[0][37])
    po_data['ADD_16'] = xstr(results[0][38])
    po_data['ADD_17'] = xstr(results[0][39])
    po_data['ADD_18'] = xstr(results[0][40])
    po_data['ADD_19'] = xstr(results[0][41])
    po_data['ADD_20'] = xstr(results[0][42])
    po_data['ADD_21'] = xstr(results[0][43])
    po_data['ADD_22'] = xstr(results[0][44])
    po_data['ADD_23'] = xstr(results[0][45])
    po_data['ADD_24'] = xstr(results[0][46])
    po_data['ADD_25'] = xstr(results[0][47])

    return po_data


# HK109:工单Wafer层专用属性
def get_mo_wafer_attrs_GC(attrs, po_data):
    if len(po_data['MARK_CODE'].replace('_', '')) < 3:
        return f"GC打标码异常, 必须为4位或5位,当前打标码为:{po_data['MARK_CODE']}"

    attrs['MARKING_CODE'] = po_data.get('MARK_CODE', '').replace('_', '')
    attrs['GC_SEC_CODE1'] = po_data.get('ADD_4', '')
    attrs['GC_SEC_CODE2'] = po_data.get('ADD_0', '')
    attrs['GC_TYPE'] = po_data.get('ADD_5', '')
    attrs['GC_WO'] = po_data.get('ADD_8', '')

    if po_data['PO_TYPE'][:3] == 'ZOR' and not po_data['ADD_0']:
        return f"GC二级代码不可为空"

    if po_data['PO_TYPE'][:3] == 'ZOR' and not po_data['ADD_8']:
        return f"GC_WO不可为空"

    if po_data['PO_TYPE'][:3] == 'ZOR' and not attrs['PODATE']:
        return f"GC_PODATE 不可为空"

    if po_data['PO_TYPE'][:3] == 'ZOR' and not po_data['ADD_5']:
        po_data['ADD_5'] = get_gc_flow(po_data)
        if not po_data['ADD_5']:
            return f"GC_FLOW不可为空"

    return ''


# 获取GC FLOW
def get_gc_flow(po_data):
    con = conn_db.HanaConn()
    product_id = po_data.get('PRODUCT_PN', '')
    wafer_sn = po_data.get('WAFER_SN', '')
    mat_info = mmi.get_mat_master_data(product_no=product_id)
    if mat_info:
        process = mat_info[0]['ZZPROCESS']
        sql = f"update zm_cdm_po_item set ADD_5='{process}',update_date=NOW() where wafer_sn = '{wafer_sn}' "
        con.exec_c(sql)
        return process
    else:
        return ''


# BJ49:工单Wafer层专用属性
def get_mo_wafer_attrs_SX(attrs, po_data):
    # GC_SEC_CODE2 => SX专用 -> BIN2:mappingdatatest.Remark + add_0(通用)
    attrs['GC_SEC_CODE2'] = ''


# US337:工单Wafer层专用属性
def get_mo_wafer_attrs_37(conn, attrs, po_data):
    conn_or = conn_db.OracleConn()
    attrs['CUSTOMER_WORKORDER'] = po_data.get('ADD_11', '')
    attrs['CUSTOMER_MPN'] = po_data.get('ADD_11', '')

    # weight37
    sql = f"SELECT distinct TO_CHAR(CREATE_DATE, 'YYYY-MM-DD') FROM weight37 WHERE WAFERID = '{po_data['LOT_WAFER_ID']}'  "
    results = conn_or.query(sql)
    if results:
        attrs['WEIGHT_YYWW'] = xstr(results[0][0])
        attrs['JOBID_WEIGHT_DATE'] = po_data['ADD_4'] + \
            '/' + xstr(results[0][0])

    # US337 NCMR
    if po_data['ADD_28'] or po_data['ADD_29']:
        ncmr_str = po_data['ADD_28'] if po_data['ADD_28'] else po_data['ADD_29']
        ncmr_list = ["PPR-", "AER-", "NCMR-", "MRB-", "APR-"]

        for row in ncmr_list:
            if row in ncmr_str:
                attrs['37_NCMR'] = ncmr_str[ncmr_str.find(
                    row):ncmr_str.find(row)+len(row)+6]

                # 判断是否已存在同样数据,值不一样则更新值,保留更新记录
                mt_c_val = attrs['37_NCMR']
                mt_id = conn.query(
                    "SELECT ZM_CDM_COMMON_MAINTAIN_INFO_MT_ID_SEQ.NEXTVAL FROM dummy")[0][0]
                sql = f"select VALUE,MT_ID from ZM_CDM_COMMON_MAINTAIN_INFO where GROUP_NAME='MT02' and MAIN_ID='{po_data['LOT_WAFER_ID']}' and SUB_ID='NCMR' "
                results_r = conn.query(sql)
                if results_r:
                    mt_e_val = xstr(results_r[0][0])
                    mt_id = results_r[0][1]
                    if mt_c_val != mt_e_val:
                        sql = f"update ZM_CDM_COMMON_MAINTAIN_INFO set VALUE='{mt_c_val}',REMARK=REMARK || ',' || '{mt_e_val}',UPDATE_DATE=now(),UPDATE_BY='{po_data['CREATE_BY']}' where MT_ID = {mt_id} and SUB_ID='NCMR' "
                        conn.exec_n(sql)
                else:
                    sql = f"insert into ZM_CDM_COMMON_MAINTAIN_INFO(MAIN_ID,SUB_ID,VALUE,GROUP_NAME,CREATE_BY,CREATE_DATE,REMARK,MT_ID,UPDATE_DATE) values('{po_data['LOT_WAFER_ID']}','NCMR','{mt_c_val}','MT02','{po_data['CREATE_BY']}',NOW(),'',{mt_id},NOW()) "
                    conn.exec_n(sql)

                # sql = f"UPDATE ZM_CDM_37_NCMR SET FLAG = '0' WHERE WAFER_ID = '{po_data['LOT_WAFER_ID']}' "
                # conn.exec_n(sql)

                # sql = f"INSERT INTO ZM_CDM_37_NCMR(WAFER_ID,LOT_ID,NCMR,FLAG,CREATE_DATE,CREATE_BY) VALUES('{po_data['LOT_WAFER_ID']}','{po_data['LOT_ID']}','{attrs['37_NCMR']}', '1',NOW(),'SYSTEM') "
                # conn.exec_n(sql)

                # 更新MES
                sql = f"DELETE FROM ZH_MES_REFERENCE WHERE ID = '37_WAFER' AND KEY1='{po_data['LOT_WAFER_ID']}' AND PROPERTY_NAME='NCMR'  "
                conn.exec_n(sql)

                sql = f'''INSERT INTO ZH_MES_REFERENCE(ID,KEY1,KEY2,KEY3,PROPERTY_NAME,PROPERTY_VALUE,VALUE_FLAG,CREATED_BY ,CREATED_TIME)
                        values('37_WAFER','{po_data['LOT_WAFER_ID']}','NULL','NULL','NCMR','{attrs['37_NCMR']}','0','07885',to_char(now(),'YYYY-MM-DD')) '''
                conn.exec_n(sql)

                break


# AA:工单Wafer层专用属性
def get_mo_wafer_attrs_AA(attrs, po_data):
    conn_or = conn_db.OracleConn()

    fab_device = po_data['FAB_DEVICE']
    part_device = po_data['CUST_FAB_DEVICE_1']
    cust_device = po_data['CUSTOMER_DEVICE']
    lot_id = po_data['LOT_ID']
    attrs['CUST_PART_NUM2'] = part_device
    attrs['CUST_PART_NUM3'] = fab_device

    # 老的EBR
    sql = f"SELECT id,ebrnumber,pt,contact FROM CUSTOMEREBRtbl WHERE BATCHID = '{lot_id}' AND pt='{cust_device}'"
    results = conn_or.query(sql)
    if results:
        attrs['EBR_ID'] = xstr(results[0][0])
        attrs['EBR_NUM'] = xstr(results[0][1])
        attrs['EBR_PART'] = xstr(results[0][2])
        attrs['EBR_CONTACR'] = xstr(results[0][3])

    # 样品订单EBR
    if po_data['PO_TYPE'] == 'ZOR1':
        attrs['AA_EBR'] = po_data['PO_ID']

    # 检查DESIGNID
    if po_data['PO_TYPE'][:3] == "ZOR" and not attrs['PROBE_SHIP_PART_TYPE']:
        return "US010 : DESIGNID不可为空"

    attrs['AA_DESIGNID'] = attrs['PROBE_SHIP_PART_TYPE']

    return ""


# 检查AT71 FC CODE
def get_mo_wafer_attrs_AT71(attrs, po_data):
    if "FC" in po_data['PRODUCT_PN'] or "FT" in po_data['PRODUCT_PN']:
        if not attrs['WO_T'][-1:] in ("E", "S", "H",""):
            return "AT71 FC的CODE最后一位必须是E或者S,H 当前CODE为:" + attrs['WO_T'] + ",请通知包装工艺确认"

    return ""


# AT51:工单Wafer层专用属性
def get_mo_wafer_attrs_EQ(attrs, po_data):
    attrs['EQ_PLANNED_LASER_SCRIBE'] = po_data.get('ADD_5', '')
    attrs['CUSTOMER_MPN'] = po_data.get('ADD_3', '')
    attrs['EQ_SO_DC1'] = po_data.get('ADD_7', '')
    attrs['EQ_SO_DC2'] = po_data.get('ADD_4', '')
    attrs['SPA_NO'] = po_data.get('ADD_4', '')
    po_type = po_data['PO_TYPE']
    print(po_type[:3])
    if len(attrs['MARKING_CODE']) != 11 and po_type[:3] == "ZOR":
        return "EQ打标码长度错误,正确的是11位"

    return ""


# SH07(展讯):工单Wafer层专用属性
def get_mo_wafer_attrs_SH07(attrs, po_data):
    attrs['ZX_SHIP_TO'] = po_data.get('ADD_7', '')
    # 加工目的
    attrs['WORK_DESC'] = po_data.get('ADD_4', '')
    # ​委外工单编号
    attrs['WORK_NO'] = po_data.get('ADD_5', '')


# 57：工单Wafer层专用属性
def get_mo_wafer_attrs_57(attrs, po_data):
    attrs['WO_S'] = po_data.get('ADD_5', '')
    attrs['WO_S'] = attrs['WO_S'].replace('南京矽力杰', 'NJ').replace(
        '天水矽力杰', 'TN').replace('西安矽力杰', 'XA').replace('杭州矽力杰', 'HZ').replace('南京矽力微', 'NJW')


def get_mo_wafer_attrs_68(attrs, po_data):
    attrs['FAB_LOT_ID'] = po_data.get(
        'ADD_1', '') + po_data.get('WAFER_ID', '')
    attrs['CUST_PART_NUM2'] = po_data.get('ADD_1', '')
    attrs['COMP_CODE'] = po_data.get('ADDRESS_CODE', '')
    print(attrs)


def get_dc(DA, STRAT_DA):
    con = conn_db.OracleConn()
    DC_RETURN = ""
    begin_da = ""
    begin_num = 0
    convert_num = 0
    DC_YY1 = ""
    DC_YY2 = ""

    sql = f" select to_char(trunc(to_date('{DA}', 'YYYY-MM-DD'), 'yyyy'), 'day') from dual "
    begin_da = con.query(sql)[0][0].strip()

    sql = f''' select TO_NUMBER(decode('{begin_da}',
                            '星期一',
                            1,
                            '星期二',
                            2,
                            '星期三',
                            3,
                            '星期四',
                            4,
                            '星期五',
                            5,
                            '星期六',
                            6,
                            '星期日',
                            7))
      from dual '''
    # begin_num = con.query(sql)[0][0]
    begin_num = 5

    if begin_num - STRAT_DA >= 0:
        convert_num = begin_num - STRAT_DA
    else:
        convert_num = begin_num - STRAT_DA + 7

    DC_YY1 = con.query(
        f"select to_char(to_date('{DA}', 'YYYY-MM-DD') + {convert_num}, 'YY') from dual")[0][0]

    DC_YY2 = con.query(
        f"select to_char(to_date('{DA}', 'YYYY-MM-DD'), 'YY') from dual")[0][0]

    if DC_YY1 == DC_YY2:
        DC_RETURN = con.query(
            f"select to_char(to_date('{DA}', 'YYYY-MM-DD') + {convert_num}, 'YYWW') from dual ")[0][0]

    else:

        DC_RETURN = con.query(
            f"select to_char(add_months(trunc(to_date('{DA}', 'YYYY-MM-DD'), 'YYYY'),12) - 1,'YYWW') from dual")[0][0]

    return DC_RETURN


def start():
    conn = conn_db.HanaConn()
    conn_or = conn_db.OracleConn()
    mo_id = "ATZ-210202226"

    sql = f"SELECT DISTINCT PO_TYPE ,MO_ID  FROM ZM_CDM_PO_ITEM zcpi WHERE CUST_CODE = 'US337' AND add_4 <> ADD_30 AND MO_ID IS NOT NULL AND MO_ID <> '' "
    results = conn.query(sql)
    for row in results:
        mo_id = row[1]

        attrs = get_mo_header_level_attributes(conn, conn_or, mo_id, '')
        if isinstance(attrs, str):
            continue

        CODE37_STAUTS = attrs.get('CODE37_STAUTS', '')
        CODE37_BLINE = attrs.get('CODE37_BLINE', '')
        CODE37_CODE = attrs.get('CODE37_CODE', '')

        if CODE37_STAUTS and CODE37_BLINE and CODE37_CODE:
            sql = f"UPDATE ZD_LOOKUP_EX SET VALUE = '{CODE37_STAUTS}' WHERE  id LIKE '%{mo_id}%' AND SUBID = 'CODE37_STAUTS' "
            conn.exec_n(sql)
            sql = f"UPDATE ZD_LOOKUP_EX SET VALUE = '{CODE37_BLINE}' WHERE  id LIKE '%{mo_id}%' AND SUBID = 'CODE37_BLINE' "
            conn.exec_n(sql)
            sql = f"UPDATE ZD_LOOKUP_EX SET VALUE = '{CODE37_CODE}' WHERE  id LIKE '%{mo_id}%' AND SUBID = 'CODE37_CODE' "
            conn.exec_n(sql)

            print(mo_id, "已经补充完成", CODE37_STAUTS, CODE37_BLINE, CODE37_CODE)

    conn.db.commit()


def run2(mo_id):
    sql = f"SELECT DISTINCT MO_ID FROM ZM_CDM_PO_ITEM zcpi WHERE HT_PN = 'XHDB16B' and mo_id = '{mo_id}' order by MO_ID "
    con = conn_db.HanaConn()

    results = con.query(sql)
    for row in results:
        mo_id = row[0]

        # 获取准确DC
        sql = f"SELECT VALUE FROM ZD_LOOKUP_EX zle WHERE id LIKE '%{mo_id}%' AND SUBID = 'ERP_CREATE_DATE_STRING' "
        results = con.query(sql)
        mo_date = results[0][0]
        mo_date_code = str(int(get_dc(mo_date, 7)) - 1)

        # 获取当前DC
        sql = f"SELECT VALUE FROM ZD_LOOKUP_EX zle WHERE id LIKE '%{mo_id}%' AND SUBID = 'ERP_CREATE_DATE' "
        results = con.query(sql)
        mo_date_code_cur = results[0][0]

        if mo_date_code_cur != mo_date_code:
            print(mo_id, "=>当前:", mo_date_code_cur,  ",正确:", mo_date_code)

            # 修改
            sql = f"UPDATE ZD_LOOKUP_EX SET VALUE = '{mo_date_code}' WHERE id LIKE '%{mo_id}%' AND SUBID = 'ERP_CREATE_DATE' and value = '{mo_date_code_cur}'  "
            con.exec_n(sql)
            print(mo_id, "修改完成")

    con.db.commit()


# ----------------------------
if __name__ == "__main__":
    # run2('BTB-210328949')
    con_1 = conn_db.HanaConn()
    get_wafer_po_data(conn=con_1, wafer_sn='100219398')