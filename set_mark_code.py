import conn_db as conn
import json
import requests
import time
from pp_get_mo_attrs import get_mo_header, get_mo_header_level_attributes
from pp_get_mo_attrs import get_mo_wafer_level_attributes
import com_ws as cw
import pp_get_mo_attrs as pgma


def xstr(s):
    return '' if s is None else str(s).strip()


# 判断是否是工单时触发
def check_mo_mark_code(ht_pn):
    con = conn.OracleConn()
    sql = f"SELECT * FROM TBL_MARKINGCODE_REP WHERE HT_PN = '{ht_pn}' AND CUST_PN = '工单创建时产生' "
    results = con.query(sql)
    if results:
        return True
    else:
        return False


# 工单打标码设置
def set_marking_code(upload_id="", lot_id="", wafer_id="", wafer_sn='', mo_id=''):
    con = conn.HanaConn()
    con_or = conn.OracleConn()
    con_dw = conn.HanaConnDW()

    sql = f'''SELECT PO_ID,CUST_CODE,CUSTOMER_DEVICE,FAB_DEVICE,HT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,WAFER_SN,PO_H,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,
        ADD_6,ADD_7,to_char(NOW(),'YYYY-MM-DD-WW-d-q'),PRODUCT_PN,ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18
        FROM ZM_CDM_PO_ITEM  WHERE 1= 1  
    '''
    sql = sql + \
        f" AND UPLOAD_ID = '{upload_id}'  " if upload_id else sql

    sql = sql + \
        f" AND LOT_ID = '{lot_id}' " if lot_id else sql

    sql = sql + \
        f" AND LOT_WAFER_ID = '{wafer_id}' " if wafer_id else sql

    sql = sql + \
        f" AND WAFER_SN = '{wafer_sn}' " if wafer_sn else sql

    sql = sql + ' ORDER BY WAFER_SN '

    results = con.query(sql)

    if not results:
        return False

    for row in results:
        item = {}
        item['PO_ID'] = xstr(row[0])
        item['CUST_CODE'] = xstr(row[1])
        item['CUSTOMER_DEVICE'] = xstr(row[2])
        item['FAB_DEVICE'] = xstr(row[3])
        item['HT_PN'] = xstr(row[4])
        item['LOT_ID'] = xstr(row[5])
        item['WAFER_ID'] = xstr(row[6])
        item['LOT_WAFER_ID'] = xstr(row[7])
        item['WAFER_SN'] = xstr(row[8])
        item['PO_H'] = xstr(row[9])
        item['PO_G'] = xstr(row[10])
        item['PO_O'] = xstr(row[11])
        item['PO_P'] = xstr(row[12])
        item['PO_Q'] = xstr(row[13])
        item['PO_R'] = xstr(row[14])
        item['PO_S'] = xstr(row[15])
        item['PO_T'] = xstr(row[16])
        item['PO_U'] = xstr(row[17])
        item['WAFER_DATE'] = xstr(row[18])
        item['PO_V'] = xstr(row[20])
        item['PO_W'] = xstr(row[21])
        item['PO_X'] = xstr(row[22])
        item['PO_Y'] = xstr(row[23])
        item['PO_Z'] = xstr(row[24])
        item['PO_AA'] = xstr(row[25])
        item['PO_AB'] = xstr(row[26])
        item['PO_AC'] = xstr(row[27])
        item['PO_AD'] = xstr(row[28])
        item['PO_AE'] = xstr(row[29])
        item['PO_AF'] = xstr(row[30])
        item['WAFER_YEAR'] = item['WAFER_DATE'].split('-')[0]
        item['WAFER_MON'] = item['WAFER_DATE'].split('-')[1]
        item['WAFER_DAY'] = item['WAFER_DATE'].split('-')[2]

        item['WAFER_WEEK'] = item['WAFER_DATE'].split('-')[3]
        if item['CUST_CODE'] in ('US010'):
            item['WAFER_WEEK'] = str(
                int(get_dc(item['WAFER_DATE'][:10], 6)) - 1)[-2:]
            print("AA特殊打标码周期")

        mo_header = {}
        mo_header['PRODUCT_PN'] = xstr(row[19])
        process = con_dw.query(
            f"select ZZPROCESS from VM_SAP_MAT_INFO where ZZCNLH ='{mo_header['PRODUCT_PN']}'  ")
        mo_header['ZZPROCESS'] = xstr(process[0][0]) if process else ""
        mo_header['CUST_CODE'] = item['CUST_CODE']
        mo_header['MO_ID'] = mo_id
        mo_header['MO_WAFER_ID'] = item['LOT_WAFER_ID']

        mo_date = con.query(
            "SELECT to_char(now(),'YYYY-MM-DD') FROM dummy ")[0][0]
        mo_dc = pgma.get_mo_date_code(
            conn=con, conn_or=con_or, mo_header=mo_header, mo_date=mo_date)
        if len(mo_dc) == 4:
            item['WAFER_WEEK'] = mo_dc[-2:]
            item['WAFER_YEAR'] = item['WAFER_YEAR'][:2] + mo_dc[:2]

        item['WAFER_WEEKDAY'] = item['WAFER_DATE'].split('-')[4]
        item['WAFER_QUAR'] = item['WAFER_DATE'].split('-')[5]
        item['REMARK_1'] = ''
        if item['HT_PN'] == 'X37A181FT':
            item['REMARK_1'] = get_37_FC_DC(item['LOT_WAFER_ID'])

        # 获取打标码
        old_mark_code = get_mark_code_old(con_or, item)
        if old_mark_code:
            item['MARK_CODE'] = old_mark_code

        new_mark_code = get_mark_code_new(item)
        if new_mark_code and new_mark_code != old_mark_code:
            item['MARK_CODE'] = new_mark_code

        # 更新打标码
        err_msg = update_mark_code(con, item)
        if err_msg:
            return err_msg

    return ""


# 获取37 FC的开工单周期
def get_37_FC_DC(lot_wafer_id):
    lot_wafer_id = lot_wafer_id.replace("+", "")
    fc_dc = ""
    con_ha = conn.HanaConn()
    con_or = conn.OracleConn()

    sql = f"""SELECT DISTINCT TO_CHAR(a.CREATE_DATE,'YYYY-MM-DD') FROM ZM_CDM_MO_HEADER a
        INNER JOIN ZM_CDM_MO_ITEM b
        ON a.MO_ID = b.MO_ID 
        WHERE replace(b.LOT_WAFER_ID,'+','') = '{lot_wafer_id}' AND a.FLAG ='1' AND a.PRODUCT_PN = '18X370181000FC'
    """
    results = con_ha.query(sql)
    if len(results) == 1:
        mo_date = xstr(results[0][0])
        fc_dc = get_dc(mo_date, 7)
    else:
        # 查询老系统
        sql = f"""   SELECT  TO_CHAR(b.ERPCREATEDATE,'YYYY-MM-DD') FROM IB_WAFERLIST a
                INNER JOIN IB_WOHISTORY B 
                ON a.ORDERNAME = b.ORDERNAME 
                WHERE replace(a.WAFERID,'+','') = '{lot_wafer_id}'
                AND b.PRODUCT = '18X370181000FC'
        """
        results = con_or.query(sql)
        if len(results) == 1:
            mo_date = xstr(results[0][0])
            fc_dc = get_dc(mo_date, 7)

    return fc_dc


def get_dc(DA, STRAT_DA):
    con = conn.OracleConn()
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


# 修改订单
def set_marking_code_po(ht_pn, wafer_sn):
    con = conn.HanaConn()
    con_or = conn.OracleConn()

    if not check_mo_mark_code(ht_pn):
        sql = f'''SELECT PO_ID,CUST_CODE,CUSTOMER_DEVICE,FAB_DEVICE,HT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,WAFER_SN,PO_H,ADD_0,ADD_1,ADD_2,ADD_3,ADD_4,ADD_5,
            ADD_6,ADD_7,to_char(CREATE_DATE,'YYYY-MM-DD-WW-d-q'),ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18 
            FROM ZM_CDM_PO_ITEM  WHERE WAFER_SN = '{wafer_sn}' '''
    else:
        sql = f'''SELECT c.PO_ID,c.CUST_CODE,c.CUSTOMER_DEVICE,c.FAB_DEVICE,c.HT_PN,c.LOT_ID,c.WAFER_ID,c.LOT_WAFER_ID,c.WAFER_SN,c.PO_H,c.ADD_0,c.ADD_1,c.ADD_2,c.ADD_3,c.ADD_4,c.ADD_5,
            c.ADD_6,c.ADD_7,to_char(a.CREATE_DATE,'YYYY-MM-DD-WW-d-q'),ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18 
            FROM ZM_CDM_MO_HEADER a
            INNER JOIN ZM_CDM_MO_ITEM b ON a.MO_ID = b.MO_ID 
            INNER JOIN ZM_CDM_PO_ITEM c ON c.WAFER_SN = b.WAFER_SN 
            WHERE a.FLAG = '1' AND b.FLAG ='1' AND c.FLAG = '1' and c.WAFER_SN = '{wafer_sn}'
         '''

    results = con.query(sql)
    if not results:
        return False

    for row in results:
        item = {}
        item['PO_ID'] = xstr(row[0])
        item['CUST_CODE'] = xstr(row[1])
        item['CUSTOMER_DEVICE'] = xstr(row[2])
        item['FAB_DEVICE'] = xstr(row[3])
        item['HT_PN'] = xstr(row[4])
        item['LOT_ID'] = xstr(row[5])
        item['WAFER_ID'] = xstr(row[6])
        item['LOT_WAFER_ID'] = xstr(row[7])
        item['WAFER_SN'] = xstr(row[8])
        item['PO_H'] = xstr(row[9])
        item['PO_G'] = xstr(row[10])
        item['PO_O'] = xstr(row[11])
        item['PO_P'] = xstr(row[12])
        item['PO_Q'] = xstr(row[13])
        item['PO_R'] = xstr(row[14])
        item['PO_S'] = xstr(row[15])
        item['PO_T'] = xstr(row[16])
        item['PO_U'] = xstr(row[17])
        item['WAFER_DATE'] = xstr(row[18])

        item['WAFER_YEAR'] = item['WAFER_DATE'].split('-')[0]
        item['WAFER_MON'] = item['WAFER_DATE'].split('-')[1]
        item['WAFER_DAY'] = item['WAFER_DATE'].split('-')[2]
        item['WAFER_WEEK'] = item['WAFER_DATE'].split('-')[3]
        item['WAFER_WEEKDAY'] = item['WAFER_DATE'].split('-')[4]
        item['WAFER_QUAR'] = item['WAFER_DATE'].split('-')[5]
        item['REMARK_1'] = ''

        item['PO_V'] = xstr(row[19])
        item['PO_W'] = xstr(row[20])
        item['PO_X'] = xstr(row[21])
        item['PO_Y'] = xstr(row[22])
        item['PO_Z'] = xstr(row[23])
        item['PO_AA'] = xstr(row[24])
        item['PO_AB'] = xstr(row[25])
        item['PO_AC'] = xstr(row[26])
        item['PO_AD'] = xstr(row[27])
        item['PO_AE'] = xstr(row[28])
        item['PO_AF'] = xstr(row[29])


        # 获取打标码
        old_mark_code = get_mark_code_old(con_or, item)
        if old_mark_code:
            item['MARK_CODE'] = old_mark_code

        new_mark_code = get_mark_code_new(item)
        if new_mark_code and new_mark_code != old_mark_code:
            item['MARK_CODE'] = new_mark_code

        # 更新打标码
        err_msg = update_mark_code(con, item)
        if err_msg:
            return err_msg

        return ""


# 修改工单
def set_marking_code_mo(ht_pn='', mo_id=''):
    con = conn.HanaConn()
    con_or = conn.OracleConn()

    if check_mo_mark_code(ht_pn):
        sql = f''' SELECT c.PO_ID,c.CUST_CODE,c.CUSTOMER_DEVICE,c.FAB_DEVICE,c.HT_PN,c.LOT_ID,c.WAFER_ID,c.LOT_WAFER_ID,c.WAFER_SN,c.PO_H,c.ADD_0,c.ADD_1,c.ADD_2,c.ADD_3,c.ADD_4,c.ADD_5,
        c.ADD_6,c.ADD_7,to_char(a.CREATE_DATE,'YYYY-MM-DD-WW-d-q'),ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18
        FROM ZM_CDM_MO_HEADER a
        INNER JOIN ZM_CDM_MO_ITEM b ON a.MO_ID = b.MO_ID 
        INNER JOIN ZM_CDM_PO_ITEM c ON c.WAFER_SN = b.WAFER_SN 
        WHERE a.FLAG = '1' AND b.FLAG ='1' AND c.FLAG = '1'
        '''
        sql = sql + \
            f" AND a.MO_ID = '{mo_id}' " if mo_id else sql

    else:
        sql = f''' SELECT c.PO_ID,c.CUST_CODE,c.CUSTOMER_DEVICE,c.FAB_DEVICE,c.HT_PN,c.LOT_ID,c.WAFER_ID,c.LOT_WAFER_ID,c.WAFER_SN,c.PO_H,c.ADD_0,c.ADD_1,c.ADD_2,c.ADD_3,c.ADD_4,c.ADD_5,
        c.ADD_6,c.ADD_7,to_char(c.CREATE_DATE,'YYYY-MM-DD-WW-d-q'),ADD_8,ADD_9,ADD_10,ADD_11,ADD_12,ADD_13,ADD_14,ADD_15,ADD_16,ADD_17,ADD_18
        FROM ZM_CDM_MO_HEADER a
        INNER JOIN ZM_CDM_MO_ITEM b ON a.MO_ID = b.MO_ID 
        INNER JOIN ZM_CDM_PO_ITEM c ON c.WAFER_SN = b.WAFER_SN 
        WHERE a.FLAG = '1' AND b.FLAG ='1' AND c.FLAG = '1'
        '''
        sql = sql + \
            f" AND a.MO_ID = '{mo_id}' " if mo_id else sql

    results = con.query(sql)

    if not results:
        return False

    for row in results:
        item = {}
        item['PO_ID'] = xstr(row[0])
        item['CUST_CODE'] = xstr(row[1])
        item['CUSTOMER_DEVICE'] = xstr(row[2])
        item['FAB_DEVICE'] = xstr(row[3])
        item['HT_PN'] = xstr(row[4])
        item['LOT_ID'] = xstr(row[5])
        item['WAFER_ID'] = xstr(row[6])
        item['LOT_WAFER_ID'] = xstr(row[7])
        item['WAFER_SN'] = xstr(row[8])
        item['PO_H'] = xstr(row[9])
        item['PO_G'] = xstr(row[10])
        item['PO_O'] = xstr(row[11])
        item['PO_P'] = xstr(row[12])
        item['PO_Q'] = xstr(row[13])
        item['PO_R'] = xstr(row[14])
        item['PO_S'] = xstr(row[15])
        item['PO_T'] = xstr(row[16])
        item['PO_U'] = xstr(row[17])
        item['WAFER_DATE'] = xstr(row[18])
        item['WAFER_YEAR'] = item['WAFER_DATE'].split('-')[0]
        item['WAFER_MON'] = item['WAFER_DATE'].split('-')[1]
        item['WAFER_DAY'] = item['WAFER_DATE'].split('-')[2]
        item['WAFER_WEEK'] = item['WAFER_DATE'].split('-')[3]
        item['WAFER_WEEKDAY'] = item['WAFER_DATE'].split('-')[4]
        item['WAFER_QUAR'] = item['WAFER_DATE'].split('-')[5]
        item['REMARK_1'] = ''

        item['PO_V'] = xstr(row[19])
        item['PO_W'] = xstr(row[20])
        item['PO_X'] = xstr(row[21])
        item['PO_Y'] = xstr(row[22])
        item['PO_Z'] = xstr(row[23])
        item['PO_AA'] = xstr(row[24])
        item['PO_AB'] = xstr(row[25])
        item['PO_AC'] = xstr(row[26])
        item['PO_AD'] = xstr(row[27])
        item['PO_AE'] = xstr(row[28])
        item['PO_AF'] = xstr(row[29])

        # 获取打标码
        old_mark_code = get_mark_code_old(con_or, item)
        if old_mark_code:
            item['MARK_CODE'] = old_mark_code

        new_mark_code = get_mark_code_new(item)
        if new_mark_code and new_mark_code != old_mark_code:
            item['MARK_CODE'] = new_mark_code

        # 更新工单打标码
        update_mark_code_mes(con, item)

    # 更新工单属性
    update_mo_mes_mark_code_attr(mo_id)


# 更新mes工单属性
def update_mo_mes_mark_code_attr(mo_id):
    con = conn.HanaConn()
    con_or = conn.OracleConn()

    res = {'ERR_MSG': ''}
    sql = f"SELECT CAST(BINTOSTR(cast(a.REQUEST_JSON as binary)) as varchar)  FROM ZM_CDM_MO_HEADER a WHERE MO_ID = '{mo_id}' "
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到工单的原始请求数据"
        return res

    mo_request_json = xstr(results[0][0])
    mo_request = json.loads(mo_request_json)

    # 工单表头属性获取
    mo_request['HEADER']['ACTION_ID'] = 'C'

    header_attrs = get_mo_header_level_attributes(con, con_or, mo_id, '')
    mo_header_attrs = []
    for key, value in header_attrs.items():
        attr = {}
        attr['NAME'] = key
        attr['VALUE'] = value
        mo_header_attrs.append(attr)

    mo_request['HEADER']['EXTRA_PROPERTY'] = mo_header_attrs

    # 工单wafer属性循环获取
    for item in mo_request['ITEM']:
        wafer_sn = item.get('WAFER_SN')
        if not wafer_sn:
            res['ERR_MSG'] = "查询不到工单的WAFER SN信息"
            return res

        wafer_attrs = get_mo_wafer_level_attributes(con, wafer_sn)
        if isinstance(wafer_attrs, str):
            res['ERR_MSG'] = wafer_attrs
            return res

        mo_wafer_attrs = []
        for key, value in wafer_attrs.items():
            attr = {}
            attr['NAME'] = key
            attr['VALUE'] = value
            mo_wafer_attrs.append(attr)

        item['WAFER_PROPERTY'] = mo_wafer_attrs

        item['MARK_CODE'] = wafer_attrs.get('MARKING_CODE', '')

    # 保存最新的属性
    save_mo_request_message(con, mo_id, mo_request)

    mo_refresh = {"MO_DATA": []}
    mo_refresh['MO_DATA'].append(mo_request)
    send_mo_refresh_request(mo_refresh, res)


# 发送创建工单请求
def send_mo_refresh_request(mo_refresh, ret_dict):
    action = cw.WS().send(mo_refresh, 'PP009')
    if not action['status']:
        return action['desc']
    output = action['data']

    return_node = output.get('RETURN')
    print(output)
    if not return_node:
        ret_dict['ERR_MSG'] = f'SAP接口返回字段错误:{output}'
        return ret_dict

    # 返回结果
    if isinstance(return_node, list):
        for item in return_node:
            if not isinstance(item, dict):
                ret_dict['ERR_MSG'] = f'RETURN节点错误:{output}'
                return ret_dict

            if item.get('TYPE') != 'S':
                ret_dict['ERR_MSG'] = '工单刷新失败'
                return ret_dict

    else:
        item = return_node
        if not isinstance(item, dict):
            ret_dict['ERR_MSG'] = f'RETURN节点错误:{output}'
            return ret_dict

        if item.get('TYPE') != 'S':
            ret_dict['ERR_MSG'] = '工单创建失败'
            return ret_dict

    return ret_dict


# 保存请求报文
def save_mo_request_message(con, mo_id, req_data):
    req_json = json.dumps(req_data, sort_keys=True, indent=2)
    if req_data.get('HEADER', {}).get('ACTION_ID') == 'N':
        sql = f"UPDATE ZM_CDM_MO_HEADER SET REQUEST_JSON = '{req_json}' WHERE MO_ID = '{mo_id}' "
    else:
        sql = f"UPDATE ZM_CDM_MO_HEADER SET REQUEST_JSON = '{req_json}',UPDATE_DATE=NOW()  WHERE MO_ID = '{mo_id}' "

    con.exec_n(sql)


# 从oracle获取
def get_mark_code_old(con_or, item):
    sql = f'''select GET_MARK_CODE_OLD('{item['CUST_CODE']}', '{item['CUSTOMER_DEVICE']}', '{item['FAB_DEVICE']}', '{item['HT_PN']}', '{item['LOT_ID']}', '{item['WAFER_ID']}',
    '{item['PO_H']}','{item['PO_G']}', '{item['PO_O']}', '{item['PO_P']}', '{item['PO_Q']}','{item['PO_R']}', '{item['PO_S']}', '{item['PO_T']}', '{item['PO_U']}',
    '{item['WAFER_YEAR']}','{item['WAFER_MON']}','{item['WAFER_DAY']}','{item['WAFER_WEEK']}','{item['WAFER_WEEKDAY']}','{item['WAFER_QUAR']}','{item['REMARK_1']}')  FROM DUAL '''

    results = con_or.query(sql)
    res = xstr(results[0][0])
    return res


# 从新接口获取
def get_mark_code_new(item):
    item_node = [item]
    req_data = json.dumps(item_node).replace('\\\\', '\\')
    print("DEBUG:", req_data)
    url = "http://10.160.1.128:9005/makingCode/map/getMarkingCode"
    headers = {"Content-Type": "application/json"}

    try:
        res_data = requests.post(
            url, data=req_data, headers=headers, timeout=(1, 5)).text
        res_dict = json.loads(res_data)

    except requests.exceptions.RequestException as e:
        print("打标码获取接口异常", e)
        return ''

    if isinstance(res_dict, list):
        if 'MARK_CODE' in res_dict[0]:
            mark_code = res_dict[0]['MARK_CODE'].replace('\\', '\\\\')
            return mark_code

    return ''


# 更新打标码
def update_mark_code(con, item):
    if not 'MARK_CODE' in item:
        return ""

    mark_dc = item['WAFER_YEAR'][2:4] + item['WAFER_WEEK']

    err_msg = check_wafer_mark_code(
        item['HT_PN'], item['LOT_WAFER_ID'], item['MARK_CODE'])
    if err_msg:
        print(err_msg)
        return err_msg

    sql = f"UPDATE ZM_CDM_PO_ITEM SET MARK_CODE = '{item['MARK_CODE']}',add_23='{mark_dc}',UPDATE_DATE=NOW(),UPDATE_BY='订单打标码更新'  WHERE WAFER_SN = '{item['WAFER_SN']}' "
    if not con.exec_c(sql):
        print("订单表更新打标码失败")
        return "订单表更新打标码失败"
    else:
        print(f"打标码更新成功:{item['LOT_WAFER_ID']}=>{item['MARK_CODE']}")
        return ""


# 更新mes打标码
def update_mark_code_mes(con, item):
    if not 'MARK_CODE' in item:
        return False

    mark_dc = item['WAFER_YEAR'][2:4] + item['WAFER_WEEK']
    err_msg = check_wafer_mark_code(
        item['HT_PN'], item['LOT_WAFER_ID'], item['MARK_CODE'])
    if err_msg:
        print(err_msg)
        return False

    sql = f"UPDATE ZM_CDM_PO_ITEM SET MARK_CODE = '{item['MARK_CODE']}',add_23='{mark_dc}',UPDATE_DATE=NOW(),UPDATE_BY='订单打标码更新' WHERE WAFER_SN = '{item['WAFER_SN']}' "
    if not con.exec_c(sql):
        print("工单表更新打标码失败")
        return False
    else:
        sql = f"UPDATE ZM_CDM_MO_ITEM SET MARK_CODE = '{item['MARK_CODE']}' WHERE WAFER_SN = '{item['WAFER_SN']}' "
        con.exec_c(sql)
        print(f"工单打标码更新成功:{item['LOT_WAFER_ID']}=>{item['MARK_CODE']}")
        return True


# 检查打标码
def check_wafer_mark_code(ht_pn, wafer_id, mark_code):
    err_msg = ''

    con_or = conn.OracleConn()
    print(ht_pn, mark_code)
    sql = f"SELECT REMARK FROM TBL_MARKINGCODE_REP  WHERE HT_PN = '{ht_pn}' "
    results = con_or.query(sql)
    if results:
        mark_rule_code = xstr(results[0][0]).replace('\\\\', '\\')
        mark_code = mark_code.replace('\\\\', '\\')
        print(mark_rule_code, mark_code)

        # 位数检查
        if len(mark_rule_code) != len(mark_code):
            err_msg = f"{wafer_id} 打标位数错误,NPI设定位数:{len(mark_rule_code)} => {mark_rule_code}, 当前位数:{len(mark_code)}=> {mark_code}"
            return err_msg

        # 字符检查
        for i in range(len(mark_rule_code)):
            if mark_rule_code[i] == '*':
                continue

            if mark_rule_code[i] != mark_code[i]:
                err_msg = f"{wafer_id}:第{i+1}位打标字符错误,NPI设定固定字符:{mark_rule_code[i]}  =>{mark_rule_code}, 当前异常字符:{mark_code[i]}=> {mark_code}"
                print(mark_rule_code[i], mark_code[i], err_msg)

                return err_msg

    # 检查通过
    return err_msg


if __name__ == "__main__":
    set_marking_code(lot_id='NPT1J.01')
