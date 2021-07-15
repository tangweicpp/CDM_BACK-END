import re
import os
import pandas as pd
import conn_db as conn
from mm_mat_info import get_customer_master_data
from mm_mat_info import get_mat_master_data
from mm_mat_info import get_mat_master_data_dc
from flask import abort, make_response
import cchardet as chardet
import time
import uuid


def xstr(s):
    return '' if s is None else str(s).strip()


# 解析excel
def parse_gen_xl(po_header):
    # 获取excel数据
    print(po_header)
    items_data = get_items_data(po_header)

    # 获取完整物料信息
    if po_header['cust_code'] == 'US010' and not 'EBR' in po_header['template_desc']:
        po_data = get_item_master_data_aa(po_header, items_data)
    else:
        if po_header['cust_code'] != '通用':
            po_data = get_item_master_data(po_header, items_data)

        else:
            for item in items_data:
                item['product_pn'] = '通用类型'
                item['product_pn_list'] = [item['product_pn']]
                if 'wafer_id_str' in item:
                    item['wafer_id_list'] = get_wafer_id_list(item)

            po_data = items_data

    return po_data


# xlsx=>csv
def get_file_name(po_header):
    file_name = po_header['file_path']
    if os.path.splitext(file_name)[-1].upper() == '.CSV':
        return file_name
    else:
        if po_header['cust_code'] == 'SH115':
            data_xls = pd.read_excel(file_name)
            csv_file_name = os.path.splitext(file_name)[0] + '.csv'
            data_xls.to_csv(csv_file_name, encoding='utf-8')
            return csv_file_name
        else:
            return file_name


# 获取订单数据
def get_items_data(po_header):
    con = conn.HanaConn()
    items_data = []

    file_path = get_file_name(po_header)
    template_sn = po_header['template_sn']

    # 全局配置
    sql = f"SELECT SHEET_INDEX,SHEET_HEADER,SHEET_START_ROW,SHEET_STOP_ROW FROM ZM_CDM_PO_TEMPLATE_HEADER WHERE id = '{template_sn}' "
    results = con.query(sql)
    if not results:
        err_msg = {"ERR_MSG": "查询不到全局配置信息"}
        abort(make_response(err_msg))

    sheet_index = results[0][0]
    sheet_header = results[0][1]
    sheet_start_row = results[0][2]
    sheet_stop_row = results[0][3]

    # 明细配置
    sql = f"""
        SELECT FIELD_NAME,PARSE_SITE_COL_ROW,PARSE_METHOD,IGNORE_CHAR_1,MAX_LEN,FIX_LEN,FIX_STRING,START_CHAR,END_CHAR,SUB_LEN,
        REPLACE_OLD_STR,REPLACE_NEW_STR,REPLACE_TIMES,SUB_LEFT_CHAR,SUB_RIGHT_CHAR,FRONT_FIELD,BEHIND_FIELD,PARSE_SITE,MIN_LEN
        FROM ZM_CDM_PO_TEMPLATE_ITEM WHERE ID = '{template_sn}' AND FLAG = '1' 
    """
    results = con.query(sql)
    if not results:
        err_msg = {"ERR_MSG": "查询不到明细配置明细"}
        abort(make_response(err_msg))

    str_cell_name = {}
    for rs in results:
        cell_name = xstr(rs[1])
        str_cell_name[cell_name] = str

    try:
        if os.path.splitext(file_path)[-1].upper() == '.CSV':
            with open(file_path, "rb") as f:
                f_content = f.read()
                ch_encoding = chardet.detect(f_content)
                f.close()

            df = pd.read_csv(
                file_path, header=None, keep_default_na=False, encoding=ch_encoding.get('encoding', 'utf-8'), converters=str_cell_name)
        else:
            df = pd.read_excel(
                file_path, header=None, keep_default_na=False, sheet_name=sheet_index, converters=str_cell_name)

        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        if os.path.splitext(file_path)[-1].upper() == '.CSV':
            df = pd.read_csv(
                file_path, header=None, keep_default_na=False, encoding='gb2312', converters=str_cell_name)

        else:
            err_msg = {"ERR_MSG": f"文件读取失败:{e}"}
            abort(make_response(err_msg))

    # 最大行
    max_rows = len(df)

    # 循环获取数据
    upload_id = con.query(
        "select ZM_CDM_REP_LIST_SEQ.NEXTVAL from dummy")[0][0]
    for i in range(max_rows-sheet_header-1):
        item_data = {'valid': True}

        for rs in results:
            item_name = rs[0]
            item_type = rs[2]
            item_row = int(rs[1].split(':')[0])-1
            item_col = int(rs[1].split(':')[1])-1

            if item_type == '2':
                item_data[item_name] = refresh_data(rs, df[item_col][item_row])

            elif item_type == '1':
                # 定义列名
                r_parse_col_name = xstr(rs[17])
                # 当前列名
                c_parse_col_name = df[item_col][sheet_header]
                # print(item_name,  "-列名:", r_parse_col_name, c_parse_col_name)

                if c_parse_col_name != r_parse_col_name:
                    abort(make_response(
                        {"ERR_MSG": f"{item_name}:配置的列名{r_parse_col_name}和当前解析到的{c_parse_col_name}不一致,请确认客户订单是否错误"}))
                item_data[item_name] = refresh_data(
                    rs, df[item_col][item_row+i+1])

            if not filter_data(item_name, item_col, rs, item_data[item_name]):
                item_data['valid'] = False

        if po_header['template_desc'] == 'DC订单' and not item_data.get('lot_id'):
            item_data['lot_id'] = get_random_DC_LOT()
        if po_header['cust_code'] == 'AC03' and 'customer_device' in item_data:
            item_data['customer_device'] = item_data.get(
                'customer_device', '') + '-' + item_data.get('add_7', '')

        if (po_header['cust_code'] in ('AA', 'US010', '通用')) or (po_header['template_desc'] == 'FO订单' and item_data['customer_device'] and item_data['lot_id']) or (item_data['valid'] and item_data.get('po_id') and item_data.get('lot_id') and (item_data.get('wafer_id_str') or item_data.get('wafer_qty')) and (item_data.get('customer_device') or item_data.get('fab_device'))):
            if po_header['cust_code'] == 'DA69':
                item_data['customer_device'] = item_data['fab_device'] + \
                    "$$" + item_data['customer_device'] + \
                    "$$" + item_data['code']

                item_data['code'] = ''

            # 除片号不同其他信息一致的,归集到一起
            if po_header['cust_code'] in ('BJ49'):
                cur_str = ""
                for key in item_data.keys():
                    if key != "wafer_id_str":
                        cur_str = cur_str + str(item_data[key]) + " "

                sql = f"SELECT ITEM_NO FROM ZM_CDM_REP_LIST WHERE UPLOAD_ID = '{upload_id}' AND ITEM_DATA = '{cur_str}' "
                results2 = con.query(sql)
                if results2:
                    item_no = results2[0][0]
                    items_data[item_no]['wafer_id_str'] = items_data[item_no]['wafer_id_str'] + \
                        "," + item_data['wafer_id_str']
                else:
                    # 新增
                    item_no = len(items_data)
                    sql = f"INSERT INTO ZM_CDM_REP_LIST(UPLOAD_ID,ITEM_DATA,ITEM_NO) VALUES({upload_id},'{cur_str}',{item_no})"
                    con.exec_c(sql)
                    items_data.append(item_data)
            else:
                items_data.append(item_data)

    if not items_data:
        err_msg = {"ERR_MSG": "没有解析到有效的订单文件"}
        abort(make_response(err_msg))
    # print(items_data)
    return items_data


def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


# 获取随机DCLOT
def get_random_DC_LOT():
    lot_part = "D" + time.strftime('%y%m%d') + get_rand_id(4).upper()
    print(lot_part)
    return lot_part


# 数据修饰
def refresh_data(rs, value):
    value = xstr(value)

    # 左部开始位置
    left_pos = xstr(rs[15])
    if left_pos:
        value = value[int(left_pos):]
        return value

    # 右部结束位置
    right_pos = xstr(rs[16])
    if right_pos:
        value = value[:-int(right_pos)]
        return value

    # 替换字符,支持多组
    replace_old_str = xstr(rs[10])
    replace_new_str = xstr(rs[11])

    if ">>" in replace_old_str and ">>" in replace_new_str:
        # 多组替换
        old_str_spi = replace_old_str.split(">>")
        new_str_spi = replace_new_str.split(">>")

        if len(old_str_spi) == len(new_str_spi):
            for i in range(len(old_str_spi)):
                value = value.replace(old_str_spi[i], new_str_spi[i])

    else:
        # 单组替换
        if xstr(rs[12]):
            replace_times = int(xstr(rs[12]))
        else:
            replace_times = None

        if replace_old_str and replace_new_str:
            value = value.replace(replace_old_str, replace_new_str, replace_times) if replace_times else value.replace(
                replace_old_str, replace_new_str)

    # 忽略字符
    ignore_ch = xstr(rs[3])
    if ignore_ch:
        value = xstr(value.replace(ignore_ch, '', 1))

    # 起始字符+截取长度
    begin_ch = xstr(rs[7])
    sub_len = xstr(rs[9])
    if begin_ch:
        begin_pos = value.find(begin_ch)
        if begin_pos != -1:
            if sub_len:
                value = value[begin_pos:(begin_pos+int(sub_len))]
            else:
                value = value[begin_pos:]
        elif begin_ch in ("PPR-", "NCMR-"):
            value = ""

    # 结束字符+截取长度
    end_ch = xstr(rs[8])
    sub_len = xstr(rs[9])
    if end_ch:
        end_ch = value.find(end_ch)
        # end_ch = value.rfind(end_ch) 最后出现,回退
        if end_ch != -1:
            value = value[:end_ch]
            if sub_len:
                value = value[(end_ch-int(sub_len)):end_ch]
            else:
                value = value[:end_ch]

    # 从左->右 : 范围截取
    left_ch = xstr(rs[13])
    right_ch = xstr(rs[14])
    if left_ch and right_ch:
        value = value[value.find(left_ch)+1:value.rfind(right_ch)]

    value = xstr(value)
    return value


# 数据过滤
def filter_data(key, cell_name, rs, value):
    # 最小字符限制
    min_len = xstr(rs[18])
    if min_len:
        if len(value) <= int(min_len):
            err_desc = f"{key}=>订单字段:{cell_name}:长度{len(value)}小于限制长度{int(min_len)}!!!"
            print(err_desc)
            return False

    # 最大字符限制
    max_len = xstr(rs[4])
    if max_len:
        if len(value) > int(max_len):
            err_desc = f"{key}=>订单字段:{cell_name}:长度{len(value)}大于限制长度{int(max_len)}!!!"
            print(err_desc)
            return False

    # 固定长度
    fix_len = xstr(rs[5])
    if fix_len:
        if len(value) != int(fix_len):
            err_desc = f"{key}=>订单字段:{cell_name}:长度{len(value)}不等于固定长度{int(fix_len)}!!!"
            print(err_desc)
            return False

    # 固定字符
    fix_string = xstr(rs[6])
    if fix_string:
        if value != fix_string and value:
            err_msg = {
                "ERR_MSG": f"订单字段{key}=>:{cell_name}:值{value}不等于固定值{fix_string}!!!请确认订单内容是否正确或选错上传模板"}
            abort(make_response(err_msg))

    return True


# 获取多对一模板对应真实客户代码
def get_exact_cust_code(po_header, po_item):
    con = conn.HanaConn()
    if po_item.get('cust_code'):
        sql = f"SELECT VALUE FROM ZM_CDM_KEY_LOOK_UP WHERE REMAKR = '{po_header['cust_code']}' AND INSTR('{po_item['cust_code']}', KEY) > 0 "
        print(sql)
        results = con.query(sql)
        if results:
            po_header['r_cust_code'] = po_header['cust_code']
            po_item['s_cust_code'] = xstr(results[0][0])
            po_header['cust_code'] = xstr(results[0][0])

    # SAP客户代码
    cust_master_data = get_customer_master_data(po_header['cust_code'])
    po_header['sap_cust_code'] = cust_master_data.get('sap_cust_code', '')


# 获取37的FAB机种
def get_37_fab_device(parent_job, cur_fab_device):
    sql = f"SELECT DISTINCT FAB_DEVICE FROM ZM_CDM_PO_ITEM WHERE add_4 = '{parent_job}' "
    results = conn.HanaConn().query(sql)
    if results:
        cur_fab_device = xstr(results[0][0])
    return cur_fab_device


# 获取准确值
def get_exact_value(po_header, item):
    # 打标码
    if item.get('mark_code'):
        item['mark_code'] = r'\\'.join(item['mark_code'].replace('\\\\', ' ').replace(
            '/', ' ').replace('//', ' ').replace('\\', ' ').split())

    # wafer数量
    if item.get('wafer_qty'):
        item['wafer_qty'] = item['wafer_qty'].replace(',', '')

    # 37FAB机种
    if po_header['cust_code'] == 'US337':
        if item.get('fab_device'):
            item['fab_device'] = get_37_fab_device(
                item.get('add_30'), item['fab_device'])

    # SH07
    if po_header.get('r_cust_code') == 'SH07' and item.get('add_3'):
        item['add_3'] = item.get('add_3', '').split()[0]

    # JX002打标码
    if po_header['cust_code'] == 'JX002':
        print("测试")
        base_code = item.get('add_4', '')
        if base_code:
            base_code = base_code.replace("Line1:", "").replace(
                "Line2:", "").replace("Line3:", "")

            item['add_4'] = base_code.split()[0]
            item['add_5'] = base_code.split()[1]
            item['add_6'] = base_code.split()[2]


# 主数据必要信息
def get_item_master_data(po_header, items_data):
    # 客户主数据
    get_exact_cust_code(po_header, items_data[0])
    
    # 物料主数据
    mat_data_list = {}
    for item in items_data:
        get_exact_value(po_header, item)

        # 机种信息
        customer_device = item.get('customer_device', '')
        fab_device = item.get('fab_device', '')
        process = item.get('process', '')
        code = item.get('code', '')
        po_process = po_header.get('process', '')
        if po_process:
            process = po_process

        # 订单上带的机种名
        item['po_customer_device'] = customer_device
        item['po_fab_device'] = fab_device

        # 获取物料主数据
        base_name = customer_device+fab_device+process+code
        if not base_name in mat_data_list:
            if not po_header['template_desc'] == 'DC订单':
                mat_data = get_mat_master_data(
                    customer_device=customer_device, fab_device=fab_device, process=process, code=code)
            else:
                mat_data = get_mat_master_data_dc(
                    customer_device=customer_device, fab_device=fab_device, process=process, code=code)

            mat_data_list[base_name] = mat_data
        else:
            mat_data = mat_data_list[base_name]

        # 物料主数据
        # item['mat_sql'] = mat_data[0]['SQL']
        if len(mat_data) > 1:
            item['warn_desc'] = '当前订单信息:' + \
                ('客户机种:' + customer_device + ' ' if customer_device else '') + \
                ('FAB机种:' + fab_device + ' ' if fab_device else '') + \
                ('Process:' + process + ' ' if process else '') + \
                ('CODE:' + code+' ' if code else '') + \
                (',无法关联唯一料号,请手动选择')

            item['product_pn_list'] = []
            for rs in mat_data:
                product_obj = {}
                product_obj['value'] = rs['MATNR']
                product_obj['label'] = rs['ZZCNLH']
                product_obj['khzy1'] = rs['ZZLKHZY1']
                product_obj['khzy2'] = rs['ZZLKHZY2']
                product_obj['khzy3'] = rs['ZZLKHZY3']
                product_obj['khzy4'] = rs['ZZLKHZY4']
                product_obj['khzy5'] = rs['ZZLKHZY5']
                product_obj['mat_hold'] = rs['ZZKHDM']
                product_obj['base_so'] = rs['ZZBASESOMO']
                product_obj['customer_device'] = rs['ZZKHXH']
                product_obj['fab_device'] = rs['ZZFABXH']
                product_obj['child_pn'] = rs['CHILDPN']
                product_obj['ht_pn'] = rs['ZZHTXH']
                product_obj['wafer_dies'] = rs['ZZJYGD']
                product_obj['lcbz'] = rs['ZZLCBZ']

                item['product_pn_list'].append(product_obj)

            item['customer_device'] = customer_device if customer_device else ''
            item['fab_device'] = fab_device if fab_device else ''
            item['ht_pn'] = ''
            item['product_pn'] = ''
            item['wafer_pn'] = ''

            wafer_good_dies = float(item.get('passbin_count', 0))
            wafer_ng_dies = float(item.get('failbin_count', 0))
            # 晶圆设计DIES
            item['wafer_dies'] = 0
            item['dies_from_po'] = True if wafer_good_dies else False
            # 实际投单DIES
            item['passbin_count'] = wafer_good_dies
            item['failbin_count'] = wafer_ng_dies

            item['sap_cust_code'] = ''
            item['sap_product_pn'] = ''
            item['trad_cust_code'] = ''
            item['wafer_id_list'] = get_wafer_id_list(item)
            if item.get('wafer_qty'):
                if int(item.get('wafer_qty')) != len(item['wafer_id_list']):
                    abort(make_response(
                        {"ERR_MSG": f"{item['lot_id']}订单行的片数量与片号对应不上"}))

            item['wafer_qty'] = len(item['wafer_id_list'])
            if item['wafer_qty'] > 25:
                abort(make_response(
                    {"ERR_MSG": f"{item['lot_id']}一个lot不可大于25片"}))
            item['wafer_list'] = []
            for wafer_id in item['wafer_id_list']:
                wafer = {}
                wafer['lot_id'] = item.get('lot_id', '')
                wafer['wafer_id'] = wafer_id
                wafer['lot_wafer_id'] = wafer['lot_id'] + wafer_id
                wafer['hold_flag'] = False
                wafer['real_wafer_id'] = 'N' if item.get(
                    'real_wafer_id', '') == 'N' else 'Y'

                item['wafer_list'].append(wafer)

        else:
            item['customer_device'] = customer_device if customer_device else mat_data[0]['ZZKHXH']
            item['fab_device'] = fab_device if fab_device else mat_data[0]['ZZFABXH']
            item['ht_pn'] = mat_data[0]['ZZHTXH']
            item['product_pn'] = mat_data[0]['ZZCNLH']
            item['sap_product_pn'] = mat_data[0]['MATNR']
            item['product_pn_list'] = [item['product_pn']]
            item['base_so'] = mat_data[0]['ZZBASESOMO']
            item['khzy1'] = mat_data[0]['ZZLKHZY1']
            item['khzy2'] = mat_data[0]['ZZLKHZY2']
            item['khzy3'] = mat_data[0]['ZZLKHZY3']
            item['khzy4'] = mat_data[0]['ZZLKHZY4']
            item['khzy5'] = mat_data[0]['ZZLKHZY5']
            item['mat_hold'] = mat_data[0]['ZZKHDM']
            item['child_pn'] = mat_data[0]['CHILDPN']
            item['lcbz'] = mat_data[0]['ZZLCBZ']

            # 订单指定类型
            if item.get('lcbz') == "Y":
                item['po_type'] = "量产订单"
            elif item.get('lcbz') == "N":
                item['po_type'] = "样品订单"
            elif item.get('lcbz') == "S":
                item['po_type'] = "小批量订单"
            else:
                item['po_type'] = ""

            wafer_good_dies = float(item.get('passbin_count', 0))
            wafer_ng_dies = float(item.get('failbin_count', 0))
            item['dies_from_po'] = True if wafer_good_dies else False
            item['wafer_dies'] = mat_data[0]['ZZJYGD']  # 晶圆设计DIES
            # 实际投单DIES
            item['passbin_count'] = wafer_good_dies if wafer_good_dies > 0 else item['wafer_dies'] - wafer_ng_dies
            item['failbin_count'] = wafer_ng_dies
            item['wafer_pn'] = ''
            item['wafer_id_list'] = get_wafer_id_list(item)

            if item.get('wafer_qty'):
                if (int(float(item.get('wafer_qty'))) != len(item['wafer_id_list'])) and (int(float(item.get('wafer_qty'))) <= 25):
                    abort(make_response(
                        {"ERR_MSG": f"{item['lot_id']}订单行的片数量与片号对应不上"}))

            item['wafer_qty'] = len(item['wafer_id_list'])
            if item['wafer_qty'] > 25:
                abort(make_response(
                    {"ERR_MSG": f"{item['lot_id']}一个lot不可大于25片"}))

            item['warn_desc'] = 'ok'
            item['wafer_list'] = []
            for wafer_id in item['wafer_id_list']:
                wafer = {}
                wafer['lot_id'] = item.get('lot_id')
                wafer['wafer_id'] = wafer_id
                wafer['lot_wafer_id'] = wafer['lot_id'] + wafer_id
                wafer['hold_flag'] = False
                wafer['real_wafer_id'] = 'N' if item.get(
                    'real_wafer_id', '') == 'N' else 'Y'

                item['wafer_list'].append(wafer)

    return items_data


# AA物料主数据
def get_item_master_data_aa(po_header, items_data):
    aa_sap_cust_code = '200115'
    if not items_data[0].get('po_id'):
        for item in items_data:
            item['product_pn'] = '非PO'
            item['product_pn_list'] = [item['product_pn']]

        return items_data

    for item in items_data:
        # 客户机种

        customer_device = item.get('customer_device', '')
        fab_device = item.get('fab_device', '')
        process = item.get('process', '')
        code = item.get('code', '')

        # 获取物料主数据
        mat_data = get_mat_master_data(
            customer_device=customer_device, fab_device=fab_device, process=process, code=code)

        # item['mat_sql'] = mat_data[0]['SQL']
        if len(mat_data) > 1:
            item['warn_desc'] = '当前订单信息:' + \
                ('客户机种:' + customer_device + ' ' if customer_device else '') + \
                ('FAB机种:' + fab_device + ' ' if fab_device else '') + \
                ('Process:' + process + ' ' if process else '') + \
                ('CODE:' + code+' ' if code else '') + \
                (',无法关联唯一料号,请手动选择')

            item['product_pn_list'] = []
            for rs in mat_data:
                product_obj = {}
                product_obj['value'] = rs['MATNR']
                product_obj['label'] = rs['ZZCNLH']
                product_obj['khzy1'] = rs['ZZLKHZY1']
                product_obj['khzy2'] = rs['ZZLKHZY2']
                product_obj['khzy3'] = rs['ZZLKHZY3']
                product_obj['khzy4'] = rs['ZZLKHZY4']
                product_obj['khzy5'] = rs['ZZLKHZY5']
                product_obj['base_so'] = rs['ZZBASESOMO']
                product_obj['mat_hold'] = rs['ZZKHDM']
                product_obj['customer_device'] = rs['ZZKHXH']
                product_obj['fab_device'] = rs['ZZFABXH']
                product_obj['ht_pn'] = rs['ZZHTXH']
                product_obj['wafer_dies'] = rs['ZZJYGD']
                product_obj['lcbz'] = rs['ZZLCBZ']

                item['product_pn_list'].append(product_obj)

            item['customer_device'] = customer_device if customer_device else ''
            item['fab_device'] = fab_device if fab_device else ''
            item['ht_pn'] = ''
            item['product_pn'] = ''
            item['sap_cust_code'] = aa_sap_cust_code
            item['sap_product_pn'] = ''
            item['trad_cust_code'] = ''

        else:
            item['sap_cust_code'] = aa_sap_cust_code
            item['customer_device'] = customer_device if customer_device else mat_data[0]['ZZKHXH']
            item['fab_device'] = fab_device if fab_device else mat_data[0]['ZZFABXH']
            item['ht_pn'] = mat_data[0]['ZZHTXH']
            item['product_pn'] = mat_data[0]['ZZCNLH']
            item['sap_product_pn'] = mat_data[0]['MATNR']
            item['product_pn_list'] = [item['product_pn']]
            item['base_so'] = mat_data[0]['ZZBASESOMO']
            item['khzy1'] = mat_data[0]['ZZLKHZY1']
            item['khzy2'] = mat_data[0]['ZZLKHZY2']
            item['khzy3'] = mat_data[0]['ZZLKHZY3']
            item['khzy4'] = mat_data[0]['ZZLKHZY4']
            item['khzy5'] = mat_data[0]['ZZLKHZY5']
            item['mat_hold'] = mat_data[0]['ZZKHDM']
            item['lcbz'] = mat_data[0]['ZZLCBZ']

            if item.get('lcbz') == "Y":
                item['po_type'] = "量产订单"
            elif item.get('lcbz') == "N":
                item['po_type'] = "样品订单"
            elif item.get('lcbz') == "S":
                item['po_type'] = "小批量订单"
            else:
                item['po_type'] = ""

    return items_data


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


if __name__ == "__main__":
    parse_gen_xl({'user_name': '07885', 'cust_code': 'US337', 'po_type': 'ZOR3', 'po_date': '20210420', 'bonded_type': 'Y', 'offer_sheet': '', 'need_delay': 'false', 'delay_days': '', 'need_mail_tip': 'false', 'mail_tip': '', 'po_level': 'primary', 'file_name': '267603HTKS_PO_PO_6000082896_00001_20210416110020.xlsx',
                  'template_sn': '26a193', 'template_type': 'LOT|WAFER|DIES|CUSTPN|FABPN|PO', 'template_desc': 'DC订单', 'create_bank_wo': 'false', 'common_checked': 'false', 'err_desc': '', 'file_path': '/opt/CDM_PRD/cdm_1.1_flask/docs/HTKS_PO_PO_6000082896_00001_20210416110020.xlsx'})
