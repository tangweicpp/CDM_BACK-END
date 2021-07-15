import pandas as pd
import conn_db as conn
import json
import re
import uuid
from web_api_client import get_data_from_web_api
from flask import abort, make_response
from sd_parse_gen_xl import get_item_master_data
from mm_mat_info import get_mat_master_data


def xstr(s):
    return '' if s is None else str(s).strip()


# 获取随机数
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


# HK098,BJ178,SZ217, GD224
def parse_HK098_FO(po_header):
    print(po_header)

    po_data = []
    file_name = po_header['file_path']

    # 文件读取

    # 读取sheet1
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    # print(df.to_html())
    # f = open('a.html', 'w')
    # str = df.to_html()
    # f.write(str)
    # f.close()

    # po_id
    if '订单编号' in df[12][5] and xstr(df[13][5]):
        po_id = xstr(df[13][5])
    else:
        err_msg = {"ERR_MSG": '客户PO号码未找到'}
        abort(make_response(err_msg))

    # 客户机种
    min_len = 7
    max_len = len(df)
    csp_qty = 0
    csp_device = ""
    if '芯片名称' in df[2][6]:
        for i in range(min_len, max_len):
            if xstr(df[3][i]):
                lot_id = xstr(df[3][i]).strip()
                wafer_id_str = xstr(df[4][i]).strip()
                # 晶圆机种
                for j in range(i, 6, -1):
                    if xstr(df[2][j]):
                        customer_device = xstr(df[2][j]).strip()
                        break

                # CSP机种
                for j in range(i, 6, -1):
                    if xstr(df[1][j]):
                        csp_device = xstr(df[1][j]).strip()
                        break

                # CSP机种数量
                for j in range(i, 6, -1):
                    if xstr(df[7][j]) and not csp_qty:
                        csp_qty = get_real_po_qty(xstr(df[7][j]).strip())
                        break

                # add_3: CSP成品机种, add_30: CSP行数量
                if po_id and customer_device and lot_id and wafer_id_str and csp_device and csp_qty:
                    po_data.append({"po_id": po_id, "customer_device": customer_device,
                                    "lot_id": lot_id,  "wafer_id_str": wafer_id_str, "add_3": csp_device, "add_30": csp_qty})

                    lot_id = ""
                    wafer_id_str = ""
                    customer_device = ""
                    csp_device = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


def parse_GD224_FO(po_header):
    print(po_header)

    po_data = []
    file_name = po_header['file_path']

    # 文件读取

    # 读取sheet1
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    # print(df.to_html())
    # f = open('a.html', 'w')
    # str = df.to_html()
    # f.write(str)
    # f.close()

    # po_id
    if '订单编号' in df[12][5] and xstr(df[13][5]):
        po_id = xstr(df[13][5])
    else:
        err_msg = {"ERR_MSG": '客户PO号码未找到'}
        abort(make_response(err_msg))

    # 客户机种
    min_len = 7
    max_len = len(df)
    csp_qty = 0
    csp_device = ""
    add_4 = ""
    if '芯片名称' in df[2][6]:
        for i in range(min_len, max_len):
            if xstr(df[3][i]):
                lot_id = xstr(df[3][i]).strip()
                wafer_id_str = xstr(df[4][i]).strip()
                # 晶圆机种
                for j in range(i, 6, -1):
                    if xstr(df[2][j]):
                        customer_device = xstr(df[2][j]).strip()
                        break

                # CSP机种
                for j in range(i, 6, -1):
                    if xstr(df[1][j]):
                        csp_device = xstr(df[1][j]).strip()
                        break

                # CSP机种数量
                for j in range(i, 6, -1):
                    if xstr(df[7][j]) and not csp_qty:
                        csp_qty = get_real_po_qty(xstr(df[7][j]).strip())
                        break

                # GD224标签品名（Part No）
                for j in range(i, 6, -1):
                    if xstr(df[16][j]):
                        add_4 = xstr(df[16][j]).strip()
                        break

                # add_3: CSP成品机种, add_30: CSP行数量
                if po_id and customer_device and lot_id and wafer_id_str and csp_device and csp_qty:
                    po_data.append({"po_id": po_id, "customer_device": customer_device,
                                    "lot_id": lot_id,  "wafer_id_str": wafer_id_str, "add_3": csp_device, "add_30": csp_qty, "add_4": add_4})

                    lot_id = ""
                    wafer_id_str = ""
                    customer_device = ""
                    csp_device = ""
                    add_4 = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# GD224特殊PO Qty
def get_real_po_qty(po_qty_str):
    if not "*" in po_qty_str:
        return po_qty_str
    else:
        num_list = po_qty_str.split('*')
        left_num = int(num_list[0])
        right_num = int(num_list[1])
        res_num = left_num * right_num
        return str(res_num)


# AB31
def parse_AB31_FO(po_header):
    print(po_header)

    po_data = []
    file_name = po_header['file_path']

    # 读取sheet1
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    # 客户机种
    min_len = 1
    max_len = len(df)
    for i in range(min_len, max_len):
        # PO
        if "订单号" in df[2][0]:
            po_id = xstr(df[2][i]).strip()

        # 客户机种
        if "FAB机种名" in df[4][0]:
            customer_device = xstr(df[4][i]).strip()

        # lotID
        if "lot号" in df[6][0]:
            lot_id = xstr(df[6][i]).strip()

        # wafer id
        if "WAFER" in df[7][0]:
            wafer_id_str = xstr(df[7][i]).strip()

        # add_1
        if "特殊需求1" in df[13][0]:
            add_1 = xstr(df[13][i]).replace("第三排印制：", "").strip()

        # CSP device
        if "客户机种名" in df[5][0]:
            csp_device = xstr(df[5][i]).strip()

        # CSP QTY, 没有 预估一个量
        csp_qty = 999999

        # add_3: CSP成品机种, add_30: CSP行数量
        if po_id and customer_device and lot_id and wafer_id_str and csp_device and csp_qty:
            po_data.append({"po_id": po_id, "customer_device": customer_device,
                            "lot_id": lot_id,  "wafer_id_str": wafer_id_str, "add_3": csp_device, "add_30": csp_qty, "add_1": add_1})

            po_id = ""
            lot_id = ""
            wafer_id_str = ""
            customer_device = ""
            csp_device = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# SH104 FO
def parse_SH104_FO(po_header):
    print(po_header)
    po_data = []
    file_name = po_header['file_path']

    # 读取sheet1
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    max_len = len(df)

    # PO
    if "委外单号" in df[0][2] and df[3][2]:
        po_id = xstr(df[3][2]).strip()

    # CSP DEVICE,QTY
    if "回货品名" in df[6][16] and df[6][17]:
        csp_device = xstr(df[6][17])

    if "数量" in df[22][16] and df[22][17]:
        csp_qty = xstr(df[22][17])

    if "加工项目" in df[7][13] and df[9][13]:
        add_2 = xstr(df[9][13])

    if "D/C" in df[16][16] and df[16][17]:
        add_3 = xstr(df[16][17])

    if "测试组合代码" in df[0][14] and df[3][14]:
        add_5 = xstr(df[3][14])

    if "标签品名" in df[8][16] and df[8][17]:
        add_6 = xstr(df[8][17])

    if "标签PKG" in df[10][16] and df[10][17]:
        add_7 = xstr(df[10][17])

    # 获取循环行
    loop_index = 24
    for i in range(24, max_len):
        if '项次' in df[0][i] and '发料品名' in df[6][i]:
            loop_index = i
            break

    customer_device = ""
    lot_id = ""
    wafer_id_str = ""
    add_4 = ""
    for i in range(loop_index, max_len):
        if '发料品名' in df[6][i-1] and df[6][i]:
            customer_device = xstr(df[6][i])

        if '发料批号' in df[10][i-1] and df[10][i]:
            lot_id = xstr(df[10][i])

        if '刻号' in df[13][i-1] and df[13][i]:
            wafer_id_str = xstr(df[13][i])

        if '发料测试' in df[24][i-1] and df[24][i]:
            add_4 = xstr(df[24][i])

        # add_3: CSP成品机种, add_30: CSP行数量
        if po_id and customer_device and lot_id and wafer_id_str and csp_device and csp_qty and add_2 and add_3 and add_4 and add_5 and add_6 and add_7:
            po_data.append({"po_id": po_id, "customer_device": customer_device,
                            "lot_id": lot_id,  "wafer_id_str": wafer_id_str, "add_29": csp_device, "add_30": csp_qty, "add_2": add_2, "add_3": add_3, "add_4": add_4,
                            "add_5": add_5, "add_6": add_6, "add_7": add_7})

            customer_device = ""
            lot_id = ""
            wafer_id_str = ""
            add_4 = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


if __name__ == '__main__':
    po_header = {'user_name': '07885', 'cust_code': 'BJ178', 'po_type': 'ZOR3', 'po_date': '20210610', 'bonded_type': 'Y', 'offer_sheet': '', 'need_delay': 'false', 'delay_days': '', 'need_mail_tip': 'false', 'mail_tip': '', 'po_level': 'primary',
                 'file_name': '5ff368HME-KS-20200909-017.xls', 'template_sn': '604057', 'template_type': 'FO', 'template_desc': 'FO订单', 'create_bank_wo': 'false', 'common_checked': 'false', 'err_desc': '', 'file_path': '/opt/CDM_BACK-END/docs/HME-KS-20210609-018 (1).xls'}
    parse_HK098_FO(po_header)
