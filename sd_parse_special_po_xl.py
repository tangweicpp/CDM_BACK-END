import pandas as pd
import conn_db as conn
import json
import re
import uuid
from flask import abort, make_response
from sd_parse_gen_xl import get_item_master_data
from mm_mat_info import get_mat_master_data


def xstr(s):
    return '' if s is None else str(s).strip()


# 获取随机数
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


# 解析US008_BUMPING_FT订单
def parse_US008_TSV_FT(po_header):
    print(po_header)
    po_data = []
    file_name = po_header['file_path']

    # 文件读取
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    po_obj = {'po_id': '', 'cust_device': '', 'lot_id': '',
              'qty': '', 'pce_price': '', 'total_price': '', 'gross_die': '', 'address_code': '', 'add_4': '', 'add_5': ''}
    for i, row in df.iterrows():
        # 客户机种
        if i == 0:
            po_obj['po_id'] = xstr(row[8])
            if not po_obj['po_id']:
                err_msg = {"ERR_MSG": "PO ID未找到"}
                abort(make_response(err_msg))

        # 跳转到21行
        if i < 20:
            continue

        # 数量金额
        qty = row[8]
        qty_2 = row[5]
        # print("测试数量:", row[5])
        pce_price = xstr(row[9])
        total_price = xstr(row[10])

        if qty and pce_price and total_price:
            if isinstance(qty, int):
                po_obj['qty'] = qty
                po_obj['qty_2'] = qty_2

            po_obj['pce_price'] = pce_price
            po_obj['total_price'] = total_price

        # LOTID
        if row[0] == 'SO#':
            po_obj['add_4'] = xstr(row[1])
            if xstr(row[2])[-2:] == "-B":
                po_obj['cust_device'] = xstr(row[2])[:-2]
            else:
                po_obj['cust_device'] = xstr(row[2])

            po_obj['lot_id'] = xstr(row[3])

            results = get_mat_master_data(
                customer_device=po_obj['cust_device'])
            if results:
                po_obj['gross_die'] = results[0]['ZZJYGD']

            if not (po_obj['cust_device'] and po_obj['gross_die'] and po_obj['qty'] and po_obj['pce_price'] and po_obj['total_price']):
                err_msg = {"ERR_MSG": f"lotid:{po_obj['lot_id']}机种或数量价格未找到"}
                abort(make_response(err_msg))

        # 出货地址
        if row[0] == 'General Instruction':
            po_obj['address_code'] = xstr(row[2])

        if po_obj['po_id'] and po_obj['cust_device'] and po_obj['lot_id'] and po_obj['qty'] and po_obj['pce_price'] and po_obj['total_price'] and po_obj['gross_die'] and po_obj['address_code']:
            # 算片数
            if po_obj['qty'] > 25:
                if int(po_obj['qty_2']) != 1:
                    wafer_qty = int(po_obj['qty_2'])
                else:
                    wafer_qty = 1
                wafer_pce_price = float(po_obj['total_price']) / wafer_qty
            else:
                wafer_qty = int(po_obj['qty'])
                wafer_pce_price = po_obj['pce_price']

            if not isinstance(wafer_qty, int):
                err_msg = {
                    "ERR_MSG": f"lotid:{po_obj['lot_id']} wafer片数不是整数,请查看订单文件"}
                abort(make_response(err_msg))

            po_obj_tmp = {}
            po_obj_tmp['add_4'] = po_obj['add_4']
            po_obj_tmp['po_id'] = xstr(po_obj['po_id'])
            po_obj_tmp['customer_device'] = xstr(po_obj['cust_device'])
            po_obj_tmp['gross_die'] = po_obj['gross_die']
            po_obj_tmp['lot_id'] = po_obj['lot_id']
            po_obj_tmp['wafer_qty'] = str(wafer_qty)
            po_obj_tmp['wafer_pcs_price'] = wafer_pce_price
            po_obj_tmp['total_price'] = po_obj['total_price']
            po_obj_tmp['address_code'], po_obj_tmp['add_1'], po_obj_tmp['add_2'], po_obj_tmp['add_3'], po_obj_tmp['add_5'], po_obj_tmp['add_6'], po_obj_tmp['add_7'] = get_US008_addresscode(
                po_obj['address_code'])
            po_data.append(po_obj_tmp)

            po_obj['cust_device'] = ''
            po_obj['gross_die'] = ''
            po_obj['lot_id'] = ''
            po_obj['qty'] = ''
            po_obj['qty_2'] = ''
            po_obj['pce_price'] = ''
            po_obj['total_price'] = ''
            po_obj['address_code'] = ''
            po_obj['add_4'] = ''

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# 获取地址代码, fab lot, TOP MARK, BOTTOM MARK
def get_US008_addresscode(address_code):
    # BK
    if 'BK:' in address_code:
        tr_bk_code = address_code.split(";")[-6].replace('BK:', '')
        tr_bk_rev = address_code.split(";")[-5].replace('BK_REV:', '')
    else:
        tr_bk_code = ""
        tr_bk_rev = ""

    # MARK
    if "TOP:" in address_code:
        tr_top_mark = address_code.split(";")[-2].replace('TOP:', '')
        tr_bottom_mark = address_code.split(";")[-1].replace('BOTTOM:', '')
    else:
        tr_top_mark = ""
        tr_bottom_mark = ""

    # ADDRESS
    address_code = address_code.upper().replace(
        'WAFER', '').replace('BUMP', '').replace(' ', '')

    address_begin_ch = address_code.find('TO')
    address_end_ch = address_code.find('AFTER')
    tr_address = ''

    # FAB LOT
    fab_begin_ch = address_code.find('LOT#')
    fab_end_ch = address_code.find(';SHIP')
    tr_fab = ''

    if address_begin_ch != -1 and address_end_ch != -1:
        tr_address = address_code[address_begin_ch+2:address_end_ch].strip()

    if fab_begin_ch != -1 and fab_end_ch != -1:
        tr_fab = address_code[fab_begin_ch+4:fab_end_ch].strip()

    # CPN
    up_address_code = address_code.upper()
    if 'CPN#' in up_address_code:
        cpn_begin_ch = up_address_code.find('CPN#')
        tr_cpn = address_code[cpn_begin_ch+4:].strip().split(';')[0]
    else:
        tr_cpn = ''

    return tr_address, tr_fab, tr_top_mark, tr_bottom_mark, tr_cpn, tr_bk_code, tr_bk_rev


# 解析HK005_HK订单
def parse_HK005_HK(po_header):
    po_data = []
    file_name = po_header['file_path']

    # 文件读取
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    if 'PO' in df[0][2]:
        po_id = xstr(df[5][2])

    for i in range(19, 40):
        cust_pn = df[3][i]
        wafer_lot = df[10][i]
        pn_flag = df[30][i]
        wafer_id_str = ""

        if cust_pn and wafer_lot and "常温" in pn_flag:

            for j in range(19, 40):
                lot_id = df[12][j]
                wafer_id_str = df[17][j]
                if lot_id == wafer_lot and wafer_id_str:
                    wafer_id_str = wafer_id_str.replace(";", ",")
                    break

            if po_id and cust_pn and wafer_lot and wafer_id_str:
                po_data.append({"po_id": po_id, "customer_device": cust_pn,
                                "lot_id": lot_id, "wafer_id_str": wafer_id_str})

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# 解析TW039_BUMPING订单
def parse_TW039_BUMPING(po_header):
    po_data = []
    file_name = po_header['file_path']

    # 文件读取
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    # po_id
    if 'PO' in df[48][4]:
        po_id = xstr(df[54][4])

    # customder_device
    if 'Part' in df[28][13]:
        customer_device = df[34][13]

    # add_2
    if '3470' in str(df[12][25]):
        add_2 = df[12][26]

    # add_3
    if 'Product ID' in df[2][30]:
        add_3 = df[12][30]

    # add_4
    if 'Date Code' in df[2][33]:
        add_4 = df[12][33]

    # 默认循环最大行:21
    max_rows = 21
    for i in range(20, 45):
        if '加工項目' in df[0][i]:
            max_rows = i
            break

    for i in range(17, max_rows):
        # FAB DEVICE
        if 'Material Part ID' in df[5][i-1]:
            fab_device = df[5][i]

        # MO LOT
        if 'Lot No' in df[23][i-1]:
            lot_id = df[23][i]

        # FAB LOT
        if 'Original' in df[29][i-1]:
            add_1 = df[29][i]

        # WaferID
        if 'remark' in df[56][i-1]:
            wafer_id_str = df[56][i]

        if po_id and customer_device and fab_device and lot_id and add_1 and add_2 and add_3 and add_4 and wafer_id_str:
            po_data.append({"po_id": po_id, "customer_device": customer_device, "fab_device": fab_device,
                            "lot_id": lot_id, "add_1": add_1, "add_2": add_2, "add_3": add_3, "add_4": add_4, "wafer_id_str": wafer_id_str})
            lot_id = ""
    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# 解析TW039_FT订单
def parse_TW039_FT(po_header):
    po_data = []
    file_name = po_header['file_path']

    # 文件读取
    try:
        df = pd.read_excel(
            file_name, sheet_name=0, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    # po_id
    if 'PO' in df[48][4]:
        po_id = xstr(df[54][4])

    # customder_device
    if 'Part ID' in df[0][10]:
        customer_device = df[8][10]

    # add_2
    if '3470' in str(df[12][25]):
        add_2 = df[12][26]

    # add_3
    if 'Product ID' in df[2][30]:
        add_3 = df[12][30]

    # add_4
    if 'Date Code' in df[2][33]:
        add_4 = df[12][33]

    # 默认循环最大行:21
    max_rows = 21
    for i in range(20, 45):
        if '加工項目' in df[0][i]:
            max_rows = i
            break

    for i in range(17, max_rows):
        # MO LOT
        if 'Lot No' in df[2][31]:
            lot_id = df[12][31]

        # FAB LOT
        if 'Lot No' in df[23][i-1]:
            add_1 = df[23][i]

        # WaferID
        if 'remark' in df[56][i-1]:
            wafer_id_str = df[56][i]

        if po_id and customer_device and lot_id and add_1 and add_2 and add_3 and add_4 and wafer_id_str:
            po_data.append({"po_id": po_id, "customer_device": customer_device,
                            "lot_id": lot_id, "add_1": add_1, "add_2": add_2, "add_3": add_3, "add_4": add_4, "wafer_id_str": wafer_id_str})
            lot_id = ""
            add_1 = ""
            add_2 = ""
            add_3 = ""
            add_4 = ""
            wafer_id_str = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# 解析SH192_BUMPING订单
def parse_SH192_BUMPING(po_header):
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

    # po_id
    print("测试PO:", df[3][4])
    if '委外加工单编号' in df[3][4]:
        po_id = xstr(df[4][4])

    # customder_device
    if '产品名称' in df[0][8]:
        customer_device = df[1][8]

    # 读取sheet2
    try:
        df2 = pd.read_excel(
            file_name, sheet_name=1, header=None, keep_default_na=False)
    except Exception as e:
        err_msg = {"ERR_MSG": f'订单文件打开失败:{e}'}
        abort(make_response(err_msg))

    # 默认循环最大行:21
    max_rows = len(df2)
    # for i in range(1, 25):
    #     if '总计' in df2[0][i]:
    #         max_rows = i
    #         break

    for i in range(1, max_rows):
        if 'Device' in df2[1][0]:
            fab_device = df2[1][i]

        if 'LOT No' in df2[2][0]:
            lot_id = df2[2][i]

        if 'wafer ID' in df2[3][0]:
            wafer_id_str = df2[3][i]

        if po_id and customer_device and fab_device and lot_id and wafer_id_str:
            po_data.append({"po_id": po_id, "customer_device": customer_device, "fab_device": fab_device,
                            "lot_id": lot_id,  "wafer_id_str": wafer_id_str})

            lot_id = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# 解析HW50_BUMPING订单
def parse_HW50_BUMPING(po_header):
    po_data = []
    file_name = po_header['file_path']

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
    # f.close

    # po_id
    print("测试PO:", df[1][4])
    if '製令編號' in df[1][4]:
        po_id = xstr(df[2][4])

    # customder_device
    if '完成品號' in df[3][4]:
        customer_device = xstr(df[4][4])

    # 默认循环最大行:21
    max_rows = 30

    for i in range(1, max_rows):
        if '0001' in df[1][i-1]:
            lot_id = df[2][i].strip()
            wafer_id_str = df[7][i-1].split()[0].strip()

            if po_id and customer_device and lot_id and wafer_id_str:
                po_data.append({"po_id": po_id, "customer_device": customer_device,
                                "lot_id": lot_id,  "wafer_id_str": wafer_id_str})

                lot_id = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# 解析HW50_FC订单
def parse_HW50_FC(po_header):
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

    # print(df.to_html())
    f = open('a.html', 'w')
    str = df.to_html()
    f.write(str)
    f.close

    # po_id
    print("测试PO:", df[1][4])
    if '製令編號' in df[1][4]:
        po_id = xstr(df[2][4])

    # customder_device
    if '完成品名' in df[3][5]:
        customer_device = xstr(df[4][5])

    # add_3
    add_3 = ""
    for i in range(25, 35):
        if 'Marking' in df[1][i] and df[2][i]:
            add_3 = xstr(df[2][i])

    # add_4
    add_4 = ""
    if '完成品號' in df[3][4] and df[4][4]:
        add_4 = xstr(df[4][4])

    # add_5
    add_5 = ""
    if '計劃批號' in df[1][8] and df[2][8]:
        add_5 = xstr(df[2][8])

    # 默认循环最大行:21
    max_rows = 30

    for i in range(1, max_rows):
        if '0001' in df[1][i-1]:
            lot_id = df[2][i].strip()
            wafer_id_str = df[7][i-1].split()[0].strip()

            if po_id and customer_device and lot_id and wafer_id_str and add_3 and add_4 and add_5:
                po_data.append({"po_id": po_id, "customer_device": customer_device,
                                "lot_id": lot_id,  "wafer_id_str": wafer_id_str, "add_3": add_3, "add_4": add_4, "add_5": add_5})

                lot_id = ""

    print(po_data)
    if not po_data:
        print("没有解析到数据")
        err_msg = {"ERR_MSG": '没有解析到数据'}
        abort(make_response(err_msg))

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# 解析SH104_WLP订单
def parse_SH104_WLP(po_header):
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

    customer_device = ""
    fab_device = ""
    lot_id = ""
    wafer_id_str = ""
    add_4 = ""

    # PO
    if "委外单号" in df[0][2] and df[3][2]:
        po_id = xstr(df[3][2]).strip()

    # CUSTOMER DEVICE
    if "回货品名" in df[6][16] and df[6][17]:
        customer_device = xstr(df[6][17])

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

    for i in range(loop_index, max_len):
        if '发料品名' in df[6][i-1] and df[6][i]:
            fab_device = xstr(df[6][i])

        if '发料批号' in df[10][i-1] and df[10][i]:
            lot_id = xstr(df[10][i])

        if '刻号' in df[13][i-1] and df[13][i]:
            wafer_id_str = xstr(df[13][i])

        if '发料测试' in df[24][i-1] and df[24][i]:
            add_4 = xstr(df[24][i])

        # add_3: CSP成品机种, add_30: CSP行数量
        if po_id and customer_device and fab_device and lot_id and wafer_id_str and add_2 and add_3 and add_4 and add_5 and add_6 and add_7:
            po_data.append({"po_id": po_id, "customer_device": customer_device, "fab_device": fab_device,
                            "lot_id": lot_id,  "wafer_id_str": wafer_id_str,  "add_2": add_2, "add_3": add_3, "add_4": add_4,
                            "add_5": add_5, "add_6": add_6, "add_7": add_7})

            fab_device = ""
            lot_id = ""
            wafer_id_str = ""
            add_4 = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# HK098,BJ178,SZ217, GD224 WLP订单
def parse_HK098_WLP(po_header):
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
    customer_device = ""
    fab_device = ""
    if '芯片名称' in df[2][6]:
        for i in range(min_len, max_len):
            if xstr(df[3][i]):
                lot_id = xstr(df[3][i]).strip()
                wafer_id_str = xstr(df[4][i]).strip()
                # 晶圆机种
                for j in range(i, 6, -1):
                    if xstr(df[2][j]):
                        fab_device = xstr(df[2][j]).strip()
                        break

                # 客户机种
                for j in range(i, 6, -1):
                    if xstr(df[1][j]):
                        customer_device = xstr(df[1][j]).strip()
                        break

                # add_3: CSP成品机种, add_30: CSP行数量
                if po_id and customer_device and lot_id and wafer_id_str:
                    po_data.append({"po_id": po_id, "customer_device": customer_device, "fab_device": fab_device,
                                    "lot_id": lot_id,  "wafer_id_str": wafer_id_str})

                    lot_id = ""
                    wafer_id_str = ""
                    customer_device = ""
                    fab_device = ""

    print(po_data)

    # 获取其他物料数据
    po_data = get_item_master_data(po_header, po_data)
    return po_data


# ----------------------------------其他特殊模板------------------------------
# 解析US026/SG005_WI订单
def parse_SG005_WI(w_file):
    # 解析文件
    try:
        df = pd.read_excel(
            w_file, header=None, keep_default_na=False)
        df = df.applymap(lambda x: str(x).strip())

    except Exception as e:
        abort(make_response({"ERR_MSG": "WI文件读取失败:{e}"}))

    con = conn.HanaConn()

    for index, row in df.iterrows():
        if index == 0:
            continue

        # 重复不再传
        sql = f"select * from ZM_CDM_US026_WI_DATA where OVT_JOB = '{row[7]}' and WAFER_ID = '{row[14]}'"
        results = con.query(sql)
        if results:
            continue

        print(len(row))
        rl = len(row)
        paras = ""
        for data in row:
            paras = paras + f"'{data}',"

        paras = paras + "NOW(),'system', ZM_CDM_US026_WI_SEQ.NEXTVAL"

        sql = f"insert into ZM_CDM_US026_WI_DATA values({paras})"
        print(sql)
        if not con.exec_n(sql):
            abort(make_response({"ERR_MSG": "WI数据保存失败"}))

    print("数据保存成功")
    con.db.commit()

    return True


if __name__ == '__main__':
    po_header = {'user_name': '07885', 'cust_code': 'HW50', 'po_type': 'ZOR3', 'po_date': '20210616', 'bonded_type': 'Y', 'offer_sheet': '', 'need_delay': 'false', 'delay_days': '', 'need_mail_tip': 'false', 'mail_tip': '', 'po_level': 'primary',
                 'file_name': '2b8a74TSHT(昆山)PO#3306-2012014-R2.26更新保税FC.xlsx', 'template_sn': '2bdb0a', 'template_type': 'LOT|WAFER|DIES|CUSTPN|FABPN|PO', 'template_desc': 'FC正式模板', 'create_bank_wo': 'false', 'common_checked': 'false', 'err_desc': '', 'file_path': '/opt/CDM_BACK-END/docs/TSHT(昆山PO#3303-2105013-2FC(非保--台湾） (1(0(0).xlsx'}
    parse_HW50_FC(po_header)
