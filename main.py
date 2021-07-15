'''
@File    :   main.py234
@Time    :   2020/08/26 13:09:10
@Author  :   Tony Tang
@Version :   1.0
@Contact :   wei.tang_ks@ht-tech.com
@License :   (C)Copyright 2020-2025, Htks
@Desc    :   main route
'''

from flask import Flask
from flask import jsonify
from flask import request
from flask import make_response
from flask_cors import CORS
from flask import send_from_directory
import json
import com_unit as cu

from sd_parse_po_file import parse_po_file
from sd_create_gen_so import create_gen_so
from sd_create_aa_so import create_aa_so
from sd_create_po_template import upload_po_template

from pp_create_gen_mo import create_mo
from pp_create_gen_mo import refresh_mo_data
from pp_create_aa_mo import get_aa_po_data
from pp_create_aa_mo import update_aa_po_data
from pp_create_dummy_mo import get_dummy_product
from pp_create_dummy_mo import create_dummy_mo
from pp_create_rework_mo import get_rework_inventory
from pp_create_rework_mo import create_rework_mo
from pp_create_fo_mo import get_fo_inventory
from pp_create_fo_mo import create_fo_mo
from mm_mat_info import get_mat_master_data
from mm_mat_info import get_mat_master_data_all
import pp_create_rework_mo as ppcrm
import sd_create_spec_data as scsd
import pp_set_wafer_id as pswi
import os
import pp_get_gen_mo as pcm
import pp_wafer_crop_mgr as pwcm
import pp_create_fo_mo as pcf
import mm_stock_data as msd
import mm_material_entry as mme

app = Flask(__name__)
CORS(app)


# ------------------------------------------------------------------------------------------------------------------------------#
# 登录校验
@app.route('/login', methods=['GET', 'POST'])
def r_login():
    if request.method == 'POST':
        username = request.values.get('username', '').strip()
        password = request.values.get('password', '').strip()
        res = cu.auth(username, password)

        return make_response(jsonify(res), 200)


# 修改密码
@app.route('/modify_user_passwd', methods=['GET', 'POST'])
def r_modify_user_passwd():
    if request.method == 'POST':
        username = request.values.get('username', '').strip()
        old_password = request.values.get('oldPassWord', '').strip()
        new_password = request.values.get('newPasswd', '').strip()
        res = cu.modify_user_passwd(username, old_password, new_password)

        return make_response(jsonify(res), 200)


# -----------------------------------------------------------订单创建---------------------------------------------------------------#
# 选项类型初始化
@app.route('/label_value_list', methods=['GET', 'POST'])
def r_label_value_list():
    if request.method == 'GET':
        res = cu.get_lbl_value_list()
        return make_response(jsonify(res), 200)


# 订单模板
@app.route('/po_template', methods=['GET', 'POST'])
def r_get_po_template():
    if request.method == 'POST':
        cust_code = request.values.get('custCode', '').strip()
        res = cu.get_po_template(cust_code)
        return make_response(jsonify(res), 200)


# SD:解析订单文件
@app.route('/upload_cust_po', methods=['GET', 'POST'])
def r_upload_cust_po():
    if request.method == 'POST':
        po_file = request.files.get('poFile')
        po_header = {}
        po_header['user_name'] = request.values.get('userName')
        po_header['cust_code'] = request.values.get('custCode')
        po_header['po_type'] = request.values.get('poType')
        po_header['po_date'] = request.values.get('poDate')
        po_header['bonded_type'] = request.values.get('bondedType')
        po_header['offer_sheet'] = request.values.get('offerSheet')
        po_header['need_delay'] = request.values.get('needDelay')
        po_header['delay_days'] = request.values.get('delayDays')
        po_header['need_mail_tip'] = request.values.get('needMailTip')
        po_header['mail_tip'] = request.values.get('mailTip', '').strip()
        po_header['po_level'] = request.values.get('poLevel')
        po_header['file_name'] = request.values.get('poFileName')
        po_header['template_sn'] = request.values.get('poTemplateSN')
        po_header['template_type'] = request.values.get('poTemplateType')
        po_header['template_desc'] = request.values.get('poTemplateDesc')
        po_header['create_bank_wo'] = request.values.get('createWaferPo')
        po_header['common_checked'] = request.values.get('commonChecked')
        po_header['process'] = request.values.get('process')
        po_header['err_desc'] = ''

        po_data = parse_po_file(po_file, po_header)
        return make_response(jsonify({'header': po_header, 'items': po_data, 'status': 200}))


# 创建销售订单
@app.route('/create_so', methods=['POST'])
def r_create_so():
    if request.method == 'POST':
        so_data = {}

        request_data = json.loads(request.get_data(as_text=True))
        so_data['header'] = request_data.get('header')
        so_data['items'] = request_data.get('items')

        if so_data['header']['cust_code'] in ('US010') and '量产订单' in so_data['header']['template_desc']:
            res = create_aa_so(so_data)
        else:
            res = create_gen_so(so_data)

        return make_response(jsonify(res), 200)


# 查询订单
@app.route('/query_po_data', methods=['GET', 'POST'])
def r_query_po_data():
    if request.method == 'GET':
        po_query = {}
        po_query['query_value'] = request.args.get('queryValue', '').strip()
        po_query['query_type'] = request.args.get('queryType', '').strip()
        po_query['query_start_date'] = request.args.get('queryStartDate')
        po_query['query_end_date'] = request.args.get('queryEndDate')
        po_query['user_name'] = request.args.get('userName')

        res = cu.query_po_data(po_query)
        return make_response(jsonify(res), 200)


# 导出订单
@app.route('/export_po_data', methods=['GET', 'POST'])
def r_export_po_data():
    if request.method == 'GET':
        po_query = {}
        po_query['query_value'] = request.args.get('queryValue', '').strip()
        po_query['query_type'] = request.args.get('queryType', '').strip()
        po_query['query_start_date'] = request.args.get('queryStartDate')
        po_query['query_end_date'] = request.args.get('queryEndDate')
        po_query['user_name'] = request.args.get('userName')

        res = cu.export_po_data(po_query)
        return make_response(jsonify(res), 200)


# 修改订单
@app.route('/update_po_data', methods=['GET', 'POST'])
def r_update_po_data():
    if request.method == 'POST':
        po_update = {}
        request_data = json.loads(request.get_data(as_text=True))
        po_update['items'] = request_data.get('updateData')

        res = cu.update_po_data(po_update)
        return make_response(jsonify(res), 200)


# 删除订单
@app.route('/delete_po_data', methods=['GET', 'POST'])
def r_delete_po_data():
    if request.method == 'POST':
        po_del = {}
        request_data = json.loads(request.get_data(as_text=True))
        po_del['header'] = request_data.get('delHeader')
        po_del['items'] = request_data.get('delData')

        res = cu.delete_po_data(po_del)
        return make_response(jsonify(res), 200)


# 整票删除订单
@app.route('/delete_po_data2', methods=['GET', 'POST'])
def r_delete_po_data2():
    if request.method == 'GET':
        po_del = {}
        po_del['header'] = request.args.get('delHeader')

        if po_del['header']:
            po_del['header'] = json.loads(po_del['header'])

        po_del['upload_id'] = request.args.get('upload_id', '').strip()
        print(po_del)
        res = cu.delete_po_data2(po_del)
        return make_response(jsonify(res), 200)


# 手动上传SO模板文件
@app.route('/upload_cust_po_2', methods=['GET', 'POST'])
def r_upload_cust_po_2():
    if request.method == 'POST':
        so_file = request.files.get('soFile')

        res = cu.import_so_file(so_file)
        return make_response(jsonify(res), 200)


# 手动创建销售订单
@app.route('/create_so_2', methods=['POST'])
def r_create_so_2():
    if request.method == 'POST':
        so_data = {}

        request_data = json.loads(request.get_data(as_text=True))
        so_data['header'] = request_data.get('header')
        so_data['items'] = request_data.get('items')

        res = cu.create_so(so_data)
        return make_response(jsonify(res), 200)


# 手动上传通用订单模板
@app.route('/post_common_po_data', methods=['POST'])
def r_post_common_po_data():
    if request.method == 'POST':

        header_file = request.files.get('commonPOFile')  # 订单文件
        header_data = {}
        header_data['user_name'] = request.values.get('userName')
        header_data['so_type'] = request.values.get('soType')
        header_data['so_date'] = request.values.get('soDate')
        res = cu.upload_common_po_file(header_data, header_file)
        return make_response(jsonify(res), 200)


# -----------------------------------------------------------订单模板配置--------------------------------------------------------------#
# 上传配置文件
@app.route('/upload_po_template', methods=['GET', 'POST'])
def r_upload_po_template():
    if request.method == 'POST':
        po_original_file = request.files.get('originalFile')  # 原始文件
        po_compare_file = request.files.get('compareFile')  # 对照文件
        po_pic_file = request.files.get('picFile')  # 截图文件
        po_header = {}
        po_header['user_name'] = request.values.get('userName', '').strip()
        po_header['cust_code'] = request.values.get('custCode', '').strip()
        po_header['template_name'] = request.values.get(
            'templateName', '').strip()
        po_header['template_type'] = request.values.get('type', '').strip()

        upload_po_template(po_original_file, po_compare_file,
                           po_pic_file, po_header)
        return make_response("success", 200)


# 获取配置列表
@app.route('/get_cust_po_template_list', methods=['GET'])
def r_get_cust_po_template_list():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cust_code'] = request.args.get('custCode', '').strip()

        ret = cu.get_cust_po_template_list(mo_query)
        return make_response(jsonify(ret), 200)


# 获取配置信息
@app.route('/get_cust_po_template_items', methods=['GET'])
def r_get_cust_po_template_items():
    if request.method == 'GET':
        mo_query = {}
        mo_query['template_sn'] = request.args.get('templateSN')

        ret = cu.get_cust_po_template_items(mo_query)
        return make_response(jsonify(ret), 200)


# 保存配置信息
@app.route('/save_cust_po_template_items', methods=['POST'])
def r_save_cust_po_template_items():
    if request.method == 'POST':
        template_items = {}
        request_data = json.loads(request.get_data(as_text=True))
        template_items['header'] = request_data.get('header')
        template_items['items'] = request_data.get('items')

        ret = cu.save_cust_po_template_items(template_items)
        if ret:
            return make_response("success", 200)
        else:
            return make_response("保存失败", 201)


# 手动创建销售订单
@app.route('/update_wafer_id', methods=['POST'])
def r_update_wafer_id():
    if request.method == 'POST':
        wafer_data = {}

        request_data = json.loads(request.get_data(as_text=True))
        wafer_data['header'] = request_data.get('header')
        wafer_data['items'] = request_data.get('items')

        res = cu.update_wafer_id(wafer_data)
        return make_response(jsonify(res), 200)


# -------------------------------------------------------------特殊订单上传维护---------------------------------------------------------#
# US010 BC
@app.route('/upload_us010_bc', methods=['GET', 'POST'])
def r_upload_us010_bc():
    if request.method == 'POST':
        bc_file = request.files.get('bcFile')

        res = scsd.import_bc_file(bc_file)
        return make_response(jsonify(res), 200)


# 提交BC数据
@app.route('/submit_bc_data', methods=['POST'])
def r_submit_bc_data():
    if request.method == 'POST':
        data = {}

        request_data = json.loads(request.get_data(as_text=True))
        data['header'] = request_data.get('header')
        data['items'] = request_data.get('items')

        res = scsd.submit_bc_data(data)
        return make_response(jsonify(res), 200)


# 查询BC数据
@app.route('/query_bc_data', methods=['GET'])
def r_query_bc_data():
    if request.method == 'GET':
        batch_id = request.args.get('BATCH_ID', '').strip()
        non_batch = request.args.get('NON_BATCH', '').strip()
        print("测试:", batch_id, non_batch)
        res = scsd.query_bc_data(batch_id, non_batch)
        return make_response(jsonify(res), 200)


# 导出BC数据
@app.route('/export_bc_data', methods=['GET'])
def r_export_bc_data():
    if request.method == 'GET':
        res = scsd.export_bc_data()
        return make_response(jsonify(res), 200)


# 导出BC数据
@app.route('/export_feds_data', methods=['GET'])
def r_export_feds_data():
    if request.method == 'GET':
        res = scsd.export_feds_data()
        return make_response(jsonify(res), 200)


# US010 FEDS
@app.route('/upload_us010_feds', methods=['GET', 'POST'])
def r_upload_us010_feds():
    if request.method == 'POST':
        bc_file = request.files.get('bcFile')

        res = scsd.import_feds_file(bc_file)
        return make_response(jsonify(res), 200)


# 提交FEDS数据
@app.route('/submit_feds_data', methods=['POST'])
def r_submit_feds_data():
    if request.method == 'POST':
        data = {}

        request_data = json.loads(request.get_data(as_text=True))
        data['header'] = request_data.get('header')
        data['items'] = request_data.get('items')

        res = scsd.submit_feds_data(data)
        return make_response(jsonify(res), 200)


# 获取US010订单
@app.route('/query_us010_po', methods=['POST'])
def r_query_us010_po():
    if request.method == 'POST':
        data = {}

        request_data = json.loads(request.get_data(as_text=True))
        data['header'] = request_data.get('header')

        res = scsd.query_us010_po(data['header'])
        return make_response(jsonify(res), 200)


# 创建US010订单
@app.route('/create_us010_po', methods=['POST'])
def r_create_us010_po():
    if request.method == 'POST':

        request_data = json.loads(request.get_data(as_text=True))
        po_data = request_data.get('items')

        res = scsd.create_us010_po(po_data)
        return make_response(jsonify(res), 200)


# 获取GC WLA转NORMAL订单--------------------------------------------------GC 转NORMAL---------------------------------------------#
@app.route('/query_GC_trun_normal_po', methods=['POST'])
def r_query_GC_trun_normal_po():
    if request.method == 'POST':
        data = {}

        request_data = json.loads(request.get_data(as_text=True))
        data['header'] = request_data.get('header')

        res = scsd.query_GC_NORMAL_po(data['header'])
        return make_response(jsonify(res), 200)


# 创建US010订单
@app.route('/create_GC_turn_normal_po', methods=['POST'])
def r_create_GC_turn_normal_po():
    if request.method == 'POST':

        request_data = json.loads(request.get_data(as_text=True))
        po_data = request_data.get('items')

        res = scsd.create_turn_normal_po(po_data)
        return make_response(jsonify(res), 200)


# -------------------------------------------------------------工单创建----------------------------------------------------------------#
# 获取工单明细数据
@app.route('/get_mo_lot_items', methods=['GET'])
def r_get_mo_lot_items():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cust_code'] = request.args.get('custCode', '').strip()
        mo_query['mo_type'] = request.args.get('moType', '').strip()
        mo_query['product_name_type'] = request.args.get(
            'productNameType', '').strip()
        mo_query['product_name'] = request.args.get('productName', '').strip()
        # res = get_mo_lot_items(mo_query)
        res = pcm.get_mo_data(mo_query)
        return make_response(jsonify(res), 200)


# 创建工单
@app.route('/create_mo', methods=['POST'])
def r_create_mo():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        res = create_mo(mo_data)
        return make_response(jsonify(res), 200)


# 工单查询
@app.route('/query_mo_list', methods=['GET'])
def r_query_mo_list():
    if request.method == 'GET':
        mo_query = {}
        mo_query['mo_pn'] = request.args.get('moPN').strip()
        mo_query['mo_pn_type'] = request.args.get('moPNType').strip()

        mo_list = cu.query_mo_list(mo_query)
        return make_response(jsonify({"mo_list": mo_list}), 200)


# 工单导出
@app.route('/export_mo_list', methods=['GET'])
def r_export_mo_list():
    if request.method == 'GET':
        mo_query = {}
        mo_query['mo_pn'] = request.args.get('moPN').strip()
        mo_query['mo_pn_type'] = request.args.get('moPNType').strip()
        mo_query['start_date'] = request.args.get('startDate').strip()
        mo_query['end_date'] = request.args.get('endDate').strip()
        mo_query['export_flag'] = request.args.get('exportFlag').strip()
        mo_query['user_name'] = request.args.get('userName').strip()

        mo_list = cu.export_mo_list(mo_query)
        return make_response(jsonify({"mo_list": mo_list}), 200)


# 工单删除
@app.route('/delete_mo_data', methods=['GET', 'POST'])
def r_delete_mo_data():
    if request.method == 'POST':
        mo_del = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_del['items'] = request_data.get('delItems')
        mo_del['del_by'] = request_data.get('delBy')
        mo_del['del_reason'] = request_data.get('delReason')
        mo_del['del_CDM'] = request_data.get('delCDM')

        response = cu.delete_mo_data(mo_del)
        return make_response(jsonify(response), 200)


# 工单修改
@app.route('/update_mo_data', methods=['GET', 'POST'])
def r_update_mo_data():
    if request.method == 'POST':
        mo_update = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_update['items'] = request_data.get('moItems')

        res = cu.update_mo_data(mo_update)
        return make_response(jsonify(res), 200)


# 工单刷新
@app.route('/refresh_mo_data', methods=['GET', 'POST'])
def r_refresh_mo_data():
    if request.method == 'POST':
        mo_refresh = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_refresh['items'] = request_data.get('moItems')

        res = refresh_mo_data(mo_refresh)
        return make_response(jsonify(res), 200)


# 获取工单CT
# @app.route('/get_customer_device_ct', methods=['GET'])
# def r_get_customer_device_ct():
#     if request.method == 'GET':
#         mo_query = {}
#         mo_query['customer_device'] = request.args.get(
#             'customer_device').strip()

#         mo_list = cu.export_mo_list(mo_query)
#         return make_response(jsonify({"mo_list": mo_list}), 200)


# ------------------------------------------------------------DUMMY,玻璃,硅基-----------------------------
# 获取DUMMY工单,硅基工单,玻璃工单的物料信息
@app.route('/get_dummy_product', methods=['GET'])
def r_get_product_grossdie():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cust_code'] = request.args.get('custCode', '').strip()
        mo_query['product_name'] = request.args.get('productName', '').strip()

        res = get_dummy_product(mo_query)
        return make_response(jsonify(res), 200)


# DUMMY工单创建
@app.route('/create_mo_dummy', methods=['POST'])
def r_create_mo_dummy():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        res = create_dummy_mo(mo_data)
        return make_response(jsonify(res), 200)


# SI硅基工单lotID申请
@app.route('/get_SI_lot_items', methods=['GET'])
def r_get_SI_lot_items():
    if request.method == 'GET':
        lots = int(request.args.get('lots'))
        res = pcf.get_SI_ID(lots)
        # print(res)
        return make_response(jsonify(res), 200)


# -------------------------------------------------------------重工工单------------------------------------------------------
# 重工库存查询
@app.route('/get_rework_inventory', methods=['GET'])
def r_get_rework_inventory():
    if request.method == 'GET':
        mo_query = {}
        mo_query['lot_id'] = request.args.get('lotID', '').strip()
        mo_query['mo_location'] = request.args.get('moLocation', '').strip()
        res = get_rework_inventory(mo_query)
        return make_response(jsonify(res), 200)


# 重工工单创建
@app.route('/create_rework_mo', methods=['POST'])
def r_create_mo_rework():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        res = create_rework_mo(mo_data)
        if res['STATUS']:
            return make_response('success', 200)
        else:
            return make_response(res['ERR_DESC'], 201)


# 散袋工单创建
@app.route('/create_sandai_mo', methods=['POST'])
def r_create_sandai_mo():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        res = ppcrm.create_sandai_mo(mo_data)
        if res['STATUS']:
            return make_response('success', 200)
        else:
            return make_response(res['ERR_DESC'], 201)


# --------------------------------------------------------------FO工单----------------------------------------------------------
# FO硅基库存查询
@app.route('/get_fo_inventory', methods=['GET'])
def r_get_fo_inventory():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cspProductID'] = request.args.get('cspProductID').strip()
        mo_query['mo_location'] = request.args.get('moLocation').strip()

        response = get_fo_inventory(mo_query)
        return make_response(jsonify(response), 200)

# FO硅基-DC库存查询


@app.route('/get_fo_dc_inventory', methods=['GET'])
def r_get_fo_dc_inventory():
    if request.method == 'GET':
        mo_query = {}
        mo_query['SILotID'] = request.args.get('SILotID').strip()
        mo_query['mo_location'] = request.args.get('moLocation').strip()

        print(mo_query)

        response = pcf.get_fo_dc_inventory(mo_query)
        return make_response(jsonify(response), 200)


# CSP工单
@app.route('/create_fo_mo', methods=['POST'])
def r_create_mo_fo():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        ret = create_fo_mo(mo_data)
        return make_response(jsonify(ret), 200)


# FO_DC工单
@app.route('/create_fo_dc_mo', methods=['POST'])
def r_create_mo_dc_fo():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        ret = pcf.create_fo_dc_mo(mo_data)
        return make_response(jsonify(ret), 200)


# -----------------------------------------------------AA订单工单------------------------------------
# AA订单查询
@app.route('/get_aa_po_data', methods=['GET'])
def r_get_aa_po_data():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cust_device'] = request.args.get('custDevice', '').strip()
        mo_query['mo_type_2'] = request.args.get('moType2', '').strip()
        mo_query['lot_id'] = request.args.get('lotID', '').strip()

        res = get_aa_po_data(mo_query)
        return make_response(jsonify(res), 200)


# AA工单
@app.route('/update_aa_po_data', methods=['POST'])
def r_create_mo_aa():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        res = update_aa_po_data(mo_data)
        return make_response(jsonify(res), 200)


# ----------------------------------------------------物料主数据信息查询-------------------------------------------
# 物料主数据
@app.route('/get_product_info', methods=['GET'])
def r_get_product_info():
    if request.method == 'GET':
        product_name = request.args.get('productName').strip()
        product_name_type = request.args.get('productNameType').strip()

        customer_device = product_name if product_name_type == 'P1' else ''
        ht_device = product_name if product_name_type == 'P2' else ''
        product_no = product_name if product_name_type == 'P3' else ''
        sap_product_no = product_name if product_name_type == 'P4' else ''

        res = {'ERR_MSG': ''}
        res['DATA'] = get_mat_master_data_all(customer_device=customer_device, ht_device=ht_device,
                                          product_no=product_no, sap_product_no=sap_product_no)

        return make_response(jsonify(res), 200)


# 物料BOM
@app.route('/get_product_bom_info', methods=['GET'])
def r_get_product_bom_info():
    if request.method == 'GET':
        product_name = request.args.get('productName').strip()

        res = cu.get_product_bom_info(product_name)

        return make_response(jsonify(res), 200)


# 物料库存
@app.route('/get_product_inv', methods=['GET'])
def r_get_product_inv():
    if request.method == 'GET':
        lot_id = request.args.get('lotID').strip()
        matnr_id = request.args.get('matnrID').strip()
        wafer_id = request.args.get('waferID').strip()
        outbox_id = request.args.get('outboxID').strip()
        inv_type = request.args.get('invType').strip()

        print(lot_id, matnr_id, wafer_id, outbox_id)
        res = cu.get_product_inv(
            lot_id, matnr_id, wafer_id, outbox_id, inv_type)
        return make_response(jsonify(res), 200)


# 物料库存
@app.route('/get_po_wafer_info', methods=['GET'])
def r_get_po_wafer_info():
    if request.method == 'GET':
        lot_id = request.args.get('lotID').strip()
        product_id = request.args.get('productID').strip()
        res = cu.get_po_wafer_info(lot_id, product_id)
        return make_response(jsonify(res), 200)


# 物料库存
@app.route('/get_wafer_mark_code', methods=['GET'])
def r_get_wafer_mark_code():
    if request.method == 'GET':
        lot_id = request.args.get('lotID').strip()
        lot_id_type = request.args.get('lotIDType').strip()

        res = cu.get_wafer_mark_code(lot_id, lot_id_type)

        return make_response(jsonify(res), 200)


# 更新打标码
@app.route('/update_wafer_mark_code', methods=['POST'])
def r_update_wafer_mark_code():
    if request.method == 'POST':
        mark_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mark_data['header'] = request_data.get('markHeader')
        mark_data['items'] = request_data.get('markData')

        res = cu.update_wafer_mark_code(mark_data)
        return make_response(jsonify(res), 200)


# ------------------------------------------------------工单自定义属性--------------------------------------------
# 查询工单自定义属性
@app.route('/get_cust_mo_attr', methods=['GET'])
def r_get_cust_mo_attr():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cust_code'] = request.args.get('custCode', '').strip()
        mo_query['mo_level'] = request.args.get('moLevel', '').strip()

        res = cu.get_cust_mo_attr(mo_query)
        return make_response(jsonify(res), 200)


# 新增工单自定义属性
@app.route('/new_cust_mo_attr', methods=['POST'])
def r_new_cust_mo_attr():
    if request.method == 'POST':
        mo_new = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_new['item'] = request_data.get('item')

        res = cu.new_cust_mo_attr(mo_new)
        return make_response(jsonify(res), 200)


# -------------------------------------------------------------------------------------------------------------------
# 更新晶圆片号
@app.route('/set_wafer_id', methods=['GET'])
def r_set_wafer_id():
    if request.method == 'GET':
        lot_id = request.args.get('lotID', '').strip()
        print("更新", lot_id)

        res = pswi.set_wafer_id(lot_id)
        return make_response(jsonify(res), 200)


# 最新更新片号
@app.route('/update_wafer_id_new', methods=['POST'])
def r_update_wafer_id_new():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        header_data = request_data.get('header')
        res = pswi.update_wafer_id_new(request_data.get('sel'), header_data)
        return make_response(jsonify(res), 200)


# -------------------------------------------------------------远程下载-----------------------------------------------

@app.route('/download/<fileID>', methods=['GET', 'POST'])
def r_download(fileID):
    file_path, file_name, file_abs_path = cu.get_download_file_path(fileID)

    try:
        if os.path.isdir(file_abs_path):
            return '<h1>文件夹无法下载</h1>'
        else:
            print("文件名:", file_abs_path, "准备下载")
            return send_from_directory(directory=file_path, filename=file_name, as_attachment=True)
    except:
        return '<h1>该文件不存在或无法下载</h1>'


# --------------------------------特殊数据维护--------------------------------------------------------------
# 上传维护数据
@app.route('/post_maintain_data', methods=['GET', 'POST'])
def r_post_maintain_data():
    if request.method == 'POST':
        header_file = request.files.get('originalFile')  # 原始文件
        header_data = {}

        header_data['user_name'] = request.values.get('userName')
        header_data['cust_code'] = request.values.get('custCode')
        header_data['template_name'] = request.values.get('templateName')
        header_data['type'] = request.values.get('type')
        print(header_data)
        res = scsd.upload_common_file(header_data, header_file)

        return make_response(jsonify(res), 200)


# 查询维护数据
@app.route('/get_maintain_data', methods=['GET', 'POST'])
def r_get_maintain_data():
    if request.method == 'GET':
        header_data = {}
        header_data['current_page'] = request.values.get('currentPage')
        header_data['page_size'] = request.values.get('pageSize')
        header_data['mt_type'] = request.values.get('mtType')
        header_data['mt_attr1'] = request.values.get('mtAttr1')

        res = scsd.get_common_file(header_data)
        return make_response(jsonify(res), 200)


# 新增维护数据
@app.route('/new_maintain_data', methods=['GET', 'POST'])
def r_new_maintain_data():
    if request.method == 'POST':
        md_new = {}
        request_data = json.loads(request.get_data(as_text=True))
        md_new['items'] = request_data.get('items')
        md_new['header'] = request_data.get('header')

        res = scsd.new_MT_Data(md_new)
        return make_response(jsonify(res), 200)


# 删除维护数据
@app.route('/delete_maintain_data', methods=['GET', 'POST'])
def r_delete_maintain_data():
    if request.method == 'POST':
        md_new = {}
        request_data = json.loads(request.get_data(as_text=True))
        md_new['items'] = request_data.get('items')
        md_new['header'] = request_data.get('header')

        res = scsd.delete_MT_data(md_new)
        return make_response(jsonify(res), 200)


# ------------------------蓝膜库存管理---------------------------------------------------------------
# 查询待维护数据
@app.route('/get_mo_crop_data', methods=['GET', 'POST'])
def r_get_mo_crop_data():
    if request.method == 'GET':
        header_data = {}
        header_data['query_type'] = request.args.get('queryType')
        header_data['query_value'] = request.args.get('queryValue')
        header_data['query_start_date'] = request.args.get('queryStartDate')
        header_data['query_end_date'] = request.args.get('queryEndDate')
        header_data['query_limit'] = request.args.get('queryLimit')

        print(header_data)
        res = pwcm.get_mo_crop_data(header_data)
        return make_response(jsonify(res), 200)


# 查询已维护记录
@app.route('/get_mo_crop_history', methods=['GET', 'POST'])
def r_get_mo_crop_history():
    if request.method == 'GET':
        header_data = {}
        header_data['query_type'] = request.args.get('queryType')
        header_data['query_value'] = request.args.get('queryValue')
        header_data['query_start_date'] = request.args.get('queryStartDate')
        header_data['query_end_date'] = request.args.get('queryEndDate')

        print(header_data)
        res = pwcm.get_mo_crop_history(header_data)
        return make_response(jsonify(res), 200)


# 导出记录
@app.route('/export_mo_crop_history', methods=['GET', 'POST'])
def r_export_mo_crop_history():
    if request.method == 'GET':
        header_data = {}
        header_data['query_type'] = request.args.get('queryType')
        header_data['query_value'] = request.args.get('queryValue')
        header_data['query_start_date'] = request.args.get('queryStartDate')
        header_data['query_end_date'] = request.args.get('queryEndDate')

        print(header_data)
        res = pwcm.export_mo_crop_history(header_data)
        return make_response(jsonify(res), 200)


# 退回蓝膜仓
@app.route('/wafer_crop_move_in', methods=['GET', 'POST'])
def r_wafer_crop_move_in():
    if request.method == 'POST':
        md_new = {}
        request_data = json.loads(request.get_data(as_text=True))
        md_new['items'] = request_data.get('items')
        md_new['header'] = request_data.get('header')

        res = pwcm.wafer_crop_move_in(md_new)
        return make_response(jsonify(res), 200)


# 调出蓝膜仓
@app.route('/wafer_crop_move_out', methods=['GET', 'POST'])
def r_wafer_crop_move_out():
    if request.method == 'POST':
        md_new = {}
        request_data = json.loads(request.get_data(as_text=True))
        md_new['items'] = request_data.get('items')
        md_new['header'] = request_data.get('header')

        res = pwcm.wafer_crop_move_out(md_new)
        return make_response(jsonify(res), 200)


# 查询工单数据
@app.route('/get_mo_inv_data', methods=['GET', 'POST'])
def r_get_mo_inv_data():
    if request.method == 'GET':
        header_data = {}
        header_data['mo_id'] = request.values.get('cdmMOID').strip()

        res = pwcm.get_mo_inv_data(header_data)
        return make_response(jsonify(res), 200)


# 打印蓝膜标签
@app.route('/print_label_01', methods=['POST'])
def r_print_label_01():
    if request.method == 'POST':
        print_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        print_data['items'] = request_data.get('items')

        res = pwcm.print_label_01(print_data)
        return make_response(jsonify(res), 200)


# -----------------------异常需求管理---------------------------------------------------------------
@app.route('/upload_execption', methods=['POST'])
def r_upload_execption():
    if request.method == 'POST':
        pics = request.files.getlist('pics')
        docs = request.files.getlist('docs')
        header_data = {}
        header_data['excep_desc'] = request.values.get('excepDesc')
        header_data['user_name'] = request.values.get('userName')
        header_data['excep_from'] = request.values.get('excepFrom')
        header_data['excep_to'] = request.values.get('excepTo')
        header_data['excep_to_grp'] = request.values.get('excepToGrp')
        header_data['excep_type'] = request.values.get('excepType')
        header_data['excep_level'] = request.values.get('execpLevel')

        res = cu.exception_upload(pics, docs, header_data)
        return make_response(jsonify(res), 200)


# 查询异常记录
@app.route('/get_exception_items', methods=['GET', 'POST'])
def r_get_exception_items():
    if request.method == 'GET':
        query_data = {}
        query_data = request.values.get('queryData')
        print(query_data)

        res = cu.get_exception_items(query_data)
        return make_response(jsonify(res), 200)


# 删除异常项
@app.route('/remove_exception_item', methods=['GET', 'POST'])
def r_remove_exception_item():
    if request.method == 'POST':
        del_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        del_data['items'] = request_data.get('items')

        res = cu.remove_exception_item(del_data)
        return make_response(jsonify(res), 200)


# 更新异常项
@app.route('/update_exception_item', methods=['GET', 'POST'])
def r_update_exception_item():
    if request.method == 'POST':
        del_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        del_data['items'] = request_data.get('items')
        del_data['header'] = request_data.get('header')

        res = cu.update_exception_item(del_data)
        return make_response(jsonify(res), 200)


# 导出异常项
@app.route('/export_exception_item', methods=['GET', 'POST'])
def r_export_maintain_data():
    if request.method == 'POST':
        export_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        export_data['header'] = request_data.get('header')

        res = cu.export_exception_item(export_data)

        return make_response(jsonify(res), 200)


# ------------------------------------------------------------线边库存----------------------------------------------
# 查询线边库存
@app.route('/get_xb_inv_items', methods=['GET', 'POST'])
def r_get_xb_inv_items():
    if request.method == 'GET':
        query_data = {}
        query_data['moLocation'] = request.values.get('moLocation')
        query_data['userName'] = request.values.get('userName')
        query_data['productID'] = request.values.get('productID')

        res = msd.get_xb_inv_items(query_data)
        return make_response(jsonify(res), 200)


# 导出盘点数据
@app.route('/export_xb_inv_items', methods=['GET', 'POST'])
def r_export_xb_inv_items():
    if request.method == 'GET':
        query_data = {}
        query_data['moLocation'] = request.values.get('moLocation')
        query_data['userName'] = request.values.get('userName')
        query_data['productID'] = request.values.get('productID')
        query_data['queryStartDate'] = request.values.get('queryStartDate')
        query_data['queryEndDate'] = request.values.get('queryEndDate')

        res = msd.export_xb_inv_items(query_data)
        return make_response(jsonify(res), 200)


# 导入库存数据
@app.route('/import_xb_inv_items', methods=['GET', 'POST'])
def r_import_xb_inv_items():
    if request.method == 'POST':
        header_file = request.files.get('partListFile')
        header_data = {}
        header_data['moLocation'] = request.values.get('moLocation')
        header_data['userName'] = request.values.get('userName')
        header_data['productID'] = request.values.get('productID')

        res = msd.import_xb_inv_items(header_file, header_data)
        return make_response(jsonify(res), 200)


# 保存线边库存
@app.route('/save_xb_inv_items', methods=['GET', 'POST'])
def r_save_xb_inv_items():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = {}
        save_data['header'] = request_data.get('header')
        save_data['items'] = request_data.get('items')

        res = msd.save_xb_inv_items(save_data)

        return make_response(jsonify(res), 200)


# 导出盘点物料清单
@app.route('/download_xb_part_list', methods=['GET', 'POST'])
def r_download_xb_part_list():
    if request.method == 'GET':
        query_data = {}
        query_data['moLocation'] = request.values.get('moLocation')
        query_data['userName'] = request.values.get('userName')
        query_data['productID'] = request.values.get('productID')

        res = msd.download_xb_part_list(query_data)
        return make_response(jsonify(res), 200)


# 导入盘点物料清单
@app.route('/upload_xb_part_list', methods=['GET', 'POST'])
def r_upload_xb_part_list():
    if request.method == 'POST':
        header_file = request.files.get('partListFile')
        header_data = {}
        header_data['moLocation'] = request.values.get('moLocation')
        header_data['userName'] = request.values.get('userName')
        header_data['productID'] = request.values.get('productID')

        res = msd.upload_xb_part_list(header_file, header_data)
        return make_response(jsonify(res), 200)


# 昆山天气
@app.route('/ks_weather', methods=['GET', 'POST'])
def r_ks_weather():
    if request.method == 'GET':
        query_data = {}
        query_data['user_id'] = request.values.get('user_id')
        query_data['user_name'] = request.values.get('user_name')
        res = cu.get_new_weather(query_data)
        return make_response(jsonify(res), 200)


# ----------------------------------------------------------------------------用户权限管理------------------------------------------------#
# 用户权限管理------------------------------------------------------
@app.route('/get_user_rights', methods=['GET', 'POST'])
def r_get_user_rights():
    if request.method == 'GET':
        query_data = {}
        query_data['user_id'] = request.values.get('user_id').strip()
        query_data['user_name'] = request.values.get('user_name').strip()
        query_data['sub_id'] = request.values.get('sub_id')

        res = cu.get_user_rights(query_data)
        return make_response(jsonify(res), 200)


# 获取用户权限
@app.route('/get_user_rights_list', methods=['GET', 'POST'])
def r_get_user_rights_list():
    if request.method == 'GET':
        query_data = {}
        query_data['user_name'] = request.values.get('userName').strip()

        res = cu.get_user_rights_list(query_data)
        return make_response(jsonify(res), 200)


# 创建用户
@app.route('/create_user', methods=['GET', 'POST'])
def r_create_user():
    if request.method == 'GET':
        query_data = {}
        query_data['user_name'] = request.values.get('userName').strip()

        res = cu.create_user(query_data)
        return make_response(jsonify(res), 200)


# 激活用户权限
@app.route('/active_user_rights', methods=['GET', 'POST'])
def r_active_user_rights():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = {}
        save_data['header'] = request_data.get('header')
        save_data['items'] = request_data.get('items')

        res = cu.active_user_rights(save_data)

        return make_response(jsonify(res), 200)


# 冻结用户权限
@app.route('/frozen_user_rights', methods=['GET', 'POST'])
def r_frozen_user_rights():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = {}
        save_data['header'] = request_data.get('header')
        save_data['items'] = request_data.get('items')
        print(save_data)
        res = cu.frozen_user_rights(save_data)
        return make_response(jsonify(res), 200)


# 获取权限对象
@app.route('/get_menus_options', methods=['GET', 'POST'])
def r_get_menus_options():
    if request.method == 'GET':
        res = cu.get_menus_options()
        return make_response(jsonify(res), 200)


# 新增用户权限
@app.route('/add_user_rights', methods=['GET', 'POST'])
def r_add_user_rights():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = request_data.get('header')

        res = cu.add_user_rights(save_data)
        return make_response(jsonify(res), 200)


# 删除用户权限
@app.route('/del_user_rights', methods=['GET', 'POST'])
def r_del_user_rights():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = request_data.get('header')

        res = cu.del_user_rights(save_data)
        return make_response(jsonify(res), 200)


# 新增用户组权限
@app.route('/add_user_group_rights', methods=['GET', 'POST'])
def r_add_user_group_rights():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = request_data.get('header')

        res = cu.add_user_group_rights(save_data)
        return make_response(jsonify(res), 200)


# 删除用户权限
@app.route('/del_user_group_rights', methods=['GET', 'POST'])
def r_del_user_group_rights():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = request_data.get('header')

        res = cu.del_user_group_rights(save_data)
        return make_response(jsonify(res), 200)


# 拷贝用户权限
@app.route('/copy_user_rights', methods=['GET', 'POST'])
def r_copy_user_rights():
    if request.method == 'GET':
        query_data = {}
        query_data['user_name'] = request.values.get('userName').strip()
        query_data['user_name_copy'] = request.values.get(
            'userNameCopy').strip()

        res = cu.copy_user_rights(query_data)
        return make_response(jsonify(res), 200)


# 程序发布说明
@app.route('/publish_sys_info', methods=['GET', 'POST'])
def r_publish_sys_info():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = {}
        save_data['header'] = request_data.get('header')

        res = msd.publish_sys_info(save_data)
        return make_response(jsonify(res), 200)


@app.route('/publish_sys_info_finish', methods=['GET', 'POST'])
def r_publish_sys_info_finish():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))
        save_data = {}
        save_data['header'] = request_data.get('header')

        res = msd.publish_sys_info_finish(save_data)
        return make_response(jsonify(res), 200)


# ----------------------------------------------------管制仓管理---------------------------
# PDA大仓出
@app.route('/pda_material_entry', methods=['POST'])
def r_material_entry():
    if request.method == 'POST':
        req_data = request.get_data(as_text=True)
        resp = mme.entry_data(req_data)
        return make_response(jsonify(resp), 200)


# ---------------------------------------------------------------------------
# PDA入管制仓
@app.route('/pda_material_in_query', methods=['GET'])
def r_pda_material_in_query():
    if request.method == 'GET':
        query_data = {}
        query_data['order'] = request.args.get('ORDER')
        query_data['user'] = request.args.get('USER')

        resp = mme.pda_material_in_query(query_data)
        return make_response(jsonify(resp), 200)


# PDA入管制仓提交
@app.route('/pda_material_in_commit', methods=['GET'])
def r_pda_material_in_commit():
    if request.method == 'GET':
        query_data = {}
        query_data['order'] = request.args.get('ORDER')
        query_data['user'] = request.args.get('USER')

        resp = mme.pda_material_in_commit(query_data)
        return make_response(jsonify(resp), 200)


# ---------------------------------------------------------------------------
# PDA出管制仓
@app.route('/pda_material_out_query', methods=['GET'])
def r_pda_material_out_query():
    if request.method == 'GET':
        query_data = {}
        query_data['order'] = request.args.get('ORDER')
        query_data['user'] = request.args.get('USER')

        resp = mme.pda_material_out_query(query_data)
        return make_response(jsonify(resp), 200)


# PDA出管制仓提交
@app.route('/pda_material_out_commit', methods=['GET'])
def r_pda_material_out_commit():
    if request.method == 'GET':
        query_data = {}
        query_data['order'] = request.args.get('ORDER')
        query_data['user'] = request.args.get('USER')

        resp = mme.pda_material_out_commit(query_data)
        return make_response(jsonify(resp), 200)


# ---------------------------------------------------------------------------
# WEB前台管理
@app.route('/get_material_entry_detail', methods=['GET', 'POST'])
def r_get_material_entry_detail():
    if request.method == 'GET':
        query_data = {}
        query_data['queryOrder'] = request.args.get('queryOrder')
        query_data['queryMatnr'] = request.args.get('queryMatnr')
        query_data['queryStartDate'] = request.args.get('queryStartDate')
        query_data['queryEndDate'] = request.args.get('queryEndDate')

        res = mme.get_material_detail(query_data)
        return make_response(jsonify(res), 200)


# 导出清单
@app.route('/export_material_entry_detail', methods=['GET', 'POST'])
def r_export_material_entry_detail():
    if request.method == 'GET':
        print("TEST")
        header_data = {}
        header_data['queryOrder'] = request.args.get('queryOrder')
        header_data['queryMatnr'] = request.args.get('queryMatnr')
        header_data['queryStartDate'] = request.args.get('queryStartDate')
        header_data['queryEndDate'] = request.args.get('queryEndDate')

        res = mme.export_material_entry_detail(header_data)
        return make_response(jsonify(res), 200)


# 获取物料管制仓库存明细
@app.route('/get_material_inventory_detail', methods=['GET', 'POST'])
def r_get_material_inventory_detail():
    if request.method == 'GET':
        query_data = {}
        query_data['queryMatnr'] = request.args.get('queryMatnr')

        res = mme.get_material_inventory_detail(query_data)
        return make_response(jsonify(res), 200)


# ---------------------包装工艺查询download PO&&& MES工单信息--------------
# 根据工单号/LOT返回对应的upload_id
@app.route('/get_upload_id_po', methods=['GET'])
def r_get_upload_id_po():
    if request.method == 'GET':
        query_data = {}
        query_data['selType'] = request.args.get('selType').strip()
        query_data['selText'] = request.args.get('selText').strip()

        res = cu.get_upload_id_po(query_data)

        return make_response(jsonify(res), 200)


@app.route('/get_upload_id_wo', methods=['GET'])
def r_get_upload_id_wo():
    if request.method == 'GET':
        query_data = {}
        query_data['selType'] = request.args.get('selType').strip()
        query_data['selText'] = request.args.get('selText').strip()

        res = cu.get_upload_id_wo(query_data)

        return make_response(jsonify(res), 200)


# PO文件
@app.route('/download_po/<fileID>', methods=['GET', 'POST'])
def r_download_po(fileID):
    file_path, file_name, file_abs_path = cu.get_download_file_path_2(
        fileID, '1')

    try:
        if os.path.isdir(file_abs_path):
            return '<h1>文件夹无法下载</h1>'
        else:
            print("文件名:", file_abs_path, "准备下载")
            return send_from_directory(directory=file_path, filename=file_name, as_attachment=True)
    except:
        return '<h1>该文件不存在或无法下载</h1>'


# WO文件
@app.route('/download_wo/<fileID>', methods=['GET', 'POST'])
def r_download_wo(fileID):
    file_path, file_name, file_abs_path = cu.get_download_file_path_2(
        fileID, '2')

    try:
        if os.path.isdir(file_abs_path):
            return '<h1>文件夹无法下载</h1>'
        else:
            print("文件名:", file_abs_path, "准备下载")
            return send_from_directory(directory=file_path, filename=file_name, as_attachment=True)
    except:
        return '<h1>该文件不存在或无法下载</h1>'


# 查询MES工单信息
@app.route('/get_mes_mo_data', methods=['GET'])
def r_get_mes_mo_data():
    if request.method == 'GET':
        query_data = {}
        query_data['selType'] = request.args.get('selType').strip()
        query_data['selText'] = request.args.get('selText').strip()

        res = cu.get_mes_mo_data(query_data)

        return make_response(jsonify(res), 200)


# 导出MES工单
@app.route('/export_mes_mo', methods=['POST'])
def r_export_mes_mo():
    if request.method == 'POST':
        request_data = json.loads(request.get_data(as_text=True))

        req_data = {}
        req_data['xlHeader'] = request_data.get('xlHeader')
        req_data['xlItems'] = request_data.get('xlItems')
        req_data['moID'] = request_data.get('moID').strip()

        resp = cu.export_mes_mo(req_data)
        return make_response(jsonify(resp), 200)


# -------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # app.run(host='0.0.0.0', debug=True, port=5078, threaded=True)
    app.config['JSON_AS_ASCII'] = False
    app.run(host='0.0.0.0', debug=True, port=5025, threaded=True)

    # server = pywsgi.WSGIServer(('0.0.0.0', 5025), app)
    # server.serve_forever()
