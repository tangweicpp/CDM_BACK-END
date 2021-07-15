'''
@File    :   main.py
@Time    :   2020/08/26 13:09:10
@Author  :   Tony Tang 
@Version :   1.0
@Contact :   wei.tang_ks@ht-tech.com
@License :   (C)Copyright 2020-2025, Htks
@Desc    :   main route
'''
from gevent.pywsgi import WSGIServer
import gevent
from gevent import monkey
monkey.patch_all()

from multiprocessing import cpu_count, Process
from flask import Flask
from flask import jsonify
from flask import request
from flask import make_response
from flask_cors import CORS
import json
import com_unit as cu
import create_so as cs
import create_wo as cw
import create_mo as cm
import create_po_template as cpt
from parse_po import get_po_data



app = Flask(__name__)
CORS(app)


@app.route('/login', methods=['GET', 'POST'])
def r_login():
    if request.method == 'POST':
        username = request.values.get('username')
        password = request.values.get('password')
        if cu.auth(username, password):
            return make_response("success", 200)
        else:
            return make_response("用户名或密码不存在", 201)


@app.route('/cust_code_list', methods=['GET', 'POST'])
def r_get_cust_code_list():
    if request.method == 'GET':
        json_data = cu.get_cust_code_list()
        return make_response(jsonify(json_data), 200)


@app.route('/so_type_list', methods=['GET', 'POST'])
def r_get_so_type_list():
    if request.method == 'GET':
        json_data = cu.get_so_type_list()
        return make_response(jsonify(json_data), 200)


@app.route('/mo_type_list', methods=['GET', 'POST'])
def r_get_mo_type_list():
    if request.method == 'GET':
        json_data = cu.get_mo_type_list()
        return make_response(jsonify(json_data), 200)


@app.route('/mo_prefix_list', methods=['GET', 'POST'])
def r_get_mo_prefix_list():
    if request.method == 'GET':
        json_data = cu.get_mo_prefix_list()
        return make_response(jsonify(json_data), 200)


@app.route('/po_template', methods=['GET', 'POST'])
def r_get_po_template():
    if request.method == 'POST':
        cust_code = request.values.get('custCode')
        json_data = cu.get_po_template(cust_code)
        return make_response(jsonify(json_data), 200)


@app.route('/get_po_data', methods=['GET', 'POST'])
def r_get_po_data():
    if request.method == 'POST':
        po_file = request.files.get('poFile')
        po_header = {}
        po_header['user_name'] = request.values.get('userName')
        po_header['cust_code'] = request.values.get('custCode')
        po_header['po_type'] = request.values.get('poType')
        po_header['bonded_type'] = request.values.get('bondedType')
        po_header['offer_sheet'] = request.values.get('offerSheet')
        po_header['need_delay'] = request.values.get('needDelay')
        po_header['delay_days'] = request.values.get('delayDays')
        po_header['need_mail_tip'] = request.values.get('needMailTip')
        po_header['mail_tip'] = request.values.get('mailTip')
        po_header['po_level'] = request.values.get('poLevel')
        po_header['file_name'] = request.values.get('poFileName')
        po_header['template_sn'] = request.values.get('poTemplateSN')
        po_header['err_desc'] = ''

        po_data = get_po_data(po_file, po_header)
        if not po_data:
            return make_response(jsonify({'err_desc': po_header['err_desc'], 'status': 201}))
        else:
            return make_response(jsonify({'header': po_header, 'items': po_data, 'status': 200}))


@app.route('/upload_po_template', methods=['GET', 'POST'])
def r_upload_po_template():
    if request.method == 'POST':
        po_original_file = request.files.get('originalFile')  # 原始文件
        po_compare_file = request.files.get('compareFile')  # 对照文件
        po_pic_file = request.files.get('picFile')  # 截图文件
        po_header = {}
        po_header['user_name'] = request.values.get('userName')
        po_header['cust_code'] = request.values.get('custCode')
        po_header['template_name'] = request.values.get('templateName')
        po_header['template_type'] = request.values.get('type')

        cpt.get_file(po_original_file, po_compare_file, po_pic_file, po_header)
        return make_response("success", 200)


@app.route('/create_so', methods=['POST'])
def r_create_so():
    if request.method == 'POST':
        so_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        so_data['header'] = request_data.get('header')
        so_data['items'] = request_data.get('items')

        ret = []
        ret = cs.create_so(so_data)
        # cw.create_wo(so_data)

        print("返回给前台：", ret)
        return make_response(jsonify({"info": ret}), 200)


@app.route('/get_mo_lot_items', methods=['GET'])
def r_get_mo_lot_items():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cust_code'] = request.args.get('custCode')
        mo_query['product_name_type'] = request.args.get('productNameType')
        mo_query['product_name'] = request.args.get('productName')

        ret = cm.get_mo_lot_items(mo_query)
        return make_response(jsonify({"info": ret}), 200)


@app.route('/create_mo', methods=['POST'])
def r_create_mo():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        ret = cm.create_mo(mo_data)
        if ret:
            return make_response("success", 200)
        else:
            return make_response("创建失败", 201)


@app.route('/query_po_data', methods=['GET', 'POST'])
def r_query_po_data():
    if request.method == 'GET':
        po_query = {}
        po_query['cust_code'] = request.args.get('custCode')
        po_query['cust_lot_id'] = request.args.get('custLotID')
        po_query['upload_id'] = request.args.get('uploadID')

        ret = cu.query_po_data(po_query)

        return make_response(jsonify({"info": ret}), 200)


@app.route('/delete_po_data', methods=['GET', 'POST'])
def r_delete_po_data():
    if request.method == 'GET':
        po_del = {}
        po_del['upload_id'] = request.args.get('uploadID')
        po_del['cust_lot_id'] = request.args.get('custLotID')

        cu.delete_po_data(po_del)

        return make_response(jsonify({"info": "success"}), 200)


@app.route('/get_product_grossdie', methods=['GET'])
def r_get_product_grossdie():
    if request.method == 'GET':
        mo_query = {}
        mo_query['cust_code'] = request.args.get('custCode')
        mo_query['product_name'] = request.args.get('productName')

        gross_dies = cm.get_product_grossdie(mo_query)
        return make_response(jsonify({"gross_dies": gross_dies}), 200)


@app.route('/create_mo_2', methods=['POST'])
def r_create_mo_2():
    if request.method == 'POST':
        mo_data = {}
        request_data = json.loads(request.get_data(as_text=True))
        mo_data['header'] = request_data.get('header')
        mo_data['items'] = request_data.get('items')

        ret = cm.create_mo_2(mo_data)
        if ret:
            return make_response("success", 200)
        else:
            return make_response("生成失败", 201)


@app.route('/get_product_data', methods=['GET'])
def r_get_product_data():
    if request.method == 'GET':
        product_name = request.args.get('product_name')

        product_data = cu.get_product_data(product_name)
        return make_response(jsonify({"product_data": product_data}), 200)


server = WSGIServer(('', 5025), app)
server.start()


def serve_forever():
    server.start_accepting()
    server._stop_event.wait()


if __name__ == "__main__":
    for i in range(cpu_count()):
        p = Process(target=serve_forever)
        p.start()
