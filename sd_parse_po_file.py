import os
import re
from sd_parse_gen_xl import parse_gen_xl
from flask import abort, make_response
import sd_parse_special_po_xl as p_SPEC
import sd_parse_FO_xl as p_FO


# 解析订单文件
def parse_po_file(po_file, po_header):
    # 保存文件
    save_po_file(po_file, po_header)

    # 厂内通用解析
    if po_header['common_checked'] == "true":
        po_data = parse_gen_xl(po_header)
        return po_data

    # US008客制
    if po_header['cust_code'] in ('US008'):
        po_data = p_SPEC.parse_US008_TSV_FT(po_header)

    # HK005客制
    elif po_header['cust_code'] in ("HK005"):
        if po_header['template_desc'] == 'HK订单':
            po_data = p_SPEC.parse_HK005_HK(po_header)

    # TW039客制
    elif po_header['cust_code'] in ("TW039"):
        if po_header['template_desc'] == 'BUMPING订单':
            po_data = p_SPEC.parse_TW039_BUMPING(po_header)

        elif po_header['template_desc'] == 'FT订单':
            po_data = p_SPEC.parse_TW039_FT(po_header)

    # SH192客制
    elif po_header['cust_code'] in ("SH192", "JS251"):
        if po_header['template_desc'] == 'BUMPING模板':
            po_data = p_SPEC.parse_SH192_BUMPING(po_header)

    # HW50客制
    elif po_header['cust_code'] in ("HW50"):
        if po_header['template_desc'] == 'BUMPING正式模板':
            po_data = p_SPEC.parse_HW50_BUMPING(po_header)

        elif po_header['template_desc'] == 'FC正式模板':
            po_data = p_SPEC.parse_HW50_FC(po_header)

    # FO--------------------------模板
    elif po_header['cust_code'] in ('HK098', 'BJ178', 'SZ217'):
        if 'FO' in po_header['template_desc']:
            po_data = p_FO.parse_HK098_FO(po_header)
        elif 'WLP' in po_header['template_desc']:
            po_data = p_SPEC.parse_HK098_WLP(po_header)

    elif po_header['cust_code'] in ('GD224'):
        if 'FO' in po_header['template_desc']:
            po_data = p_FO.parse_GD224_FO(po_header)

    elif po_header['cust_code'] in ('AB31'):
        if 'FO' in po_header['template_desc']:
            po_data = p_FO.parse_AB31_FO(po_header)

    elif po_header['cust_code'] in ('SH104'):
        if 'FO' in po_header['template_desc']:
            po_data = p_FO.parse_SH104_FO(po_header)
        elif 'WLP' in po_header['template_desc']:
            po_data = p_SPEC.parse_SH104_WLP(po_header)
    else:
        # 客户模板通用解析
        po_data = parse_gen_xl(po_header)

    return po_data


# 保存订单文件
def save_po_file(po_file, po_header):
    # 文件目录
    doc_dir = os.path.join(os.getcwd(), 'docs/')
    if not os.path.exists(doc_dir):
        os.makedirs(doc_dir)

    # 文件名
    doc_file_name = po_file.filename
    doc_path = get_doc_path(doc_dir=doc_dir, doc_file_name=doc_file_name)
    try:
        po_file.save(doc_path)
    except Exception as e:
        abort(make_response({"ERR_MSG": "文件保存失败"}))

    po_header['file_path'] = doc_path


# 获取文件完整的路径
def get_doc_path(doc_dir, doc_file_name):
    doc_path = os.path.join(doc_dir, doc_file_name)
    directory, file_name = os.path.split(doc_path)
    while os.path.isfile(doc_path):
        pattern = '(\d+)\)\.'
        file_name = file_name.replace(")", "")
        if re.search(pattern, file_name) is None:
            file_name = file_name.replace('.', '(0).')
        else:
            current_number = int(re.findall(pattern, file_name)[-1])
            new_number = current_number + 1
            file_name = file_name.replace(
                f'({current_number}).', f'({new_number}).')
        doc_path = os.path.join(directory + os.sep + file_name)

    return doc_path
