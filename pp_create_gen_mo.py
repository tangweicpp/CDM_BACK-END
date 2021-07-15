from flask import abort, make_response
from com_unit import get_rand_id
import conn_db as conn
import time
import json
import com_ws as cw
from pp_get_mo_attrs import get_mo_header_level_attributes
from pp_get_mo_attrs import get_mo_wafer_level_attributes
from web_api_client import get_data_from_web_api
import rfc
import set_mark_code as smc
import uuid


def xstr(s):
    return '' if s is None else str(s).strip()


def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]


def get_so_action(con, header):
    action = "C"
    header_no = "b670dd8d0"
    # return action, header_no

    sql = f"SELECT SO_SN FROM ZM_CDM_SO_HEADER WHERE PO_NO='{header['BSTKD']}' AND PO_TYPE='{header['AUART']}' AND CUST_CODE = '{header['KUNNR']}' AND SO_NO IS NOT NULL"
    results = con.query(sql)
    if results:
        action = "C"
        header_no = xstr(results[0][0])

    else:
        action = "N"
        header_no = get_rand_id(8)

        # 新建SO表头记录
        sql = f'''INSERT INTO ZM_CDM_SO_HEADER(PO_NO,PO_TYPE,SO_SN,SO_CREATE_BY,SO_CREATE_DATE,CUST_CODE,FLAG)
            values('{header['BSTKD']}','{header['AUART']}','{header_no}','{header['CREATER']}',NOW(),'{header['KUNNR']}','1') '''

        con.exec_n(sql)

    return action, header_no


# 创建销售订单
def create_so(con, wafer_sn_agg, po_type_new):
    so_data_list = {'SO_DATA': []}

    # header
    sql = f"""SELECT PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY,String_agg(WAFER_SN ,''',''') FROM ZM_CDM_PO_ITEM WHERE wafer_sn in ('{wafer_sn_agg}')
            GROUP BY PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY  """

    results = con.query(sql)
    for row in results:
        so_data = {'HEADER': {}, 'ITEM': []}

        if po_type_new:
            po_type = po_type_new
        else:
            po_type = xstr(row[0])
            po_type = 'ZOR3'

        sap_cust_code = xstr(row[1])
        trad_cust_code = xstr(row[2])
        po_id = xstr(row[3])
        creater = xstr(row[4])
        wafer_sn_list = xstr(row[5])

        header = {}
        so_data['HEADER'] = header
        header['AUART'] = po_type
        header['KUNNR'] = sap_cust_code
        header['KUNRE'] = trad_cust_code
        header['BSTKD'] = po_id
        header['CREATER'] = creater
        # header['UPLOAD_ID'] = upload_id
        header['ACTION'], header['HEAD_NO'] = get_so_action(con, header)

        # items
        sql = f"""SELECT SAP_PRODUCT_PN,sum(PASSBIN_COUNT+FAILBIN_COUNT),CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN,WAFER_PCS_PRICE,WAFER_DIE_PRICE,ADDRESS_CODE,count(1),REMARK1,String_agg(WAFER_SN ,''','''),PO_DATE,REMAKR4,REMAKR5
            FROM ZM_CDM_PO_ITEM WHERE WAFER_SN IN ('{wafer_sn_list}')
            GROUP BY SAP_PRODUCT_PN,CUSTOMER_DEVICE,FAB_DEVICE,PRODUCT_PN,WAFER_PCS_PRICE,WAFER_DIE_PRICE,ADDRESS_CODE,REMARK1,PO_DATE,REMAKR4,REMAKR5 """

        results2 = con.query(sql)
        for row in results2:
            sap_product_pn = xstr(row[0])
            gross_dies = row[1]
            cust_device = xstr(row[2])
            fab_device = xstr(row[3])
            product_pn = xstr(row[4])
            wafer_pcs_price = xstr(row[5])
            wafer_die_price = xstr(row[6])
            address_code = xstr(row[7])
            wafer_pcs = row[8]
            po_item = xstr(row[9])
            wafer_sn_list_2 = xstr(row[10])

            if len(xstr(row[11])) == 8:
                po_date = xstr(row[11])
            elif len(xstr(row[11])) > 8:
                a = xstr(row[11])
                po_date = a[:4] + a[5:7] + a[8:10]
            else:
                po_date = time.strftime('%Y%m%d')

            item = {}
            item['ACTION'] = 'N'
            item['ITEM_NO'] = get_rand_id(6)
            item['BSTDK'] = po_date
            item['BNAME'] = header['CREATER']
            item['MATNR'] = sap_product_pn
            item['KWMENG'] = gross_dies
            item['ZCUST_DEVICE'] = cust_device
            item['ZFAB_DEVICE'] = fab_device
            item['POSEX'] = po_item
            item['VBELN'] = xstr(row[12])  # 参考退货单号
            item['POSNR'] = xstr(row[13])  # 参考退货行号
            item['INCO1'] = ''
            item['INCO2'] = ''
            item['ZZDZDM'] = address_code

            sql = f'''INSERT INTO ZM_CDM_SO_ITEM(SO_SN,CDM_ITEM_SN,PRD_ID,QTY,CREATE_BY,CREATE_DATE,FLAG,SAP_PRD_ID)
                values('{header['HEAD_NO']}','{item['ITEM_NO']}','{product_pn}','{item['KWMENG']}',
                '{item['BNAME']}',now(),'0','{item['MATNR']}')
                '''
            if not con.exec_n(sql):
                con.db.rollback()

            # -----------------------------waferlist-------------------------------
            item['WAFER_LIST'] = []
            sql = f""" SELECT LOT_ID,WAFER_ID,PASSBIN_COUNT,FAILBIN_COUNT,WAFER_SN,WAFER_HOLD,LOT_WAFER_ID FROM ZM_CDM_PO_ITEM
                WHERE WAFER_SN IN ('{wafer_sn_list_2}') ORDER BY LOT_ID,WAFER_ID """

            results3 = con.query(sql)
            for row3 in results3:
                lot_id = xstr(row3[0])
                wafer_id = xstr(row3[1])
                lot_wafer_id = xstr(row3[6])
                wafer_sn = xstr(row3[4])
                wafer_good_dies = row3[2]
                wafer_ng_dies = row3[3]
                wafer_gross_dies = wafer_good_dies + wafer_ng_dies

                wafer = {}
                wafer['ACTION'] = 'N'
                wafer['ZFAB_DEVICE'] = fab_device
                wafer['ZCUST_DEVICE'] = cust_device
                wafer['ZCUST_LOT'] = lot_id
                wafer['ZCUST_WAFER_ID'] = lot_wafer_id
                wafer['ZGOODDIE_QTY'] = wafer_good_dies
                wafer['ZBADDIE_QTY'] = wafer_ng_dies
                wafer['ZGROSSDIE_QTY'] = wafer_gross_dies

                sql = f"""INSERT INTO ZM_CDM_SO_SUB_ITEM(ITEM_SN,WAFER_SN,CUST_LOT_ID,CUST_WAFER_ID,CUST_LOTWAFER_ID,GOOD_DIES,NG_DIES,FLAG,REMARK1,REMARK2)
                values('{item['ITEM_NO']}','{wafer_sn}','{lot_id}','{wafer_id}','{lot_wafer_id}',
                '{wafer_good_dies}','{wafer_ng_dies}','0','','')
                """
                if not con.exec_n(sql):
                    con.db.rollback()

                item['WAFER_LIST'].append(wafer)

            so_data['ITEM'].append(item)

        so_data_list['SO_DATA'].append(so_data)

    print(so_data_list)
    # 发送数据
    res = get_data_from_web_api("SD017", so_data_list)
    print(res['RES_DATA_D'])
    return_node = res['RES_DATA_D']['RETURN'][0]
    err_msg = return_node.get('MESSAGE')
    if err_msg != "成功":
        abort(make_response({"ERR_MSG": err_msg}))

    so_id = str(return_node.get('VBELN', ''))
    so_item = str(return_node.get('POSNR', ''))

    # 更新订单
    sql = f"update zm_cdm_po_item set so_id = '{so_id}', so_item='{so_item}',UPDATE_BY='已更新SO',UPDATE_DATE=NOW() where wafer_sn in ('{wafer_sn_agg}') "
    print(sql)
    con.exec_n(sql)
    return so_id, so_item


# 判断SO ,ITEM
def check_so_item(so_id, so_item, c_matnr, c_po):
    c_matnr = ("00000000000000"+c_matnr)[-18:]
    con_dw = conn.HanaConnDW()
    # sql = f"SELECT MATNR FROM VBAP WHERE VBELN = '{so_id}' AND POSNR = '{so_item}' "
    sql = f"""SELECT a.BSTNK, b.MATNR FROM VBAK a
    INNER JOIN VBAP b ON a.VBELN = b.VBELN 
    WHERE b.VBELN = '{so_id}' AND b.POSNR = '{so_item}'
    """

    results = con_dw.query(sql)
    if results:
        # 真实的料号
        r_po = xstr(results[0][0])
        r_matnr = xstr(results[0][1])

        # PO检查
        if not r_po in c_po:
            abort(make_response(
                {"ERR_DESC": f"SO: {so_id}, ITEM: {so_item} 对应客户PO为{r_po}, 和当前工单的PO{c_po}不一致"}))

        # 物料检查
        if r_matnr != c_matnr:
            abort(make_response(
                {"ERR_DESC": f"SO: {so_id}, ITEM: {so_item} 对应料号为{r_matnr}, 和当前工单的料号{c_matnr}不一致"}))

        print("检查通过")
    else:
        pass
        # abort(make_response(
        #     {"ERR_DESC": f"您输入的SO:{so_id} SO_ITEM:{so_item}, 查询不到"}))


# RFC查询SO ,ITEM
def check_so_item_by_rfc(so_id, so_item, c_matnr, c_po):
    # SO长度检查10位固定
    if len(so_id) != 10:
        abort(make_response(
            {"ERR_DESC": f"SO:{so_id},长度异常(正确10位),请确认SO是否错误"}))

    c_matnr = ("00000000000000"+c_matnr)[-18:]
    r_po, r_matnr = rfc.get_so_info(so_id, so_item)

    # SO检查
    if not (r_po and r_matnr):
        abort(make_response(
            {"ERR_DESC": f"SO:{so_id},SO_ITEM:{so_item}找不到SAP数据,请确认SO是否错误"}))

    # PO检查
    if not r_po in c_po:
        abort(make_response(
            {"ERR_DESC": f"SO: {so_id}, ITEM: {so_item} 对应客户PO为{r_po}, 和当前工单的PO:{c_po}不一致"}))

    # 物料检查
    if r_matnr != c_matnr:
        abort(make_response(
            {"ERR_DESC": f"SO: {so_id}, ITEM: {so_item} 对应料号为{r_matnr}, 和当前工单的料号:{c_matnr}不一致"}))

    print("检查通过:", so_id, so_item, c_matnr, r_matnr, c_po, r_po)


# 判断重工产生什么类型的SO
def check_rep_so(con, mo_header, mo_items):
    base_so_flag = mo_header['base_so']
    fhfs = mo_header['fhfs']

    for item in mo_items:
        wafer_sn_agg = ""
        wafer_list = item['waferList']
        for wafer in wafer_list:
            wafer_sn = wafer['waferSN']
            wafer_sn_agg = wafer_sn_agg + wafer_sn + "','"
        wafer_sn_agg = wafer_sn_agg[:-3]

        if base_so_flag == 'Y':
            # 判断是否需要新增
            sql = f"select * from zm_cdm_po_item where wafer_sn in ('{wafer_sn_agg}') and update_by = '已更新SO' "
            results = con.query(sql)
            if not results:
                # 基于SO
                if "入库待发" in fhfs:
                    # 建立免费的SO
                    item['soID'], item['soItem'] = create_so(
                        con, wafer_sn_agg, "ZOR4")

                elif "入库即发" in fhfs:
                    # 建立新的收费的SO
                    item['soID'], item['soItem'] = create_so(
                        con, wafer_sn_agg, "")


# 创建工单
def create_mo(mo_data):
    print(mo_data)

    # 数据库业务开启
    con = conn.HanaConn()
    con_dw = conn.HanaConnDW()
    con_or = conn.OracleConn()

    # 工单请求数据
    mo_response = {"ERR_DESC": "", "ITEM_LIST": []}
    # 工单号list
    mo_id_list = []
    # 一票工单设一个ID
    mo_create_id = get_rand_id(8)

    # FC检查引线框架数量
    if mo_data['header']['process'] in ('FC', 'FC+FT'):
        check_frame_inventory(mo_data)

    # 厂内重工
    if mo_data['header']['moType'] == "YP03":
        check_rep_so(con, mo_data['header'], mo_data['items'])

    # 多工单循环执行
    for item in mo_data['items']:
        mo_request = {'MO_DATA': []}
        header_node = {'HEADER': {}, 'ITEM': []}

        # 工单表头层级
        # DC工单-> YP05-> E
        if mo_data['header']['moType'] == 'YP10':
            header_node['HEADER']['LOT_TYPE'] = 'YP05'

        # 硅基工单-> YP01-> Q
        elif mo_data['header']['moType'] == 'YP12':
            header_node['HEADER']['LOT_TYPE'] = 'YP01'

        # CSP-> YP01->Q
        elif mo_data['header']['moType'] == 'YP13':
            # 量产
            if mo_data['header']['moPrefix'][1:2] == 'T':
                header_node['HEADER']['LOT_TYPE'] = 'YP02'
            # 样品
            elif mo_data['header']['moPrefix'][1:2] == 'S':
                header_node['HEADER']['LOT_TYPE'] = 'YP01'
            else:
                header_node['HEADER']['LOT_TYPE'] = 'YP02'

        else:
            header_node['HEADER']['LOT_TYPE'] = mo_data['header']['moType']

        # 工单前缀检查
        # 样品订单=>样品工单
        if header_node['HEADER']['LOT_TYPE'] == "YP01" and mo_data['header']['moPrefix'][1:2] != 'S' and mo_data['header']['moType'][:3] != 'YP1':
            abort(make_response(
                {"ERR_DESC": f"NPI维护的是样品订单, 工单前缀第二位必须为S,当前工单前缀第二位是:{mo_data['header']['moPrefix'][1:2]}"}))

        # 重工
        if header_node['HEADER']['LOT_TYPE'] in ("YP03", "YP04") and mo_data['header']['moPrefix'][1:2] != 'R':
            abort(make_response(
                {"ERR_DESC": f"重工工单前缀第二位必须为R,当前工单前缀第二位是:{mo_data['header']['moPrefix'][1:2]}"}))

        # 获取工单号
        mo_id = get_mo_id(con, mo_data['header']['moPrefix'])
        mo_id_list.append(mo_id)
        # 其他属性
        header_node['HEADER']['SHOP_ORDER'] = mo_id
        header_node['HEADER']['PRD_ID'] = mo_data['header']['productName2']
        header_node['HEADER']['SAP_PRD_ID'] = mo_data['header']['sapProductName']
        header_node['HEADER']['ORDER_QTY'] = item['moWaferQty']
        header_node['HEADER']['CUST_LOT_QTY'] = 1
        header_node['HEADER']['PLAN_START_DATE'] = item['startDate'].replace(
            '日期:', '')
        header_node['HEADER']['PLAN_END_DATE'] = item['endDate'].replace(
            '日期:', '')
        header_node['HEADER']['PRIORITY'] = mo_data['header']['moPriority']
        header_node['HEADER']['CUST_ID'] = mo_data['header']['custCode']
        header_node['HEADER']['CREATOR'] = mo_data['header']['userName']
        header_node['HEADER']['PO'] = item['poID']
        header_node['HEADER']['PO_ITEM'] = item['poItem']
        header_node['HEADER']['SAP_SO'] = item['soID']
        header_node['HEADER']['SAP_SO_ITEM'] = item['soItem']
        header_node['HEADER']['ZZBASESOMO'] = 'Y' if header_node['HEADER']['SAP_SO'] else 'N'
        header_node['HEADER']['DC'] = time.strftime('%y%W')
        header_node['HEADER']['ACTION_ID'] = 'N'

        # 检查SO ITEM
        if header_node['HEADER']['SAP_SO']:
            check_so_item_by_rfc(header_node['HEADER']['SAP_SO'], header_node['HEADER']
                                 ['SAP_SO_ITEM'],  header_node['HEADER']['SAP_PRD_ID'], header_node['HEADER']['PO'])

        # 插入工单头表
        sql = f"""insert into ZM_CDM_MO_HEADER(MO_TYPE, MO_ID, CUST_CODE, CUSTOMER_DEVICE, HT_PN, PRODUCT_PN, SAP_PRODUCT_PN, FAB_DEVICE, WAFER_PN, PLAN_START_DATE, PLAN_END_DATE, MO_PRIORITY, MO_DC,
        LOT_QTY, WAFER_QTY, DIE_QTY, PO_ID, PO_ITEM, SO_ID, SO_ITEM, CREATE_BY, CREATE_DATE, FLAG, REMARK1,ID,FIRST_COMMIT_DATE) VALUES('{header_node['HEADER']['LOT_TYPE']}', '{header_node['HEADER']['SHOP_ORDER']}', '{header_node['HEADER']['CUST_ID']}',
        '{mo_data['header']['custPN']}', '{mo_data['header']['htPN']}', '{header_node['HEADER']['PRD_ID']}', '{header_node['HEADER']['SAP_PRD_ID']}', '', '{mo_create_id}', '{header_node['HEADER']['PLAN_START_DATE']}',
        '{header_node['HEADER']['PLAN_END_DATE']}', '{header_node['HEADER']['PRIORITY']}', '{header_node['HEADER']['DC']}', '{header_node['HEADER']['CUST_LOT_QTY']}', '{header_node['HEADER']['ORDER_QTY']}', '',
        '{header_node['HEADER']['PO']}', '{header_node['HEADER']['PO_ITEM']}', '{header_node['HEADER']['SAP_SO']}', '{header_node['HEADER']['SAP_SO_ITEM']}', '{header_node['HEADER']['CREATOR']}', NOW(), '1', '{mo_data['header']['custRework']}',ZM_CDM_MO_HEADER_SEQ.NEXTVAL,'{header_node['HEADER']['PLAN_END_DATE']}')
        """

        if not con.exec_n(sql):
            con.db.rollback()
            mo_response['ERR_DESC'] = "工单头表插入失败"
            return mo_response

        # 工单wafer层级
        for wafer in item['waferList']:
            # 保税非保和库存校验
            if mo_data['header']['moType'] in ('YP01', 'YP02') and not wafer['zzmylx'] and item['lotProprity'] != '1':
                mo_response['ERR_DESC'] = "客户库存必须维护贸易类型"
                return mo_response

            # 保税非保和库存校验
            if wafer['zzmylx'] and wafer['zzmylx'] != mo_data['header']['moPrefix'][:1]:
                mo_response['ERR_DESC'] = f"库存贸易类型:{wafer['zzmylx']},工单第一位:{mo_data['header']['moPrefix'][:1]},两者必须一致"
                return mo_response

            # 打标码
            if mo_data['header']['moType'][:3] == "YP0":
                # 判断是否是工单打标码,更新打标码
                if smc.check_mo_mark_code(mo_data['header']['htPN']):
                    # 工单打标码
                    err_msg = smc.set_marking_code(
                        wafer_sn=wafer['waferSN'], mo_id=mo_id)
                    if err_msg:
                        con.db.rollback()
                        mo_response['ERR_DESC'] = err_msg
                        return mo_response

                else:
                    # 再次获取订单打标码
                    if mo_data['header']['markCodeRule'] and (len(wafer['markCode']) != len(mo_data['header']['markCodeRule'])):
                        err_msg = smc.set_marking_code_po(
                            mo_data['header']['htPN'], wafer['waferSN'])
                        if err_msg:
                            con.db.rollback()
                            mo_response['ERR_DESC'] = err_msg
                            return mo_response

            # 获取WAFER层级属性
            wafer_attrs = get_mo_wafer_level_attributes(con, wafer['waferSN'])
            if isinstance(wafer_attrs, str):
                mo_response['ERR_DESC'] = wafer_attrs
                con.db.rollback()
                return mo_response

            mo_wafer_attrs = []
            for key, value in wafer_attrs.items():
                attr = {}
                attr['NAME'] = key
                attr['VALUE'] = value
                mo_wafer_attrs.append(attr)

            # 扣账主材料号
            attr = {}
            attr['NAME'] = "WAFER_MAT_NO"
            attr['VALUE'] = wafer.get('waferPartNo', '')
            mo_wafer_attrs.append(attr)

            # GROSSDIES
            attr = {}
            attr['NAME'] = "PRODUCT_GROSS_DIES"
            attr['VALUE'] = wafer.get(
                'productGrossDies', '')
            mo_wafer_attrs.append(attr)

            # FAB LOT
            attr = {}
            attr['NAME'] = "FAB_LOT_ID"
            attr['VALUE'] = wafer.get(
                'fabLotID', '') + wafer['waferID'] if wafer.get('fabLotID', '') else ''
            mo_wafer_attrs.append(attr)

            wafer_node = {}
            wafer_node['WAFER_PROPERTY'] = mo_wafer_attrs
            wafer_node['WAFER_ID'] = wafer['lotWaferID']
            wafer_node['GROSS_DIE'] = str(
                int(wafer['goodDies']) + int(wafer['ngDies']))
            wafer_node['GOOD_DIE'] = wafer['goodDies']
            wafer_node['ISSUE_DIE'] = wafer_node['GROSS_DIE']
            wafer_node['CUST_LOT_ID'] = wafer['lotID']
            wafer_node['WAFER_SN'] = wafer['waferSN']
            wafer_node['MARK_CODE'] = wafer_attrs.get('MARKING_CODE', '')
            wafer_node['FAB_LOT_ID'] = wafer.get(
                'fabLotID', '') + wafer['waferID'] if wafer.get('fabLotID', '') else ''

            # map检查
            if mo_data['header']['custCode'] in ('KR001', 'KR009') and header_node['HEADER']['LOT_TYPE'] in ("YP01", "YP02"):
                if wafer['ngDies'] == 0 or wafer['ngDies'] == '0':
                    mo_response['ERR_DESC'] = "KR001,KR009没有map数据更新,请联系内勤上传map文件"
                    con.db.rollback()
                    return mo_response

            # 判断是否需要切割订单,供下次用
            if ("FC" in mo_data['header']['process'] or "FT" in mo_data['header']['process']) and (int(wafer['goodDies']) < int(wafer['queryGoodDies'])) and (int(wafer['goodDies']) != int(wafer['inventoryGoodDies'])):
                split_po_data(
                    con, wafer['waferSN'], wafer['goodDies'], wafer['queryGoodDies'])

            # 插入工单明细表
            sql = f""" insert into ZM_CDM_MO_ITEM(MO_ID, MO_ITEM, LOT_ID, WAFER_ID, LOT_WAFER_ID, WAFER_SN, GROSS_DIE_QTY, GOOD_DIE_QTY, NG_DIE_QTY, MARK_CODE, FLAG, ID,FLAG2)
            VALUES('{header_node['HEADER']['SHOP_ORDER']}', '{mo_create_id}', '{wafer_node['CUST_LOT_ID']}', '{wafer['waferID']}', '{wafer_node['WAFER_ID']}', '{wafer['waferSN']}',
            '{wafer_node['GROSS_DIE']}', '{wafer['goodDies']}', '{wafer['ngDies']}', '{wafer_node['MARK_CODE']}', '1',ZM_CDM_MO_ITEM_SEQ.NEXTVAL ,'1')
            """

            if not con.exec_n(sql):
                con.db.rollback()
                mo_response['ERR_DESC'] = "工单明细表插入失败"
                return mo_response

            # 库存二次检查
            if mo_data['header']['moType'] in ("YP01", "YP02") and item['lotProprity'] != "1":
                if not 'WAFER_INV_ITEMS' in wafer:
                    mo_response['ERR_DESC'] = "样品量产订单库存不可为空"
                    return mo_response

            # 库存信息保存
            if 'WAFER_INV_ITEMS' in wafer:
                update_wafer_inv_items(con, con_dw, wafer, mo_id)

            # 更新订单明细表
            sql = f"update ZM_CDM_PO_ITEM set flag2='1',MO_ID='{mo_id}',MO_ITEM='{mo_create_id}',UPDATE_DATE=NOW() WHERE WAFER_SN = '{wafer['waferSN']}' and flag2='0' "
            if not con.exec_n(sql):
                con.db.rollback()
                mo_response['ERR_DESC'] = "订单明细表更新工单信息失败"
                return mo_response

            header_node['ITEM'].append(wafer_node)

        # 获取工单层级属性
        mo_attrs = get_mo_header_level_attributes(
            con, con_or, header_node['HEADER']['SHOP_ORDER'], mo_data['header']['processKey'])
        if isinstance(mo_attrs, str):
            con.db.rollback()
            mo_response['ERR_DESC'] = mo_attrs
            return mo_response

        mo_header_attrs = []
        for key, value in mo_attrs.items():
            attr = {}
            attr['NAME'] = key
            attr['VALUE'] = value
            mo_header_attrs.append(attr)

        header_node['HEADER']['EXTRA_PROPERTY'] = mo_header_attrs
        mo_request['MO_DATA'].append(header_node)

        # 保存请求报文
        save_mo_request_json(con, mo_id, header_node)

        # 发送工单请求
        send_mo_request(con, mo_request, mo_id, mo_response)

    # 获取返回结果
    if get_mo_status(con, mo_id_list, mo_response):
        # 事务提交
        con.db.commit()
        con_dw.db.commit()
    else:
        # 事务回滚
        con.db.rollback()
        con_dw.db.rollback()

    return mo_response


# 切割订单,以备下次用
def split_po_data(con, wafer_sn, goodDies, queryGoodDies):
    # 更新当前片
    sql = f"UPDATE ZM_CDM_PO_ITEM set PASSBIN_COUNT = {int(goodDies)},UPDATE_DATE=now(),UPDATE_BY='FC小批量投单' where wafer_sn = '{wafer_sn}' "
    con.exec_n(sql)
    # 剩余量新增
    curGoodDies = int(queryGoodDies) - int(goodDies)
    # 备份当前行数据
    sql = f"INSERT INTO ZM_CDM_PO_ITEM_DELETE SELECT * FROM ZM_CDM_PO_ITEM WHERE WAFER_SN = '{wafer_sn}' "
    con.exec_n(sql)
    wafer_sn_new = wafer_sn + '+'

    # 更新临时表数据
    sql = f"UPDATE ZM_CDM_PO_ITEM_DELETE SET flag2= '0',MO_ID ='',MO_ITEM ='',FLAG3 =FLAG3 || '+',PASSBIN_COUNT={curGoodDies}, WAFER_SN = '{wafer_sn_new}' WHERE WAFER_SN = '{wafer_sn}' "
    con.exec_n(sql)

    # 反插入订单数据
    sql = f"INSERT INTO ZM_CDM_PO_ITEM SELECT * FROM ZM_CDM_PO_ITEM_DELETE WHERE WAFER_SN = '{wafer_sn_new}'"
    con.exec_n(sql)

    # 删除临时数据
    sql = f"delete from ZM_CDM_PO_ITEM_DELETE where wafer_sn = '{wafer_sn_new}'"
    con.exec_n(sql)


# 更新晶圆库存分布
def update_wafer_inv_items(con, con_dw, wafer, mo_id):
    wafer_inv_items = wafer['WAFER_INV_ITEMS']
    wafer_mo_dies = int(wafer['goodDies']) + int(wafer['ngDies'])
    wafer_invs = []

    # 1.遍历数组, 如果有一箱的数量等于工单数量那么就直接锁定这一箱
    for item in wafer_inv_items:
        if item['ZDIE_QTY_RM'] >= wafer_mo_dies:
            wafer_inv = {}
            wafer_inv['WERKS'] = item['WERKS']
            wafer_inv['CHARG'] = item['CHARG']
            wafer_inv['LGORT'] = item['LGORT']
            wafer_inv['MATNR'] = item['MATNR']
            wafer_inv['ZBIN_NO'] = item['ZBIN_NO']
            wafer_inv['ZOUT_BOX'] = item['ZOUT_BOX']
            wafer_inv['ZSEQ'] = item['ZSEQ']
            wafer_inv['ZWAFER_LOT'] = item['ZWAFER_LOT']
            wafer_inv['ZWAFER_ID'] = item['ZWAFER_ID']
            # wafer_inv['ZDIE_QTY_GI'] = item['ZDIE_QTY_RM']
            wafer_inv['ZDIE_QTY_GI'] = wafer_mo_dies
            wafer_invs.append(wafer_inv)

            # 更新工单表
            update_mo_wafer_invs(con, wafer, wafer_invs)
            # 更新库存指向
            update_inventory_pointer(con, con_dw, wafer, wafer_invs, mo_id)

            return True

    # 2.累加数量, 如果良品相加数量小于或刚好等于则锁定这几项
    qtys = 0  # 当前待累加量
    qtys2 = 0  # 已累加量

    for item in wafer_inv_items:
        # 累加当前行
        qtys = qtys + item['ZDIE_QTY_RM']

        if qtys <= wafer_mo_dies:
            wafer_inv = {}
            wafer_inv['WERKS'] = item['WERKS']
            wafer_inv['CHARG'] = item['CHARG']
            wafer_inv['LGORT'] = item['LGORT']
            wafer_inv['MATNR'] = item['MATNR']
            wafer_inv['ZBIN_NO'] = item['ZBIN_NO']
            wafer_inv['ZOUT_BOX'] = item['ZOUT_BOX']
            wafer_inv['ZSEQ'] = item['ZSEQ']
            wafer_inv['ZWAFER_LOT'] = item['ZWAFER_LOT']
            wafer_inv['ZWAFER_ID'] = wafer['lotWaferID']
            wafer_inv['ZDIE_QTY_GI'] = item['ZDIE_QTY_RM']
            qtys2 = qtys2 + item['ZDIE_QTY_RM']
            wafer_invs.append(wafer_inv)
        else:
            wafer_inv = {}
            wafer_inv['WERKS'] = item['WERKS']
            wafer_inv['CHARG'] = item['CHARG']
            wafer_inv['LGORT'] = item['LGORT']
            wafer_inv['MATNR'] = item['MATNR']
            wafer_inv['ZBIN_NO'] = item['ZBIN_NO']
            wafer_inv['ZOUT_BOX'] = item['ZOUT_BOX']
            wafer_inv['ZSEQ'] = item['ZSEQ']
            wafer_inv['ZWAFER_LOT'] = item['ZWAFER_LOT']
            wafer_inv['ZWAFER_ID'] = wafer['lotWaferID']
            wafer_inv['ZDIE_QTY_GI'] = wafer_mo_dies - qtys2
            wafer_invs.append(wafer_inv)

    # 更新工单表
    if wafer_invs:
        update_mo_wafer_invs(con, wafer, wafer_invs)
        update_inventory_pointer(con, con_dw, wafer, wafer_invs, mo_id)
        return True


# 更新库存指向
def update_inventory_pointer(con, con_dw, wafer, wafer_invs, mo_id):
    for row in wafer_invs:
        sql = f"""insert into ZM_CDM_INVENTORY_POINTER(MANDT,ZSEQ,WERKS,CHARG,MATNR,ZWAFER_LOT,ZWAFER_ID,ZOUT_BOX,ZBIN_NO,ZDIE_QTY_GI,MO_ID,MO_WAFER_ID,MO_WAFER_SN,MO_FLAG,ID,CREATE_DATE)
        values('900','{row['ZSEQ']}','{row['WERKS']}','{row['CHARG']}','{row['MATNR']}','{row['ZWAFER_LOT']}','{row['ZWAFER_ID']}','{row['ZOUT_BOX']}','{row['ZBIN_NO']}','{row['ZDIE_QTY_GI']}',
        '{mo_id}','{wafer['lotWaferID']}','{wafer['waferSN']}','1',ZM_CDM_INVENTORY_POINTER_SEQ.NEXTVAL,NOW())
        """
        con.exec_n(sql)
        con_dw.exec_n(sql)


# 更新工单表
def update_mo_wafer_invs(con, wafer, wafer_invs):
    wafer_invs_str = json.dumps(wafer_invs)
    print(wafer_invs)
    # 更新后台工单明细表
    sql = f"UPDATE ZM_CDM_MO_ITEM SET REMARK ='{wafer_invs_str}' where WAFER_SN ='{wafer['waferSN']}' "
    if not con.exec_n(sql):
        con.db.rollback()
        abort(make_response({"ERR_DESC": "工单扣账信息保存异常"}))


# 保存请求报文
def save_mo_request_json(con, mo_id, req_data):
    req_json = json.dumps(req_data, sort_keys=True, indent=2)
    if req_data.get('HEADER', {}).get('ACTION_ID') == 'N':
        sql = f"UPDATE ZM_CDM_MO_HEADER SET REQUEST_JSON = '{req_json}' WHERE MO_ID = '{mo_id}' "
    else:
        sql = f"UPDATE ZM_CDM_MO_HEADER SET REQUEST_JSON = '{req_json}',UPDATE_DATE=NOW()  WHERE MO_ID = '{mo_id}' "

    con.exec_n(sql)


# 工单失效
def disable_mo_id(con, mo_id, err_msg):
    sql = f"UPDATE ZM_CDM_MO_HEADER SET FLAG = '0',REMARK2='{err_msg}',REMARK3='E'  WHERE MO_ID = '{mo_id}' "
    con.exec_n(sql)
    sql = f"UPDATE ZM_CDM_MO_ITEM SET FLAG = '0',FLAG2=ID  WHERE MO_ID = '{mo_id}' "
    con.exec_n(sql)
    sql = f"UPDATE ZM_CDM_PO_ITEM SET MO_ID = '',MO_ITEM ='',FLAG2 ='0' WHERE MO_ID = '{mo_id}' "
    con.exec_n(sql)


# 发送SAP工单创建请求
def send_mo_request(con, mo_request, mo_id, res):
    # 创建SAP请求
    sap_res = get_data_from_web_api("PP009", mo_request)
    # 异常返回处理
    if sap_res['ERR_MSG']:
        disable_mo_id(con, mo_id, sap_res['ERR_MSG'])
        res['ERR_DESC'] = sap_res['ERR_MSG']
        return res

    return_node = sap_res['RES_DATA_D'].get('RETURN')
    if not return_node:
        disable_mo_id(con, mo_id, "SAP返回异常:没有RETURN节点")
        res['ERR_DESC'] = "SAP返回异常:没有RETURN节点"
        return res

    # 解析返回结果
    if isinstance(return_node, list):
        for mo_item in return_node:
            update_mo_data(con, mo_item)
    else:
        mo_item = return_node
        update_mo_data(con, mo_item)


# 更新工单数据
def update_mo_data(con, mo_item):
    mo_id = mo_item.get('SHOP_ORDER', '')
    sap_mo_id = mo_item.get('AUFNR', '')
    mo_status = mo_item.get('TYPE', '')
    mo_message = mo_item.get('MESSAGE', '')

    if mo_status == 'S':
        sql = f"UPDATE ZM_CDM_MO_HEADER SET SAP_MO_ID = '{sap_mo_id}',REMARK2='{mo_message}',REMARK3='S' WHERE MO_ID = '{mo_id}' "
        con.exec_c(sql)
    else:
        disable_mo_id(con, mo_id, mo_message)


# 获取工单创建状态
def get_mo_status(con, mo_id_list, res):
    mo_flag = True
    for mo_id in mo_id_list:
        mo_item = {'SHOP_ORDER': '', 'AUFNR': '', 'MESSAGE': '', 'TYPE': '', 'LOT_ID': '',
                   'WAFER_ID': [], 'WAFER_ID_LIST': ''}

        sql = f"SELECT MO_ID,SAP_MO_ID,REMARK2,REMARK3,WAFER_PN FROM ZM_CDM_MO_HEADER WHERE MO_ID = '{mo_id}' "
        results = con.query(sql)
        if results:
            mo_item['AUFNR'] = xstr(results[0][1])
            mo_item['MESSAGE'] = xstr(results[0][2])
            mo_item['TYPE'] = xstr(results[0][3])
            mo_item['MO_CREATE_ID'] = xstr(results[0][4])

            if mo_item['TYPE'] == 'S':
                mo_item['SHOP_ORDER'] = xstr(
                    results[0][0])
            else:
                mo_item['SHOP_ORDER'] == ''
                mo_flag = False

        sql = f"SELECT LOT_ID,WAFER_ID FROM ZM_CDM_MO_ITEM WHERE MO_ID = '{mo_id}' ORDER BY LOT_ID,WAFER_ID "
        results = con.query(sql)
        if results:
            for row in results:
                mo_item['LOT_ID'] = xstr(row[0])
                mo_item['WAFER_ID'].append(xstr(row[1]))

            mo_item['WAFER_ID_LIST'] = ','.join(mo_item['WAFER_ID'])

        # 结果整合
        res['ITEM_LIST'].append(mo_item)

    return mo_flag


# 获取工单号
def get_mo_id(con, mo_id_prefix):
    mo_date = time.strftime('%y%m%d')
    sql = "SELECT ZM_CDM_MO_SEQ.Nextval FROM dummy"
    mo_seq = ('000' + xstr(con.query(sql)[0][0]))[-3:]
    mo_id = mo_id_prefix + '-' + mo_date + mo_seq
    return mo_id


# 刷新工单数据
def refresh_mo_data(mo_refresh):
    res = {"ERR_MSG": ""}
    con = conn.HanaConn()
    con_or = conn.OracleConn()
    print(mo_refresh)

    for mo_item in mo_refresh['items']:
        ht_pn = mo_item['HT_PN']
        mo_id = mo_item['MO_ID']

        # 打标码更新
        smc.set_marking_code_mo(ht_pn=ht_pn, mo_id=mo_id)

        # 获取mo_request_json
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
            wafer_id = item.get('WAFER_ID')
            wafer_sn = item.get('WAFER_SN')
            if not wafer_sn:
                wafer_sn = get_mo_wafer_sn(con, mo_id, wafer_id)

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
        save_mo_request_json(con, mo_id, mo_request)

        mo_refresh = {"MO_DATA": []}
        mo_refresh['MO_DATA'].append(mo_request)
        send_mo_refresh_request(mo_refresh, res)

    con.db.commit()
    return res


# 获取工单wafer唯一码
def get_mo_wafer_sn(con, mo_id, wafer_id):
    sql = f"SELECT WAFER_SN FROM ZM_CDM_MO_ITEM zcmi WHERE MO_ID = '{mo_id}' AND LOT_WAFER_ID = '{wafer_id}' "
    results = con.query(sql)
    if results:
        return xstr(results[0][0])
    return ''


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


# 检查引线框架库存---------------------------------------------
def check_frame_inventory_backup(mo_data):
    sap_product_id = mo_data['header']['sapProductName']
    sap_process = mo_data['header']['process']
    sap_product_id = sap_product_id[-8:]
    mo_dies = 0  # 工单总dies
    for row in mo_data['items']:
        mo_dies = mo_dies + int(row['grossDies'])

    if sap_process not in ("FC", "FC+FT"):
        return True

    # 框架料号列表
    con = conn.HanaConnDW()
    f_matnr_list = ('10103365', '10103413', '10103417', '10103493', '10103418', '10103366', '10103415', '10103367', '10103495', '10103412', '10103416', '10104245', '10103403', '10103411', '10103787', '10103337', '10103786', '10103919', '10103918', '10103410', '10103409', '10103364',
                    '10103419', '10103401', '10103791', '10103523', '10103522', '10103491', '10103352', '10103778', '10103350', '10103497', '10103362', '10103488', '10103405', '10103398', '10104275', '10103399', '10103397', '10103492', '10103489', '10103490', '10103400', '10103406', '10103404')

    sql = f"""  SELECT b.IDNRK FROM VM_SAP_V_MAT a
        INNER JOIN VM_SAP_V_ITEM b on a.STLNR = b.STLNR
        INNER JOIN VM_SAP_MAT_INFO c ON c.MATNR = b.IDNRK
        WHERE SUBSTRING(a.MATNR,LENGTH(a.MATNR)-7) = '{sap_product_id}'
		AND REPLACE(b.IDNRK,'0000000000','') IN {str(f_matnr_list)}
        AND a.WERKS = '1200'
    """
    results = con.query(sql)
    if not results:
        print("BOM无法引线框架")
        return True

    # 框架料号
    f_matnr = xstr(results[0][0])
    # 框架库存余量
    sql = f"SELECT sum(CLABS) FROM MCHB WHERE MATNR = '{f_matnr}' "
    results = con.query(sql)
    if not results:
        frame_inv_qty = 0
    frame_inv_qty = results[0][0]
    print(f_matnr, frame_inv_qty)
    if mo_dies > frame_inv_qty:
        abort(make_response(
            {"ERR_DESC": f"FC/FT引线框架工单需求量:{mo_dies},实际库存量:{frame_inv_qty}, 库存不足,不允许开工单;请联系物控确认"}))
    else:
        print(f"引线框架库存充足:工单需求量:{mo_dies},实际库存量:{frame_inv_qty}")
        return True


# 检查引线框架库存---------------------------------------------
def check_frame_inventory(mo_data):
    con_dw = conn.HanaConnDW()

    sap_product_id = mo_data['header']['sapProductName']
    sap_product_id = ('00000000000000000' + sap_product_id)[-18:]

    # 框架料号列表
    f_matnr_list = ('10103365', '10103413', '10103417', '10103493', '10103418', '10103366', '10103415', '10103367', '10103495', '10103412', '10103416', '10104245', '10103403', '10103411', '10103787', '10103337', '10103786', '10103919', '10103918', '10103410', '10103409', '10103364',
                    '10103419', '10103401', '10103791', '10103523', '10103522', '10103491', '10103352', '10103778', '10103350', '10103497', '10103362', '10103488', '10103405', '10103398', '10104275', '10103399', '10103397', '10103492', '10103489', '10103490', '10103400', '10103406', '10103404')

    sql = f""" SELECT SP.IDNRK,P02.STAGE,SP.MENGE,SP.MEINS,SK.BMENG 
    FROM MARA MA
    INNER JOIN MAKT MK ON MK.MATNR  =MA.MATNR AND MK.SPRAS='1'
    INNER JOIN MAST MT ON MT.MATNR =MA.MATNR AND MT.MATNR =MK.MATNR  AND MT.WERKS ='1200'
    INNER JOIN STKO SK ON SK.STLNR =MT.STLNR 
    INNER JOIN STPO SP ON SP.STLNR =SK.STLNR 
    INNER JOIN MARA MA1 ON MA1.MATNR=SP.IDNRK 
    INNER JOIN MAKT MK1 ON MK1.MATNR =MA1.MATNR  AND MK1.SPRAS='1'
    INNER JOIN ZKTPP0002 P02 ON P02.ARBPL = SP.SORTF AND P02.WERKS ='1200'
    WHERE  MT.WERKS ='1200' AND MA.MATNR = '{sap_product_id}'
    AND SUBSTRING(SP.IDNRK, 11) IN {str(f_matnr_list)}
    """
    results = con_dw.query(sql)
    if not results:
        print("未使用指定引线框架,无需检查")
        return True

    # 框架料号,站点
    frame_matnr = xstr(results[0][0])
    frame_site = xstr(results[0][1])
    # 总用量
    menge = int(results[0][2])
    # 规格
    meins = results[0][3]
    # 基数
    bmenge = int(results[0][4])

    # 计算单颗die框架用量
    if meins == 'KEA':
        smenge = (menge * 1000) / bmenge
    else:
        smenge = menge / bmenge

    # 1.计算工单框架需求量mo_request_qty
    mo_dies = 0
    for row in mo_data['items']:
        mo_dies = mo_dies + int(row['grossDies'])

    frame_mo_qty = mo_dies * smenge

    # 2.计算框架库存量frame_inv_qty
    sql = f"SELECT sum(CLABS) FROM MCHB WHERE MATNR = '{frame_matnr}' "
    results = con_dw.query(sql)
    if not results or not results[0][0]:
        frame_inv_qty = 0
        abort(make_response(
            {"ERR_DESC": f"FC引线框架{frame_matnr}工单需求量:{frame_mo_qty},可用库存量0, 库存不足,不允许开工单;请联系物控确认"}))

    if meins == 'KEA':
        frame_inv_qty = float(results[0][0]) * 1000
    else:
        frame_inv_qty = float(results[0][0])

    # 工单占用量计算
    mo_usage_qty = get_mo_frame_qty(frame_matnr, frame_site)
    mo_usage_qty = mo_usage_qty * smenge
    frame_inv_qty = frame_inv_qty - mo_usage_qty

    # 3.比较
    print(frame_mo_qty, frame_inv_qty)
    if frame_mo_qty > frame_inv_qty:
        abort(make_response(
            {"ERR_DESC": f"FC引线框架工单需求量:{frame_mo_qty},可用库存量:{frame_inv_qty}, 库存不足,不允许开工单;请联系物控确认"}))
    else:
        print(f"引线框架库存充足:工单需求量:{frame_mo_qty},实际库存量:{frame_inv_qty}")
        return True


# 计算哪些工单耗用了该框架
def get_mo_frame_qty(frame_matnr, frame_site):
    con_dw = conn.HanaConnDW()
    con = conn.HanaConn()
    mo_list = []

    sql = f"""SELECT MO_ID FROM ZM_CDM_MO_HEADER WHERE FLAG ='1' AND ('0000000000' || SAP_PRODUCT_PN) IN 
    (SELECT DISTINCT MA.MATNR 
    FROM MARA MA
    INNER JOIN MAKT MK ON MK.MATNR  =MA.MATNR AND MK.SPRAS='1'
    INNER JOIN MAST MT ON MT.MATNR =MA.MATNR AND MT.MATNR =MK.MATNR  AND MT.WERKS ='1200'
    INNER JOIN STKO SK ON SK.STLNR =MT.STLNR 
    INNER JOIN STPO SP ON SP.STLNR =SK.STLNR 
    INNER JOIN MARA MA1 ON MA1.MATNR=SP.IDNRK 
    INNER JOIN MAKT MK1 ON MK1.MATNR =MA1.MATNR  AND MK1.SPRAS='1'
    INNER JOIN ZKTPP0002 P02 ON P02.ARBPL = SP.SORTF AND P02.WERKS ='1200'
    WHERE  MT.WERKS ='1200'AND SP.IDNRK IN ('{frame_matnr}')
    )
    """
    results = con_dw.query(sql)
    for row in results:
        mo_id = row[0]
        if not check_frame_deducted(con, mo_id, frame_site):
            mo_list.append(mo_id)

    if not mo_list:
        mo_frame_qty = 0
        return mo_frame_qty

    # 计算工单总数量
    mo_list = tuple(mo_list)
    sql = f"""SELECT SUM( CAST(B.GROSS_DIE_QTY as INTEGER)) FROM ZM_CDM_MO_HEADER A 
    INNER JOIN ZM_CDM_MO_ITEM B ON A.MO_ID = B.MO_ID 
    WHERE A.MO_ID IN {str(mo_list)} AND A.FLAG = '1'
    """
    results = con.query(sql)
    if results and results[0]:
        mo_frame_qty = results[0][0]
    else:
        mo_frame_qty = 0

    return mo_frame_qty


# 判断工单是否已经扣框架
def check_frame_deducted(con, mo_id, frame_site):
    sql = f"SELECT * FROM ZR_REPORT_EAP_MOVE WHERE SHOP_ORDER = '{mo_id}' AND SITE = '{frame_site}' AND VALUE = 'DONE' AND COMMENT = 'S' "
    results = con.query(sql)
    if results:
        return True
    else:
        return False


if __name__ == "__main__":
    data = {'header': {'moInvBin': '', 'moType': 'YP02', 'moPrefix': 'ATW', 'moPriority': '', 'custCode': 'HK006', 'custPN': 'GHW9466R16', 'htPN': 'Y68237B', 'productName': '19Y68237B001BR', 'productName2': '19Y68237B001BR', 'queryMOText': '查询', 'productNameType': 'P3', 'sapProductName': '32115057', 'planStartDate': '20210715', 'planEndDate': '20210730', 'createBy': '', 'createDate': '', 'mixMOFlag': False, 'bonded': '', 'custRework': 'N', 'processKey': 'CU PILLAR(1P2M)', 'primaryMat': '000000000042208447', 'fhfs': '入库即发到客户端', 'userName': '07885', 'moTypePO': 'YP02', 'markCodeFlag': 'N', 'process': 'BUMPING+OS', 'markCodeRule': '', 'base_so': 'Y', 'lcbz': 'Y', 'frozen': '', 'grossDies': '7403'}, 'items': [{'bonded': '保税', 'fabLotID': 'C010193.00', 'grossDies': 7403, 'inventoryGrossDies': 185075, 'lotBin': '', 'lotID': 'N520193.8SC', 'lotProprity': '0', 'moInvStatus': 'Y', 'moWaferQty': 1, 'poID': 'GULF21060KS', 'poItem': '000010', 'soID': '10001060242', 'soItem': '000010', 'subLotID': 'C010193.00', 'waferIDStrList': '01', 'waferList': [{'CHARG': '2003161631', 'LGORT': '1904', 'MATNR': '000000000042208447', 'WAFER_INV_ITEMS': [
        {'CHARG': '2003161631', 'LGORT': '1904', 'MATNR': '000000000042208447', 'WERKS': '1200', 'ZBIN_NO': '', 'ZDIE_QTY_GI': 0, 'ZDIE_QTY_RM': 7403, 'ZGROSS_DIE_QTY': 7403, 'ZOUT_BOX': '', 'ZSEQ': '001', 'ZWAFER_ID': 'C010193.0001', 'ZWAFER_LOT': 'C010193.00'}], 'WERKS': '1200', 'ZDIE_QTY_GI': 0, 'ZDIE_QTY_GOOD_RM': 7403, 'ZDIE_QTY_NG_RM': 0, 'ZDIE_QTY_RM': 7403, 'ZGROSS_DIE_QTY': 7403, 'ZSEQ': '001', 'ZWAFER_GROSS_DIE': 7403, 'ZWAFER_ID': 'C010193.0001', 'ZWAFER_LOT': 'C010193.00', 'bomPartID': '42208447', 'fabLotID': 'C010193.00', 'goodDies': 7403, 'grossDies': 7403, 'invPartID': '42208447', 'inventoryDesc': '库存充足', 'inventoryDies': 7403, 'inventoryFlag': True, 'inventoryGIDies': 0, 'inventoryGoodDies': 7403, 'inventoryGrDies': 7403, 'inventoryStatus': '库存充足', 'lotID': 'N520193.8SC', 'lotWaferID': 'N520193.8SC01', 'mapFlag': True, 'mapMessage': 'map无需更新', 'markCode': '', 'ngDies': 0, 'poGoodDies': 7403, 'primary_mat': '', 'productGrossDies': '7403', 'queryGoodDies': 7403, 'realID': 'Y', 'waferBin': 'G+B', 'waferID': '01', 'waferPartNo': '000000000042208447', 'waferSN': '100589114', 'zzmylx': 'A'}], 'waferPartNo': '', 'waferQty': 25, 'waferSNList': ['100589114'], 'startDate': '日期:20210715', 'endDate': '日期:20210730'}]}

    create_mo(data)
