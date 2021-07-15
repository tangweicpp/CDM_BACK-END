import conn_db as conn
import uuid
import pp_create_fo_mo as gsw
from flask import abort, make_response


def xstr(s):
    return '' if s is None else str(s).strip()


# 获取DUMMY工单,硅基工单,玻璃工单的物料信息
def get_dummy_product(mo_query):
    res = {'ERR_MSG': '', 'DATA': {}}
    con = conn.HanaConnDW()
    cust_code = mo_query['cust_code']
    product_name = mo_query['product_name']

    sql = f"SELECT DISTINCT PARTNER FROM VM_SAP_PO_CUSTOMER vspc WHERE ZZYKHH = '{cust_code}' "
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = f"客户代码{cust_code}还未建立SAP客户, 请联系内勤/销售建立"

        res['ERR_SQL'] = sql
        return res

    sap_cust_code = xstr(results[0][0]).lstrip('0')

    sql = f''' SELECT DISTINCT MATNR,ZZJYGD,ZZKHXH,ZZHTXH FROM VM_SAP_MAT_INFO WHERE ZZCNLH = '{product_name}' '''
    results = con.query(sql)
    if not results:
        res['ERR_MSG'] = "查询不到客户代码:" + \
            cust_code + ",成品料号:" + product_name + "的对照关系"

        res['ERR_SQL'] = sql
        return res

    res['DATA']['SAP_PRODUCT_NAME'] = xstr(results[0][0]).lstrip('0')
    res['DATA']['GROSS_DIES'] = int(xstr(results[0][1]))
    res['DATA']['CUST_DEVICE'] = xstr(results[0][2])
    res['DATA']['HT_DEVICE'] = xstr(results[0][3])
    res['DATA']['SAP_CUST_CODE'] = sap_cust_code

    return res


# 创建特殊订单:DUMMY
def create_dummy_mo(mo_data):
    res = {"ERR_MSG": ""}
    con = conn.HanaConn()
    header = mo_data['header']
    items = mo_data['items']
    header['upload_id'] = get_rand_id(8)

    # 插入订单头表
    sql = f'''  INSERT INTO ZM_CDM_PO_HEADER(CUST_CODE,PO_TYPE,USER_NAME,UPLOAD_ID,FLAG,UPLOAD_DATE,ID) values('{header['custCode']}','{header['moType']}','{header['userName']}',
    '{header['upload_id']}','1',now(),ZM_CDM_PO_HEADER_SEQ.NEXTVAL) '''
    if not con.exec_n(sql):
        con.db.rollback()
        res['ERR_MSG'] = "订单头表插入异常"
        return res

    # 硅基工单
    if header['moType'] == "YP12":
        # 插入订单明细表
        for row in items:
            # lot插入seq表
            sql = f""" INSERT INTO ZM_CDM_COMMON_SEQ(ZZTYPE,ZZBASE,ZZKEY,ZZSEQ,ZZTIME) values('SI_WAFER_ID','{row.get('waferLotBase','')}','{row.get('lotID','')}',{row.get('waferLotSeq',0)},now())  """
            exec_flag, exec_msg = con.exec_n_2(sql)
            if not exec_flag:
                con.db.rollback()
                abort(make_response({"ERR_MSG": exec_msg}))

            for wafer_id in row['waferIDList']:
                wafer_id = ('0' + wafer_id) if len(wafer_id) == 1 else wafer_id
                lot_wafer_id = row['lotID'] + '-' + ('00' + wafer_id)[-2:]
                wafer_check_sum = gsw.get_sum_str(lot_wafer_id+"A0")
                lot_wafer_id = lot_wafer_id + wafer_check_sum

                sql = f""" INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE,PO_TYPE,CUSTOMER_DEVICE,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,
                    PASSBIN_COUNT,FAILBIN_COUNT,FLAG,FLAG2,FLAG3,CREATE_DATE,CREATE_BY,WAFER_TIMES,UPLOAD_ID,WAFER_SN,HT_PN,MARK_CODE,PO_ID)
                    values('{header['custCode']}','{header['moType']}','{header['custDevice']}','{header['productName']}','{header['sapProductName']}',
                    '{row['lotID']}','{wafer_id}','{lot_wafer_id}',{int(row['grossDies'])},0,'1','0','1',NOW(),
                    '{header['userName']}','','{header['upload_id']}', zm_cdm_wafer_sn_seq_new.nextval ,'{header['htDevice']}','{lot_wafer_id}','')
               
                """
                if not con.exec_n(sql):
                    con.db.rollback()
                    res['ERR_MSG'] = "订单明细表插入异常"
                    return res
    else:
        # 其他工单
        # 插入订单明细表
        for row in items:
            for wafer_id in row['waferIDList']:
                wafer_id = ('0' + wafer_id) if len(wafer_id) == 1 else wafer_id
                lot_wafer_id = row['lotID']+('00' + wafer_id)[-2:]
                sql = f""" INSERT INTO ZM_CDM_PO_ITEM(CUST_CODE,PO_TYPE,CUSTOMER_DEVICE,PRODUCT_PN,SAP_PRODUCT_PN,LOT_ID,WAFER_ID,LOT_WAFER_ID,
                    PASSBIN_COUNT,FAILBIN_COUNT,FLAG,FLAG2,FLAG3,CREATE_DATE,CREATE_BY,WAFER_TIMES,UPLOAD_ID,WAFER_SN,HT_PN,MARK_CODE,PO_ID)
                    values('{header['custCode']}','{header['moType']}','{header['custDevice']}','{header['productName']}','{header['sapProductName']}',
                    '{row['lotID']}','{wafer_id}','{lot_wafer_id}',{int(row['grossDies'])},0,'1','0','1',NOW(),
                    '{header['userName']}','','{header['upload_id']}', zm_cdm_wafer_sn_seq_new.nextval ,'{header['htDevice']}','{lot_wafer_id}','')
                """
                if not con.exec_n(sql):
                    con.db.rollback()
                    res['ERR_MSG'] = "订单明细表插入异常"
                    return res

    con.db.commit()
    return res


# 获取随机id
def get_rand_id(id_len):
    return str(uuid.uuid1())[:id_len]
