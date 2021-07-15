import conn_db as conn
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
        sql = f'''INSERT INTO ZM_CDM_SO_HEADER(PO_NO,PO_TYPE,SO_SN,SO_CREATE_BY,SO_CREATE_DATE,CUST_CODE,FLAG,PO_UPLOAD_ID)
            values('{header['BSTKD']}','{header['AUART']}','{header_no}','{header['CREATER']}',NOW(),'{header['KUNNR']}','0','{header['UPLOAD_ID']}') '''

        con.exec_n(sql)

    return action, header_no


def run(upload_id):
    con = conn.HanaConn()
    so_data_list = {'SO_DATA': []}

    # header
    sql = f"""SELECT PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY,String_agg(WAFER_SN ,''',''') FROM ZM_CDM_PO_ITEM WHERE UPLOAD_ID = '{upload_id}'
            GROUP BY PO_TYPE,SAP_CUST_CODE,TRAD_CUST_CODE,PO_ID,CREATE_BY  """

    results = con.query(sql)
    for row in results:
        so_data = {'HEADER': {}, 'ITEM': []}

        po_type = xstr(row[0])
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
        header['UPLOAD_ID'] = upload_id
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
            po_date = xstr(row[11])
            # if len(po_date) != 8:
            #     abort(make_response({"ERR_MSG": "接单日期错误"}))

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


if __name__ == "__main__":
    run("1888041+")
