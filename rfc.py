# from pyrfc import Connection

# conn = Connection(ashost="192.168.98.249", sysnr='00', client='120', user='RFC_WW', passwd='Init1234')


import pyrfc
import tablib

conn_params_test = {
    "user": "KSPJ",
    "passwd": "Ab-123456",
    "ashost": "192.168.98.249",
    "sysnr": "00",
    "lang": "ZH",
    "client": "120"
}


conn_params_prd = {
    "user": "KSPP2",
    "passwd": "Ab-123456",
    "ashost": "192.168.98.127",
    "sysnr": "00",
    "lang": "ZH",
    "client": "900"
}


# 测试
def read_table_test():
    conn = pyrfc.Connection(**conn_params_test)

    option_parameter = [
        {"TEXT": "CHARG EQ '000000000030101532' "}
    ]

    # 读取标准表
    result = conn.call('RFC_READ_TABLE',
                       QUERY_TABLE="mast",
                       DELIMITER=",",
                       ROWCOUNT=20,
                       OPTIONS=option_parameter)

    # print(result)

    for row in result['DATA']:
        print(row)

    # 获取 FIELDS 表参数
    fields = tablib.Dataset()
    fields.dict = result['FIELDS']
    print(fields)

    # 获取 DATA 表参数
    data = tablib.Dataset()
    data.dict = result['DATA']
    print(data)

    # result = conn.call('ZKMM_CHANGE_PO_02')
    # print(result)

    # result = conn.call('STFC_CONNECTION', REQUTEXT=u'Hello SAP!')
    # print(result)

    # ABAP structures are mapped to Python dictionaries
    # IMPORTSTRUCT = {"RFCFLOAT": 1.23456789, "RFCCHAR1": "A"}

    # # ABAP tables are mapped to Python lists, of dictionaries representing ABAP tables' rows
    # IMPORTTABLE = []

    # result = conn.call(
    #     "STFC_STRUCTURE", IMPORTSTRUCT=IMPORTSTRUCT, RFCTABLE=IMPORTTABLE)

    # print(result["ECHOSTRUCT"])
    # print(result["RFCTABLE"])


# 查询BOM内晶圆组件料号
def get_bom_unit(matnr):
    # 0.创建连接
    conn = pyrfc.Connection(**conn_params_prd)

    # 1.查询物料清单
    option_parameter = [
        {"TEXT": f"MATNR EQ '{matnr}' AND WERKS EQ '1200'  "}
    ]

    # 读取标准表
    result = conn.call('RFC_READ_TABLE',
                       QUERY_TABLE="MAST",
                       DELIMITER=",",
                       ROWCOUNT=0,
                       OPTIONS=option_parameter, FIELDS=["STLNR"])

    for row in result['DATA']:
        stlnr = row['WA'].strip()

    # 2.查询组件
    option_parameter = [
        {"TEXT": f"STLNR EQ '{stlnr}'  "}
    ]

    # 读取标准表
    result = conn.call('RFC_READ_TABLE',
                       QUERY_TABLE="STPO",
                       DELIMITER=",",
                       ROWCOUNT=0,
                       OPTIONS=option_parameter, FIELDS=["IDNRK"])

    units = []
    for row in result['DATA']:
        unit = row['WA'].strip()
        if unit[10:11] in ('3', '4'):
            units.append(unit)

    if units:
        units = "','".join(units)
    else:
        units = ''

    return units


# 查询SAP SO明细
def get_so_info(so_id, so_item):
    # 0.创建连接
    conn = pyrfc.Connection(**conn_params_prd)
    po_id = ""
    sap_product_id = ""

    # 1.查询SO对应的PO号
    option_parameter = [
        {"TEXT": f"VBELN EQ '{so_id}' "}
    ]

    # 读取标准表
    result = conn.call('RFC_READ_TABLE',
                       QUERY_TABLE="VBAK",
                       DELIMITER=",",
                       ROWCOUNT=0,
                       OPTIONS=option_parameter, FIELDS=["BSTNK"])

    for row in result['DATA']:
        po_id = row['WA'].strip()

    # 2.查询组件
    option_parameter = [
        {"TEXT": f"VBELN EQ '{so_id}' AND POSNR EQ '{so_item}'  "}
    ]

    # 读取标准表
    result = conn.call('RFC_READ_TABLE',
                       QUERY_TABLE="VBAP",
                       DELIMITER=",",
                       ROWCOUNT=0,
                       OPTIONS=option_parameter, FIELDS=["MATNR"])

    for row in result['DATA']:
        sap_product_id = row['WA'].strip()

    return po_id, sap_product_id


def read_table_prd2(STLNR):
    conn = pyrfc.Connection(**conn_params_prd)

    option_parameter = [
        # {"TEXT": "ZWAFER_ID EQ 'NBE075000' AND CHARG EQ '2001886688' "}
        {"TEXT": f"STLNR EQ '{STLNR}'  "}
    ]

    # 读取标准表
    result = conn.call('RFC_READ_TABLE',
                       QUERY_TABLE="STPO",
                       DELIMITER=",",
                       ROWCOUNT=0,
                       OPTIONS=option_parameter, FIELDS=["IDNRK"])

    for row in result['DATA']:
        # print(row['WA'].split(',')[4].strip())
        print(row['WA'].strip())

    # 获取 FIELDS 表参数
    fields = tablib.Dataset()
    fields.dict = result['FIELDS']
    # print(fields)

    # 获取 DATA 表参数
    data = tablib.Dataset()
    data.dict = result['DATA']
    # print(data)

    # result = conn.call('ZKMM_CHANGE_PO_02')
    # print(result)

    # result = conn.call('STFC_CONNECTION', REQUTEXT=u'Hello SAP!')
    # print(result)

    # ABAP structures are mapped to Python dictionaries
    # IMPORTSTRUCT = {"RFCFLOAT": 1.23456789, "RFCCHAR1": "A"}

    # # ABAP tables are mapped to Python lists, of dictionaries representing ABAP tables' rows
    # IMPORTTABLE = []

    # result = conn.call(
    #     "STFC_STRUCTURE", IMPORTSTRUCT=IMPORTSTRUCT, RFCTABLE=IMPORTTABLE)

    # print(result["ECHOSTRUCT"])
    # print(result["RFCTABLE"])


if __name__ == "__main__":
    # get_bom_unit()
    so_id = "1030107282"
    so_item = "000010"
    print(get_so_info(so_id, so_item))
