''' 
@File    :   web_api_client.py
@Time    :   2020-12-21
@Author  :   Tony Tang 
@Version :   1.0
@Contact :   wei.tang_ks@ht-tech.com
@Desc    :   公共web接口 
'''

import requests
import json
from requests.auth import HTTPBasicAuth


def get_data_from_web_api(req_type, req_str):
    res = {"ERR_MSG": ""}
    if not (req_type and req_str):
        res["ERR_MSG"] = "请求类别和请求内容不可为空"
        return res

    if req_type == "SD017":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/SD/KSSD017"
        req_url = "http://192.168.98.126:50000/RESTAdapter/SD/KSSD017"
    elif req_type == "PP009":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/PP/KSPP009"
        req_url = "http://192.168.98.126:50000/RESTAdapter/PP/KSPP009"
    elif req_type == "MM108":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/MM/KSMM108"
        req_url = "http://192.168.98.126:50000/RESTAdapter/MM/KSMM108"
    elif req_type == "MM138":
        # req_url = "http://192.168.98.245:50000/RESTAdapter/MM/KSMM138"
        req_url = "http://192.168.98.126:50000/RESTAdapter/MM/KSMM138"
    else:
        res["ERR_MSG"] = f"暂不包含请求类别{req_type}的WEB接口"
        return res

    res = send_request_to_web_api(req_url, req_str, req_type)
    return res


def send_request_to_web_api(req_url, req_str, req_type):
    res = {"ERR_MSG": ""}
    # req_user = "WS_USER"
    req_user = "pouser"
    # req_passwd = "ht1234"
    req_passwd = "Qwer4321"
    req_content_type = {"Content-Type": "application/json"}

    try:
        if isinstance(req_str, dict):
            req_str = json.dumps(req_str)
        if not isinstance(req_str, str):
            res['ERR_MSG'] = '接口输入参数非法'
            return res

        res["REQ_URL"] = req_url
        res["REQ_DATA_S"] = req_str

        if req_type == "PP009":
            res["RES_DATA_S"] = requests.post(url=req_url, data=req_str, headers=req_content_type,
                                              auth=HTTPBasicAuth(req_user, req_passwd)).text
        else:
            res["RES_DATA_S"] = requests.post(url=req_url, data=req_str, headers=req_content_type,
                                              auth=HTTPBasicAuth(req_user, req_passwd), timeout=120).text

        res["RES_DATA_D"] = json.loads(res["RES_DATA_S"])

    except Exception as e:
        res['ERR_MSG'] = f"接口返回数据异常:{e}"
        return res

    else:
        return res


if __name__ == "__main__":
    req_data = {
        "ITEM": {
            "WERKS": "1200",
            "MATNR": "32109633",
            "Z_WAFERLOT": "H36B37.5"
        }
    }

    res = get_data_from_web_api("MM108", req_str=req_data)
    print(res)
