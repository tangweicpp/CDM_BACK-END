'''
@File    :   com_ws.py
@Time    :   2020/11/13 19:12:39
@Author  :   Tony Tang 
@Version :   1.0
@Contact :   wei.tang_ks@ht-tech.com
@License :   (C)Copyright 2020-2025, Htks
@Desc    :   通用Web service接口类
'''
import requests
import json
from requests.auth import HTTPBasicAuth


class WS():
    def __init__(self) -> None:
        self.url = ''
        # self.user = 'WS_USER'
        self.user = 'pouser'
        # self.passwd = 'ht1234'
        self.passwd = 'Qwer4321'
        self.content_type = {"Content-Type": "application/json"}
        self.action = {'status': False, 'data': {}, 'desc': ''}

    def send(self, input, send_to):
        if send_to == 'SD017':
            # self.url = 'http://192.168.98.245:50000/RESTAdapter/SD/KSSD017'
            # PRD
            self.url = 'http://192.168.98.126:50000/RESTAdapter/SD/KSSD017'

        elif send_to == 'PP009':
            # self.url = 'http://192.168.98.245:50000/RESTAdapter/PP/KSPP009'
            # PRD
            self.url = 'http://192.168.98.126:50000/RESTAdapter/PP/KSPP009'

        elif send_to == 'MM108':
            # self.url = 'http://192.168.98.245:50000/RESTAdapter/MM/KSMM108'
            # PRD
            self.url = 'http://192.168.98.126:50000/RESTAdapter/MM/KSMM108'

        else:
            self.action['status'] = False
            self.action['desc'] = '接口URL未定义'
            return self.action

        try:
            if isinstance(input, dict):
                input = json.dumps(input)

            if not isinstance(input, str):
                self.action['status'] = False
                self.action['desc'] = '接口输入参数非法'
                return self.action

            output = requests.post(self.url, data=input, headers=self.content_type,
                                   auth=HTTPBasicAuth(self.user, self.passwd)).text

            output = json.loads(output)
            self.action['status'] = True
            self.action['data'] = output

        except Exception as e:
            self.action['status'] = False
            self.action['desc'] = f"ERROR:{e}"
            return self.action

        return self.action

    def __del__(self):
        pass


if __name__ == "__main__":
    input = {
        "ITEM": {
            "MATNR": "30100508",
            "WERKS": "1200",
            "Z_WAFERLOT": "PVPB45"
        }
    }
    ret = WS().send(input, 'MM108')
    print(ret)
