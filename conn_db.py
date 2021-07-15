import pyhdb as hdb
import pymssql as mss
import cx_Oracle as ora
import pymysql


class HanaConn:
    def __init__(self) -> None:
        try:
            # 测试环境
            # self.db = hdb.connect('10.160.2.23', '31041',
            #                       'WIP', 'Sap12345')
            # 正式环境
            self.db = hdb.connect('10.160.2.20', '30015',
                                  'WIP', 'Sap12345')
            self.cur = self.db.cursor()
        except Exception as e:
            print("[ERROR] HANA connection failed!!! ==>", e)
            self.db = None
            self.cur = None

    def __del__(self):
        if self.cur and self.db:
            try:
                self.cur.close()
                self.db.close()
            except Exception as e:
                print("[ERROR] HANA close failed!!! ==>", e)

    def query(self, sql):
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] HANA query failed!!! ==>", e)
            return None
        else:
            return results

    def query2(self, sql):
        try:
            self.cur.execute(sql)
            header_str = self.cur.description
            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] HANA query failed!!! ==>", e)
            return None, None
        else:
            return results, header_str

    def exec_n(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            return True

    def exec_n_2(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False, e
        else:
            return True, ""

    def exec_c(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            self.db.commit()
            return True


class HanaConnDW:
    def __init__(self) -> None:
        try:
            # 测试环境
            # self.db = hdb.connect('10.160.2.23', '31015',
            #                       'ERP', 'Sap12345')

            self.db = hdb.connect('10.160.2.38', '30015',
                                  'ERP_PROD', 'Sap12345prod')

            self.cur = self.db.cursor()
        except Exception as e:
            print("[ERROR] HANA connection failed!!! ==>", e)
            self.db = None
            self.cur = None

    def __del__(self):
        if self.cur and self.db:
            try:
                self.cur.close()
                self.db.close()
            except Exception as e:
                print("[ERROR] HANA close failed!!! ==>", e)

    def query(self, sql):
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] HANA query failed!!! ==>", e)
            return None
        else:
            return results

    def query2(self, sql):
        try:
            self.cur.execute(sql)
            header_str = self.cur.description

            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] HANA query failed!!! ==>", e)
            return None
        else:
            return results, header_str

    def exec_n(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            return True

    def exec_n_2(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False, e
        else:
            return True, ""

    def exec_c(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            self.db.commit()
            return True


class OracleConn:
    def __init__(self) -> None:
        try:
            self.db = ora.connect(
                'INSITEQT2/KsMesDB_ht89@10.160.2.19:1521/mesora')
            self.cur = self.db.cursor()
        except Exception as e:
            print("[ERROR] ORACLE connection failed!!! ==>", e)
            self.db = None
            self.cur = None

    def __del__(self):
        if self.cur and self.db:
            try:
                self.cur.close()
                self.db.close()
            except Exception as e:
                print("[ERROR] ORACLE close failed!!! ==>", e)

    def query(self, sql):
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] ORACLE query failed!!! ==>", e)
            return None
        else:
            return results

    def query2(self, sql):
        try:
            self.cur.execute(sql)
            header_str = self.cur.description

            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] ORACLE query failed!!! ==>", e)
            return None
        else:
            return results, header_str

    def exec_n(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] ORACLE execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            return True

    def exec_c(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] ORACLE execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            self.db.commit()
            return True


class MssConn:
    def __init__(self) -> None:
        try:
            self.db = mss.connect('10.160.1.13', 'sa', 'ksxtDB', 'ERPBASE')
            self.cur = self.db.cursor()
        except Exception as e:
            print("[ERROR] SQL_SERVER connection failed!!! ==>", e)
            self.db = None
            self.cur = None

    def __del__(self):
        if self.cur and self.db:
            try:
                self.cur.close()
                self.db.close()
            except Exception as e:
                print("[ERROR] SQL_SERVER close failed!!! ==>", e)

    def query(self, sql):
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] SQL_SERVER query failed!!! ==>", e)
            return None
        else:
            return results

    def query2(self, sql):
        try:
            self.cur.execute(sql)
            header_str = self.cur.description

            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] SQL_SERVER query failed!!! ==>", e)
            return None
        else:
            return results, header_str

    def exec_n(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] SQL_SERVER execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            return True

    def exec_c(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] SQL_SERVER execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            self.db.commit()
            return True


class MysqlConn:
    def __init__(self) -> None:
        try:
            self.db = pymysql.connect(host='10.160.1.33', port=3306, user='administrator_hr',
                                      passwd='xsw@cde3', db='HRS', charset='utf8mb4')

            self.cur = self.db.cursor()
        except Exception as e:
            print("[ERROR] Mysql connection failed!!! ==>", e)
            self.db = None
            self.cur = None

    def __del__(self):
        if self.cur and self.db:
            try:
                self.cur.close()
                self.db.close()
            except Exception as e:
                print("[ERROR] Mysql close failed!!! ==>", e)

    def query(self, sql):
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] Mysql query failed!!! ==>", e)
            return None
        else:
            return results

    def query2(self, sql):
        try:
            self.cur.execute(sql)
            header_str = self.cur.description
            results = self.cur.fetchall()
        except Exception as e:
            print("[ERROR] Mysql query failed!!! ==>", e)
            return None, None
        else:
            return results, header_str

    def exec_n(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            return True

    def exec_n_2(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False, e
        else:
            return True, ""

    def exec_c(self, sql):
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("[ERROR] HANA execute failed!!! ==>", e)
            self.db.rollback()
            return False
        else:
            self.db.commit()
            return True
