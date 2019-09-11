import requests
import json
import logging
import urllib
import pyodbc

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

import uuid
import random
import time

# for Logging
import socket
import os

APP_NAME = "CRMREFUND"
APIURL = 'http://192.168.0.40/smsapi/api/SMS/SendSMS'

# for Logging
def get_ipaddr():
    try:
        host_name = socket.gethostname()    
        return socket.gethostbyname(host_name)
    except:
        return "Unable to get Hostname and IP"


class ConnectDB:
    def __init__(self):
        ''' Constructor for this class. '''
        self._connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.75;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
        self._cursor = self._connection.cursor()

    def query(self, query):
        try:
            result = self._cursor.execute(query)
        except Exception as e:
            logging.error('error execting query "{}", error: {}'.format(query, e))
            return None
        finally:
            return result

    def update(self, sqlStatement):
        try:
            self._cursor.execute(sqlStatement)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def exec_sp(self, sqlStatement, params):
        try:
            self._cursor.execute(sqlStatement, params)
        except Exception as e:
            logging.error('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            self._cursor.commit()

    def exec_spRet(self, sqlStatement, params):
        try:
            result = self._cursor.execute(sqlStatement, params)
        except Exception as e:
            print('error execting Statement "{}", error: {}'.format(sqlStatement, e))
            return None
        finally:
            return result

    def __del__(self):
        self._cursor.close()


def send_sms(dataobj):
    headers = {'Content-type': 'application/json'}
    session = requests.Session()
    return session.post(APIURL, data=json.dumps(dataobj), headers=headers)


def getDfltParam():
    """
    index value
    0 = SMS Thank you for TH
    1 = SMS Thank you for Eng
    2 = Log Path for TH
    """

    strSQL = """
    SELECT remarks
    FROM dbo.CRM_Param WITH(NOLOCK)
    WHERE param_code = 'CRM_SMSTHX_REFUND'
    ORDER BY param_seqn
    """

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.remarks)

    return returnVal


def getListData():

    strSQL = """
    SELECT hyrf_id
    FROM dbo.crm_contact_refund WITH(NOLOCK)
    WHERE 1=1
    AND tf01_appv_flag = 'A' 
    AND tf02_appv_flag = 'A' 
    AND ac01_appv_flag = 'A' 
    AND ac02_appv_flag = 'A'
	  ORDER BY createdate
    """

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.hyrf_id)

    return returnVal


def updateRefund(hyrf_id, send_status):
    myConnDB = ConnectDB()

    params = (send_status, hyrf_id)

    sqlStmt = """
    UPDATE dbo.crm_contact_refund
    SET sms_thx_sent_status = ?,
        sms_thx_sent_date = GETDATE()
    WHERE hyrf_id = ?
    """
    myConnDB.exec_sp(sqlStmt, params)
    return


def main(smsTH: str, smsEN: str):
    # Get Data List
    hyrfs = getListData()

    if not hyrfs:
        logging.info("No Data found to process Data")

    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.75;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    for hyrf in hyrfs:

        str_sql = """
        SELECT a.foreigner, a.mobile, a.transfernumber
        FROM dbo.crm_contact_refund a WITH(NOLOCK)
        WHERE a.hyrf_id = {}
        """.format(hyrf)

        df = pd.read_sql(sql=str_sql, con=db)

        # assign variable
        foreigner = df.iat[0, 0]
        # mobile = df.iat[0, 1]
        ref1 = df.iat[0, 2]

        # Kai Fix Mobile No.
        mobile = '0830824173' # Kai
        # mobile = '0814584803' # Nam
        # mobile = '0844384171' # PFon
        # mobile = '0860554484' # PKae
        # mobile = '0922464243' # Nid

        if foreigner == 'F':
            sms_msg = smsEN
        else:
            sms_msg = smsTH

        # Kai Random msg
        sms_msg = "{} ({})".format(sms_msg, random.randint(500, 50000))
        logging.info("SMS Message = {}".format(sms_msg))

        # Update Status Send Mail Success
        updateRefund(hyrf_id=hyrf, send_status='Y')
        logging.info("Successfully sent sms")

        dataobj = sms_json_model(mobile, sms_msg, ref1)
        logging.info(dataobj)

        response = send_sms(dataobj)
        data = response.json()
        logging.info(data)
        logging.info('Status Code : {}'.format(response.status_code))
        logging.info('Status Message : {}'.format(data[0]['SendStatus']))
        logging.info('Status Text : {}'.format(data[0]['Result']))

        time.sleep(2)


def sms_json_model(mobile: str, sms_msg: str, ref1: str):
    dataobj = [{
        "smsid": 0,
        "sendByApp": APP_NAME,
        "sendDate": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "mobileNumber": mobile,
        "sender": "APRefund",
        "ref1": ref1,
        "ref2": "-",
        "ref3": "-",
        "messageFrom": "APRefund",
        "title": "-",
        "message": sms_msg,
        "sendStatus": "",
        "result": "",
        "fileName": "",
        "fullPath": ""
    }]
    return dataobj


if __name__ == '__main__':
    # Get Default Parameter from DB
    dfltVal = getDfltParam()

    smsTH = dfltVal[0]
    smsEN = dfltVal[1]
    log_path = dfltVal[2]
    # log_path = '.'

    logFile = log_path + '/BatchHappyRefundSendSMSThx.log'
    
    APPNAME='BatchHappyRefundSendSMSThx'
    IPADDR=get_ipaddr()
    FORMAT="%(asctime)-5s {} {}: [%(levelname)-8s] >> %(message)s".format(IPADDR, APPNAME)

    logging.basicConfig(level=logging.DEBUG,
                        format=FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main(smsTH, smsEN)
    logging.info('End Process')
    logging.debug('#####################')
