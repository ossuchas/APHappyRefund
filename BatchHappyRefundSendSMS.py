import requests
import json
import logging
import urllib
import pyodbc

APP_NAME = "CRMREFUND"
APIURL = 'http://192.168.0.40/smsapi/api/SMS/SendSMS'

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
    0 = Subject Mail for TH
    1 = Body Mail for TH
    2 = Log Path for TH
    3 = Subject Mail for EN
    4 = Body Mail for EN
    """

    strSQL = """
    SELECT remarks
    FROM dbo.CRM_Param
    WHERE param_code = 'CRM_CS_REFUND'
    ORDER BY param_seqn
    """

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.remarks)

    return returnVal


def main():
    dataobj = [{
        "smsid": 0,
        "sendByApp": APP_NAME,
        "sendDate": "2019-09-04T04:03:06.431Z",
        "mobileNumber": "0830824173",
        "sender": "AP-Refund",
        "ref1": "10133CT9163802",
        "ref2": "string",
        "ref3": "string",
        "messageFrom": "string",
        "title": "string",
        "message": "string",
        "sendStatus": "string",
        "result": "string",
        "fileName": "string",
        "fullPath": "string"
    }]

    response = send_sms(dataobj)
    data = response.json()
    print(data)
    print('Status Code : {}'.format(response.status_code))
    print('Status Message : {}'.format(data[0]['SendStatus']))
    print('Status Text : {}'.format(data[0]['Result']))


if __name__ == '__main__':
    # Get Default Parameter from DB
    # dfltVal = getDfltParam()
    #
    # mailSubject = dfltVal[0]
    # mailBody = dfltVal[1]
    # log_path = dfltVal[2]
    # mailSubject_en = dfltVal[3]
    # mailBody_en = dfltVal[4]
    #
    # logFile = log_path + '/BatchHappyRefundMailSend.log'
    #
    # logging.basicConfig(level=logging.DEBUG,
    #                     format='%(asctime)-5s [%(levelname)-8s] >> %(message)s',
    #                     datefmt='%Y-%m-%d %H:%M:%S',
    #                     filename=logFile,
    #                     filemode='a')
    #
    # logging.debug('#####################')
    # logging.info('Start Process')
    main()
    # logging.info('End Process')
    # logging.debug('#####################')
