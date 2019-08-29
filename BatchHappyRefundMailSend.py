import logging
import os.path
import smtplib
import urllib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from sqlalchemy import create_engine

import pyodbc
import re

# Make a regular expression 
# for validating an Email 
regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'


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


def validateEmail(email):
    if(re.search(regex,email)):
        return True
    else:
        return False


def getDfltParam():
    """
    index value
    0 = Subject Mail
    1 = Body Mail
    2 = Log Path
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


def send_email(subject, message, from_email, to_email=[], attachment=[]):
    """
    :param subject: email subject
    :param message: Body content of the email (string), can be HTML/CSS or plain text
    :param from_email: Email address from where the email is sent
    :param to_email: List of email recipients, example: ["a@a.com", "b@b.com"]
    :param attachment: List of attachments, exmaple: ["file1.txt", "file2.txt"]
    """
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ", ".join(to_email)
    msg.attach(MIMEText(message, 'html'))

    for f in attachment:
        with open(f, 'rb') as a_file:
            basename = os.path.basename(f)
            part = MIMEApplication(a_file.read(), Name=basename)

        part['Content-Disposition'] = 'attachment; filename="%s"' % basename
        msg.attach(part)

    email = smtplib.SMTP('aphubtran01.ap-thai.com', 25)
    email.sendmail(from_email, to_email, msg.as_string())
    email.quit()
    return;

def getListData():

    strSQL = """
    --SELECT personcardid, fullname, nationality, mobile, email, contractnumber
    SELECT hyrf_id
    FROM dbo.crm_contact_refund
    WHERE 1=1
	  AND ISNULL(email_sent_status,'N') <> 'Y'
	  ORDER BY createdate
    """

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.hyrf_id)

    return returnVal


def main(mailSubject, mailBody):
    # Get Project ID List
    hyrfs = getListData()
    
    if not hyrfs:
        print("No Data Found..!!")
        logging.info("No Data found to process Data")

    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.75;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    for hyrf in hyrfs:
        str_sql = """
        SELECT fullname, email, remainingtotalamount FROM dbo.crm_contact_refund WHERE hyrf_id = {}
        """.format(hyrf)

        df = pd.read_sql(sql=str_sql, con=db)
        
        # assign variable 
        full_name = df.iat[0, 0]
        email = df.iat[0, 1]
        remain_amt = df.iat[0, 2]

        if validateEmail(email):
            logging.info("Valid email => {}".format(email))
            logging.info("Send Mail Start")
            sender = 'happyrefund@apthai.com'
            receivers = ['varunya@apthai.com','jutamas@apthai.com','penkhae@apthai.com','pornnapa@apthai.com','suchat_s@apthai.com']
            # receivers = [email]
            bodyMailtmp = mailBody.replace("{full_name}", full_name)

            subject = mailSubject
            bodyMsg = "{}".format(bodyMailtmp)

            attachedFile = []

            send_email(subject, bodyMsg, sender, receivers, attachedFile)
            logging.info("Successfully sent email")
        else:
            logging.info("Not valid email => {}".format(email))

    logging.info("Send Mail to Customer Finish")

if __name__ == '__main__':
    # Get Default Parameter from DB
    dfltVal = getDfltParam()

    mailSubject = dfltVal[0]
    mailBody = dfltVal[1]
    # log_path = dfltVal[2]
    log_path = '.'
    
    logFile = log_path + '/BatchHappyRefundMailSend.log'

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-5s [%(levelname)-8s] >> %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main(mailSubject, mailBody)
    logging.info('End Process')
    logging.debug('#####################')
