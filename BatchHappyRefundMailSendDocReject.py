import logging
import os.path
import re
import smtplib
import urllib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from sqlalchemy import create_engine

import pyodbc

# for Logging
import socket
import os

# Make a regular expression
# for validating an Email1
regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'


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


def validateEmail(email):
    if(re.search(regex,email)):
        return True
    else:
        return False


def getDfltParam():
    """
    index value
    0 = Subject Mail Thankyou for TH
    1 = Body Mail Thankyou for TH
    2 = Log Path Thankyou for TH
    3 = Subject Mail Thankyou for EN
    4 = Body Mail Thankyou for EN
    """

    strSQL = """
    SELECT remarks
    FROM dbo.CRM_Param
    WHERE param_code = 'CRM_MAILDOCREJT_REFUND'
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

    # email = smtplib.SMTP('aphubtran01.ap-thai.com', 25)
    email = smtplib.SMTP('apmail.apthai.com', 25)
    email.sendmail(from_email, to_email, msg.as_string())
    email.quit()
    return;

def getListData():

    strSQL = """
    SELECT hyrf_id
    FROM dbo.crm_contact_refund
    WHERE 1=1
    AND ac03_reject_doc_flag = 'Y'
    AND ISNULL(email_reject_doc_status,'N') NOT IN ('Y','E')
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
    SET email_reject_doc_status = ?,
        email_reject_doc_date = GETDATE()
    WHERE hyrf_id = ?
    """
    myConnDB.exec_sp(sqlStmt, params)
    return


def main(mailSubject, mailBody, mailSubject_en, mailBody_en):
    # Get Project ID List
    hyrfs = getListData()

    if not hyrfs:
        logging.info("No Data found to process Data")

    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.75;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    for hyrf in hyrfs:

        str_sql = """
        SELECT a.fullname, a.email, a.remainingtotalamount ,
        format(a.ac02_due_date,N'dd MMMM พ.ศ. yyyy','th-TH') AS transferdateapprove,
        a.addressnumber, a.unitnumber, a.project
        , a.foreigner, b.ProjectNameEng, format(a.ac02_due_date,N'dd MMMM yyyy')
        FROM dbo.crm_contact_refund a LEFT JOIN dbo.ICON_EntForms_Products b
		ON a.productid = b.ProductID
        WHERE a.hyrf_id = {}
        """.format(hyrf)

        df = pd.read_sql(sql=str_sql, con=db)

        # assign variable
        full_name = df.iat[0, 0]
        email = df.iat[0, 1]
        remain_amt = df.iat[0, 2]
        transfer_date = df.iat[0, 3]
        address_no = df.iat[0, 4]
        unit_no = df.iat[0, 5]
        project = df.iat[0, 6]
        foreigner = df.iat[0, 7]
        project_en = df.iat[0, 8]
        transfer_date_en = df.iat[0, 9]

        if foreigner == 'F':
            project = project_en
            transfer_date = transfer_date_en
            mailSubject = mailSubject_en
            mailBody = mailBody_en

        if validateEmail(email):
            logging.info("Valid email => {}".format(email))
            logging.info("Send Mail Start")
            sender = 'happyrefund@apthai.com'
            # receivers = ['varunya@apthai.com', 'jutamas@apthai.com', 'penkhae@apthai.com', 'pornnapa@apthai.com', 'suchat_s@apthai.com', 'happyrefund@apthai.com']
            # receivers = ['suchat_s@apthai.com']
            receivers = [email]
            bodyMailtmp = mailBody.replace("{full_name}", full_name)
            # bodyMailtmp = bodyMailtmp.replace("{due_date}", transfer_date)

            subject = mailSubject
            bodyMsg = "{}".format(bodyMailtmp)

            attachedFile = []

            # Send Email to Customer
            send_email(subject, bodyMsg, sender, receivers, attachedFile)
            logging.info("Successfully sent email")

            # Update Status Send Mail Success
            updateRefund(hyrf_id=hyrf, send_status='Y')
        else:
            # Update Status Send Mail Format Error
            updateRefund(hyrf_id=hyrf, send_status='E')
            logging.info("Not valid email => {}".format(email))

    logging.info("Send Mail to Customer Finish")


if __name__ == '__main__':
    # Get Default Parameter from DB
    dfltVal = getDfltParam()

    mailSubject = dfltVal[0]
    mailBody = dfltVal[1]
    log_path = dfltVal[2]
    mailSubject_en = dfltVal[3]
    mailBody_en = dfltVal[4]

    logFile = log_path + '/BatchHappyRefundMailSendDocReject.log'

    APPNAME='BatchHappyRefundMailSendDocReject'
    IPADDR=get_ipaddr()
    FORMAT="%(asctime)-5s {} {}: [%(levelname)-8s] >> %(message)s".format(IPADDR, APPNAME)

    logging.basicConfig(level=logging.DEBUG,
                        format=FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main(mailSubject, mailBody, mailSubject_en, mailBody_en)
    logging.info('End Process')
    logging.debug('#####################')
