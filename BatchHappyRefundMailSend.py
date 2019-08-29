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
    0 = SQL Statement for Main Query
    1 = Excel File Name
    2 = receivers ;
    3 = Subject Mail
    4 = Body Mail
    5 = Footer Mail
    6 = Log Path
    """

    strSQL = """
    SELECT long_desc 
    FROM dbo.CRM_Param
    WHERE param_code = 'CRM_BG1_GRSC_XLS'
    ORDER BY param_seqn
    """

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.long_desc)

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
    --AND email LIKE '%_@__%.__%'
      --AND PATINDEX('%[^a-z,0-9,@,.,_,\-]%', email) = 0
	  AND ISNULL(email_sent_status,'N') <> 'Y'
	  ORDER BY createdate
    """

    myConnDB = ConnectDB()
    result_set = myConnDB.query(strSQL)
    returnVal = []

    for row in result_set:
        returnVal.append(row.hyrf_id)

    return returnVal


def main(dfltVal):
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
        # print(full_name, email, remain_amt)

        if not validateEmail(email):
            print("Not valid email => {}".format(email))
        else:
            print("Valid email => {}".format(email))

        
        logging.info("Send Mail Start")
        # sender = 'no-reply@apthai.com'
        sender = 'happyrefund@apthai.com'
        receivers = ['suchat_s@apthai.com']
        bodyMail = f'<p style="font-family:AP;">เรียนคุณ {full_name}<br /> \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ทางบริษัท เอพี (ไทยแลนด์) จำกัด (มหาชน)  ขอขอบคุณท่านเป็นอย่างสูงที่ได้ให้ความไว้วางใจที่มอบให้กับบริษัทฯ <br /> \
และตามที่ท่านได้ชำระเงินมานั้น  มียอดเงินจำนวนหนึ่งที่ท่านชำระเงินเกินเข้ามาให้กับบริษัทฯ ซึ่งทางบริษัทฯ <br /> \
ได้อำนวยความสะดวกเพิ่มช่องทางในการที่จะให้ท่านนำส่งเอกสาร  เพื่อยืนยันการคืนเงินส่วนที่ท่านชำระเงินเข้ามา <br /> \
โดยสามารถเข้าไปที่ web site ของทางบริษัทฯ ตาม link  นี้ได้ทันที โดยรายละเอียดสามารถตรวจสอบได้จาก web site ของทางบริษัทฯ<br /> \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ทั้งนี้ หากท่านมีข้อสงสัยหรือต้องการสอบถามข้อมูลเพิ่มเติม สามารถติดต่อเจ้าหน้าที่ได้ที่หมายเลขโทรศัพท์ 0-2261-2518-22 ต่อ 999 หรือ 888 <br /><br  /> \
  ช่องทางการคืนเงินให้กับลูกค้า : http://happyrefund.apthai.com <br /><br /> \
  ขอขอบคุณและขอแสดงความนับถือ<br /> \
  บริษัท เอพี (ไทยแลนด์) จำกัด (มหาชน)</p>'

        subject = "{}-{}".format('[CRM-HappyRefund] Summary Report Gross', hyrf)
        bodyMsg = "{}".format(bodyMail)

        attachedFile = []

        send_email(subject, bodyMsg, sender, receivers, attachedFile)
        logging.info("Successfully sent email")
    
    logging.info("Send Mail to Customer Finish")

if __name__ == '__main__':
    # Get Default Parameter from DB
    dfltVal = getDfltParam()

    log_path = '/home/ubuntu/tmp/log'
    logFile = log_path + '/BatchHappyRefundMailSend.log'

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-5s [%(levelname)-8s] >> %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main(dfltVal)
    logging.info('End Process')
    logging.debug('#####################')
