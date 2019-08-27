import smtplib
import os.path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import pyodbc
import xlwt
import logging
from datetime import datetime, timedelta
import glob
from dateutil.relativedelta import relativedelta


class ConnectDB:
    def __init__(self):
        ''' Constructor for this class. '''
        self._connection = pyodbc.connect( 'Driver={SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
        # self._connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;')
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


def main(dfltVal):
    last_month = datetime.now() - relativedelta(months=1)

    # parameter date format dd/mm/yyyy for filename
    starting_day_of_current_year = format(datetime.now().date().replace(month=1, day=1), '%Y%m%d')
    yesterday = format(datetime.now() - timedelta(days=1), '%Y%m%d')

    # parameter date format dd/mm/yyyy for subject mail and body mail
    vs_parm_start = format(datetime.now().date().replace(month=1, day=1), '%d/%m/%Y')
    vs_parm_yest = format(datetime.now() - timedelta(days=1), '%d/%m/%Y')
    vs_parm_date = "{}-{}".format(vs_parm_start, vs_parm_yest)

    fileName = "{}_{}-{}.xls".format(dfltVal[1], starting_day_of_current_year, yesterday)

    # logging.info("Generate Data to Excel File Start")
    # genData2Xls(dfltVal[0], fileName)
    # logging.info("Generate Data to Excel File Finish")

    logging.info("Send Mail Start")
    sender = 'no-reply@apthai.com'
    receivers = dfltVal[2].split(';')

    subject = "{} ({})".format(dfltVal[3], vs_parm_date)
    bodyMsg_tmp = dfltVal[4].replace("PERIOD_MONTH", vs_parm_date)
    bodyMsg = "{}{}".format(bodyMsg_tmp, dfltVal[5])

    logging.debug("receivers = {}".format(receivers))
    logging.debug("subject = {}".format(subject))
    logging.debug("fileName = {}".format(fileName))
    logging.debug("bodyMsg = {}".format(bodyMsg))

    attachedFile = [fileName]

    send_email(subject, bodyMsg, sender, receivers, attachedFile)
    logging.info("Successfully sent email")


if __name__ == '__main__':
    # Get Default Parameter from DB
    dfltVal = getDfltParam()

    log_path = dfltVal[6]
    logFile = log_path + '\SendMailBG1GrossCancel.log'

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-5s [%(levelname)-8s] >> %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main(dfltVal)

    # # Delete Excel File in current path execution
    # logging.info('Delete Execl File')
    # deleteXLSFile()
    # logging.info('End Process')
    logging.debug('#####################')
