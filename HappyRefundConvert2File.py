from sqlalchemy import create_engine
import urllib
import sqlalchemy
import pyodbc
import pandas as pd
import numpy as np
import base64

params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.75;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;'
params = urllib.parse.quote_plus(params)

db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)
str_sql = """
SELECT img_name AS FileName, img_file AS FileObject FROM dbo.crm_refund_docref 
--WHERE img_name = 'S__18956292.jpg'
WHERE img_ref_contact_refund = 10653
      """

data = pd.read_sql(sql=str_sql, con=db)

for index, row in data.iterrows():
    print("Object = {}".format(row['FileObject']))
    print("File name = {}".format(row['FileName']))

    value = row['FileObject']
    filename = r"C:\Users\suchat_s\PycharmProjects\ConvertBinary2PDF\img\{}".format(row['FileName'])
    with open(filename, "wb") as fh:
          fh.write(base64.decodebytes(value))

