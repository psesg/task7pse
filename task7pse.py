import os
import platform
import sys
import pandas as pd
import numpy as np
import jaydebeapi
from sqlalchemy import types

pd.set_option("display.max_columns", 200)

plat = platform.system()
print("Common info:\nOS name:\t{}\nplatform:\t{}\nversion:\t{}\nrelease:\t{}\nPython v.:\t{}.{}.{}".format(
    os.name,
    plat,
    platform.version(),
    platform.release(),
    sys.version_info.major,
    sys.version_info.minor,
    sys.version_info.micro
))

if plat == "Linux":
    jarFile = '/home/demipt2/ojdbc8.jar'
    xlsFile = '/home/demipt2/medicine.xlsx'
    print("Unix-specific info: {}".format(platform.linux_distribution()))
if plat == "Windows":
    jarFile = r'C:\sqldeveloper\jdbc\lib\ojdbc8.jar'
    xlsFile = 'medicine.xlsx'
dirver = 'oracle.jdbc.driver.OracleDriver'
addr_ = 'de-oracle.chronosavant.ru' + ':' + '1521' + '/' + 'deoracle'
url = 'jdbc:oracle:thin:@' + addr_
#print('url', url)
DBUser = 'demipt2'
DBPwd = 'peregrintook'
conn = jaydebeapi.connect(dirver, url, [DBUser, DBPwd], jarFile)
conn.jconn.setAutoCommit(False)
curs = conn.cursor()

# read from XLS
df = pd.read_excel(xlsFile, sheet_name ='hard', header=0, index_col = None)

# rename fields
df.rename({'Код пациента': 'PAT_CODE', 'Анализ': 'AN_CODE', 'Значение': 'VAL'}, axis=1, inplace=True)

# add extra field, clear and set type of data
df = df.assign(SIMPL = None)
conditions = [
    (df['VAL'] == 'Отриц.') | (df['VAL'] == 'Отр') | (df['VAL'] == '-'),
    (df['VAL'] == 'Положительно') | (df['VAL'] == 'Положит.') | (df['VAL'] == '+') ]
choices = ['N', 'Y']
choicesval = [None, None]
df['SIMPL'] = np.select(conditions, choices, default=None)
df['VAL'] = np.select(conditions, choicesval, default=df['VAL'])
df['VAL'] = df['VAL'].astype(float)
df['PAT_CODE'] = df['PAT_CODE'].astype(int)

# set Nan to None for insert Null into database
df = df.astype(object)
df = df.where(pd.notnull(df), None)

# https://stackoverflow.com/questions/41566950/how-to-make-df-to-sql-create-varchar2-object
#dtyp = {c:types.VARCHAR(df[c].str.len().max()) for c in df.columns[df.dtypes == 'object'].tolist()}

#print(df)
print(df.values.tolist())

# delete data if exist
sql_str = "delete from DEMIPT2.PANA_XLS"
print("\ndeleting from  DEMIPT2.PANA_XLS...\n'{}'".format(sql_str))
try:
    curs.execute(sql_str)
except Exception as e:
    print("Error deleting:{}".format(e))
else:
    conn.commit()
finally:
    print("deleted {} rows".format(curs.rowcount))

# insert data from DataFrame
sql_str = "insert into DEMIPT2.PANA_XLS (PAT_CODE, AN_CODE, VAL, SIMPL) values (?,?,?,?)"
print("\ninserting to  DEMIPT2.PANA_XLS...\n'{}'".format(sql_str))
try:
    curs.executemany(sql_str, df.values.tolist())
except Exception as e:
    print("Error insertion:{}".format(e))
else:
    conn.commit()
finally:
    print("inserted {} rows".format(curs.rowcount))

curs.close()
conn.close()
