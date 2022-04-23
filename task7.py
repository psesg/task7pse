import os
import platform
import sys
import pandas as pd
import numpy as np
import jaydebeapi
from sqlalchemy import types

pd.set_option("display.max_columns", 200)

def get_sql_str(filename):
    sqlstr = ''
    try:
        f = open(filename)
    except FileNotFoundError:
        print("File does not exist '{}'".format(filename))
    else:
        sqlstr = f.read()
        f.close()
    finally:
        return sqlstr

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
    sqlscrFile1 = '/home/demipt2/pana/task7_1.sql'
    sqlscrFile2 = '/home/demipt2/pana/task7_2.sql'
    # file in my PANA dir
    task7file = '/home/demipt2/pana/pana_task7_out.xlsx'
    print("Unix-specific info: {}".format(platform.linux_distribution()))
if plat == "Windows":
    jarFile = r'C:\sqldeveloper\jdbc\lib\ojdbc8.jar'
    # file in current dir
    xlsFile = 'medicine.xlsx'
    task7file = 'pana_task7_out.xlsx'
    sqlscrFile1 = 'task7_1.sql'
    sqlscrFile2 = 'task7_2.sql'
dirver = 'oracle.jdbc.driver.OracleDriver'
addr_ = 'de-oracle.chronosavant.ru' + ':' + '1521' + '/' + 'deoracle'
url = 'jdbc:oracle:thin:@' + addr_
#print('url', url)
DBUser = 'demipt2'
DBPwd = 'peregrintook'
conn = jaydebeapi.connect(dirver, url, [DBUser, DBPwd], jarFile)
conn.jconn.setAutoCommit(False)
curs = conn.cursor()

# make query for task 7.1
# Вы забираете данные с листа 'hard'. Нужно отыскать пациентов, у которых не
# в норме два и более анализов. Вывести телефон, имя, название анализа и
# заключение 'Повышен', 'Понижен' или 'Положительный'. Сохранить в xlsx.

# read from XLS
print("\nread data from XLS file: {}...".format(xlsFile))
df = pd.read_excel(xlsFile, sheet_name ='hard', header=0, index_col = None)
print("making dataframe for put into database and show df.head(5)...")
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

print(df.head(5))
#print(df.values.tolist())

# put data from xls to DEMIPT2.PANA_XLS table
# delete data if exist
sql_str = "delete from DEMIPT2.PANA_XLS"
print("\ndeleting from table DEMIPT2.PANA_XLS...")
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
print("\nputting dataframe to table DEMIPT2.PANA_XLS...")
try:
    curs.executemany(sql_str, df.values.tolist())
except Exception as e:
    print("Error insertion:{}".format(e))
else:
    conn.commit()
finally:
    print("inserted {} rows".format(int(-curs.rowcount/2)))

sql_str = get_sql_str(sqlscrFile1)
#print(sql_str)

print("\nExecuting query and getting result dataframe...")
try:
    curs.execute(sql_str)
except Exception as e:
    print("Error getting data:{}".format(e))
else:
    df = pd.DataFrame(curs.fetchall(), columns=[x[0] for x in curs.description])
    print("Show result dataframe:\n")
    print(df)
    print("\nwrite dataframe to {}...".format(task7file))
    df.to_excel(task7file, sheet_name='task7', header=True, index=False)


# insert data from query (task7 additional)
# delete data if exist
sql_str = "delete from DEMIPT2.PANA_MEDAN_DECODE_RES"
print("\ndeleting from table DEMIPT2.PANA_MEDAN_DECODE_RES...")
try:
    curs.execute(sql_str)
except Exception as e:
    print("Error deleting:{}".format(e))
else:
    conn.commit()
finally:
    print("deleted {} rows".format(curs.rowcount))

sql_str = get_sql_str(sqlscrFile2)
#print(sql_str)

print("\ninserting to table DEMIPT2.PANA_MEDAN_DECODE_RES...")
try:
    curs.execute(sql_str)
except Exception as e:
    print("Error insertion:{}".format(e))
else:
    conn.commit()
finally:
    print("inserted {} rows".format(curs.rowcount))


curs.close()
conn.close()
