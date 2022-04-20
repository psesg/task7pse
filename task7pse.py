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

# make query for task 7.1
# Вы забираете данные с листа 'hard'. Нужно отыскать пациентов, у которых не
# в норме два и более анализов. Вывести телефон, имя, название анализа и
# заключение 'Повышен', 'Понижен' или 'Положительный'. Сохранить в xlsx.

sql_str = """
SELECT * FROM
(
SELECT 
    --tr.id,
    tr.phone,
    tr.name client,
    a.name an_name,
    case
        when a.simple is not null and  tr.simpl is not null and  a.simple = 'Y' and  tr.simpl = 'Y' then 'Положительный'
        when tr.val is not null and  tr.val < a.min_value then 'Понижен' 
        when tr.val is not null and  tr.val > a.max_value then 'Повышен' 
        else 'Норма'
    end  as res
FROM
    (SELECT 
        t.id,
        t.name,
        t.phone,
        r.pat_code,
        r.an_code,
        r.val,
        r.simpl
    FROM DE.MED_NAMES t
    LEFT JOIN DEMIPT2.PANA_XLS r
        ON t.id = r.pat_code
    ORDER BY t.id
    ) tr
    LEFT JOIN DE.MED_AN_NAME a
        ON tr.an_code = a.code
WHERE tr.id in (
--
    SELECT 
        resul.id
    FROM
    (
    SELECT 
        tr.id,
        tr.phone,
        tr.name client,
        tr.an_code,
        a.name an_name,
        case
            when a.simple is not null and  tr.simpl is not null and  a.simple = 'Y' and  tr.simpl = 'Y' then 'Положительный'
            when tr.val is not null and  tr.val < a.min_value then 'Понижен' 
            when tr.val is not null and  tr.val > a.max_value then 'Повышен' 
            else 'Норма'
        end  as res,
        
        case
            when a.simple is not null and  tr.simpl is not null and  a.simple = 'Y' and  tr.simpl = 'Y' then 1
            when tr.val is not null and  tr.val < a.min_value then 1 
            when tr.val is not null and  tr.val > a.max_value then 1 
            else 0
        end  as kol
        
    FROM
        (SELECT 
            t.id,
            t.name,
            t.phone,
            r.pat_code,
            r.an_code,
            r.val,
            r.simpl
        FROM DE.MED_NAMES t
        LEFT JOIN DEMIPT2.PANA_XLS r
            ON t.id = r.pat_code
        ORDER BY t.id
        ) tr
        LEFT JOIN DE.MED_AN_NAME a
            ON tr.an_code = a.code
        ) resul
    GROUP BY resul.id
    HAVING sum(resul.kol) >= 2
--
    )
)
WHERE res <> 'Норма'
"""
print("\ngetting from  database...\n")
try:
    curs.execute(sql_str)
except Exception as e:
    print("Error getting data:{}".format(e))
else:
    df = pd.DataFrame(curs.fetchall(), columns=[x[0] for x in curs.description])
    print(df)


curs.close()
conn.close()
