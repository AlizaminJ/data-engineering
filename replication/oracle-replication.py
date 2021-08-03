import pyodbc
import cx_Oracle
import time
import os
import pickle
import math
import pandas as pd

# CONNECTION PARAMETERS
server = '<SERVER>'
service_name = '<SERVICE>'
SID = '<SID>'
database = '<DATABASE>'
username = '<USERNAME>'
password = '<PASSWORD>'

# TABLE PARAMATERS
# note tables to replicated with its replication key (usually modified, updated) in the dictionary below
table = {
  'table1':'id',
  'table2':'id
    }

# a folder to cache replication parameter
cachedir = 'cachedir'
# a folder to save files
savedir = 'savedir'


def connection():
    try:
        cnxn = cx_Oracle.connect(fr'{username}/{password}@{server}/{service_name}')
        return cnxn
    except Exception as e:
        print(e)


def replication():
    file = open(os.path.join(cachedir, database.lower() + '_' + t.lower()), 'rb')
    max_value = pickle.load(file)
    file.close()
    print("replication starts ...")
    print("replication parameter is detected")
    print(f"processing table {t} ...")
    print(f"current max value: {max_value}")
    sql = f'SELECT * from {database}.{t} WHERE {id} > %d ORDER BY {id} ASC' % max_value
    query = pd.read_sql_query(sql, cnxn)
    df = pd.DataFrame(query)
    max_value = df[df.columns[0]].max()
    if math.isnan(max_value):
        print(f"no new rows in {t}")
        print("done")
        print("============================")
    elif max_value > 0:
        try:
            print(f"{df.shape[0]} rows fetched")
            file_to_save = database.lower() + '_' + t.lower() + '_' + time.strftime(
                '%Y_%m_%d_%H_%M_%S') + '.txt'  # ('%Y_%m_%d-%I_%M_%S_%p')   #("%Y%m%d-%H%M%S")
            directory = t.lower()
            # parent_dir = 'aunivers-superoffice'

            subdirectory = os.path.join(savedir, directory)

            if os.path.isdir(subdirectory):
                pass
            else:
                os.mkdir(subdirectory)
                print(f"subdirectory for {t} created in {savedir}")

            df.to_csv(os.path.join(subdirectory, file_to_save), sep='|', index=False)
            print(f'{file_to_save} exported as a file')
            print(f"replication key for {t}: " + df.columns[0])
            print(f"new max value: {max_value}")
            file_to_cache = open(os.path.join(cachedir, database.lower() + '_' + t.lower()), 'wb')
            pickle.dump(max_value, file_to_cache)
            file_to_cache.close()
            print("done")
            print("============================")
        except Exception as e:
            print(e)
    else:
        print("============================")
        print("something got wrong...")
        print("============================")


if __name__ == '__main__':
    try:
        print("============================")
        print("connecting ...")
        cnxn = connection()
        print("connected")
        print("============================")
        if os.path.isdir(savedir):
            pass
            print(f"save directory '{savedir}' exists")
        else:
            os.mkdir(savedir)
            print(f"save directory '{savedir}' created")

        if os.path.isdir(cachedir):
            pass
            print(f"cache directory '{cachedir}' exists")
        else:
            os.mkdir(cachedir)
            print(f"cache directory '{cachedir}' created")
        print("============================")
        for t, id in table.items():
            try:
                replication()
            except:
                print(f"replication key for {t}: {id}")
                print("no previous replication parameter is detected, starting from zero ...")
                max_value = 0
                print(f"max value: {max_value}")
                file_to_cache = open(os.path.join(cachedir, database.lower() + '_' + t.lower()), 'wb')
                pickle.dump(max_value, file_to_cache)
                file_to_cache.close()
                print("replication parameter created")
                replication()
        cnxn.close()
    except Exception as e:
        print(e)
