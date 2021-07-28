import math
import os
import pickle
import pandas as pd
import time
import pyodbc
pyodbc.pooling = False
from sqlalchemy import create_engine

# CONNECTION PARAMETERS
server = '<SERVER>'
database = '<DATABSE>'
username = '<USERNAME>'
password = '<PASSWORD>'
driver = '<DRIVER>' # the driver has to be installed separately
schema = '<SCHEMA>'

# note tables to replicated with its replication key (usually modified, updated) in the dictionary below
table = {
    'sales': 'ROWNUMBER',
    'sales_hist': 'ROWNUMBER'
    }

# a folder to cache replication parameter
cachedir = '<FOLDER>'
# a folder to save files
savedir = '<FOLDER>'

def connection():
    try:
        con_str = ('mssql+pyodbc://{username}:{password}@{server}/{dbname}?driver={driver}'
                   .format(username=username,
                           password=password,
                           server=server,
                           dbname=database,
                           driver=driver))
        engine = create_engine(con_str)
        conn = engine.connect()
        return conn
    except Exception as e:
        print(e)


def replication(t, id):
    file = open(os.path.join(cachedir, database.lower() + '_' + t.lower()), 'rb')
    max_value = pickle.load(file)
    file.close()
    print("replication starts ...")
    print("replication parameter is detected")
    print(f"processing table {t} ...")
    print(f"current max value: {max_value}")
    try:
        query = f'SELECT * from {schema}.{t} WHERE {id} > %d ORDER BY {id} ASC' % max_value
        try:
            dfs = []
            i = 1
            for chunk in pd.read_sql_query(query, con=cnxn, chunksize=200000):
                # print('type: %s shape %s' % (type(chunk), chunk.shape))
                print('chunk number: %i, chunk shape: %s' % (i, chunk.shape))
                dfs.append(chunk)
                i = i + 1
            df = pd.concat(dfs)
        except:
            df = pd.read_sql_query(query, cnxn)
        max_value = df[id].max()

    except Exception as e:
        print(e)
    if math.isnan(max_value):
        print(f"no new rows in {t}")
        print("done")
        print("============================")
    elif max_value > 0:
        try:
            print(f"{df.shape[0]} rows fetched")
            file_to_save = database.lower() + '_' + t.lower() + '_' + time.strftime(
                '%Y_%m_%d_%H_%M_%S') + '.txt'  # ('%Y_%m_%d-%I_%M_%S_%p') or ("%Y%m%d-%H%M%S")
            directory = t.lower()
            subdirectory = os.path.join(savedir, directory)
            if os.path.isdir(subdirectory):
                pass
            else:
                os.mkdir(subdirectory)
                print(f"subdirectory for {t} created in {savedir}")
            df.to_csv(os.path.join(subdirectory, file_to_save), sep='|', index=False)
            print(f'{file_to_save} exported as a file')
            print(f"new max value for replication key {id}: {max_value}")
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
        # cnxn = connection()
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
                replication(t, id)
            except:
                print(f"replication key for {t}: {id}")
                print("no previous replication parameter is detected, starting from zero ...")
                max_value = 0
                print(f"max value: {max_value}")
                file_to_cache = open(os.path.join(cachedir, database.lower() + '_' + t.lower()), 'wb')
                pickle.dump(max_value, file_to_cache)
                file_to_cache.close()
                print("replication parameter created")
                replication(t, id)
                print("============================")
        cnxn.close()
    except Exception as e:
        print(e)
