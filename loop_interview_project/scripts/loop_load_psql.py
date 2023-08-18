import sys
import pandas as pd
import psycopg
from psycopg import sql
from io import StringIO
from datetime import datetime

def connect(parameters):
    conn=None
    try:
        print('Connection to the PostgreSQL database...')
        conn=psycopg.connect(**parameters)
    except (Exception, psycopg.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

def log(message):
    timestamp_format='%Y-%h-%d-%H-%M-%S'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open ('logfile.txt','a') as f:
        f.write(timestamp+','+message+'\n')

def load(conn,df,table):
    buffer=StringIO()
    df.to_csv(buffer,header=False,index=False)
    buffer.seek(0)
    cur=conn.cursor()
    
    #Creating a tmp table in the database
    cur.execute(f"CREATE TEMP TABLE tmp_table (LIKE {table})")
    
    #Loading the data from buffer into the tmp table
    try:
        with buffer as f:
            with cur.copy(sql.SQL("COPY tmp_table FROM STDIN WITH (FORMAT CSV)")) as copy:
                while data := f.read(1000000):  #Random block size 1000000
                    copy.write(data)

        #Copying from tmp_table to final table using INSERT ON CONFILCT DO NOTHING to avoid error due to duplicate values

        cur.execute(f"INSERT INTO {table} SELECT * FROM tmp_table ON CONFLICT DO NOTHING;")

        #Droping the tmp table
        cur.execute(f"DROP TABLE tmp_table;")
        conn.commit()

        print("Loading of csv  in database table ",table)


    except(Exception,psycopg.DatabaseError) as error:
        print('Error:%s',error)
        conn.rollback()
        cur.close()

    cur.close()

def extract_transform():
    data_status=pd.read_csv("../datasets/store status.csv")
    data_menu=pd.read_csv("../datasets/Menu hours.csv")
    data_timezone=pd.read_csv("../datasets/bq-results-20230125-202210-1674678181880.csv")    
    data=[data_status,data_menu,data_timezone]
    return data

if __name__=="__main__":
    
    tables=['store_status','menu_hours','store_timezone']

    param_dic={'host':'localhost',
               'port':5432,
               'user':'kali',
               'password':'password',
               'dbname':'loop_interview'
               }

    log("Connecting to Database...")
    conn=connect(param_dic)

    log("Extraction Started")
    data=extract_transform()
    log("Extraction Complete")
    
    
    log("Loading Store Status Data into store_status Table")
    load(conn,data[0],'store_status')
    log("Loading of store_status Table Complete")

    log("Loading menu hours into menu_hours Table")
    load(conn,data[1],'menu_hours')    
    log("Loading of menu_hours Table Complete")

    log("Loading store timezone into store_timezone Table")
    load(conn,data[2],'store_timezone')    
    log("Loading of menu_hours Table Complete")
    
    log("Process Complete")