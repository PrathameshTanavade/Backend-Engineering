from fastapi import FastAPI, Request , Response
from fastapi.responses import StreamingResponse
from fastapi.responses import FileResponse

import io
import sys
import numpy as np
import pandas as pd
import psycopg
from psycopg import sql
from datetime import datetime, timedelta

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
    
param_dic={'host':'localhost',
               'port':5432,
               'user':'kali',
               'password':'password',
               'dbname':'loop_interview'
               }


def postgresql_to_dataframe(conn, select_query, column_names):
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    
    tupples = cursor.fetchall()
    cursor.close()
    
    df = pd.DataFrame(tupples, columns=column_names)
    return df

def queries():
    current_time=datetime.now().strftime("%Y-%m-%d %H:00:00")

    last_hour= (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
    last_hour_query= r"select * from store_status where timestamp_utc::timestamp between '"+last_hour+r"' and '"+current_time+r"' ;"

    last_day=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:00:00")
    last_day_query=r"select * from store_status where timestamp_utc::timestamp between '"+last_day+r"' and '"+current_time+r"' ;"

    last_sunday=(datetime.now() - timedelta(days=datetime.now().weekday()-6,weeks=1)).strftime("%Y-%m-%d %H:00:00")
    last_monday=(datetime.now() - timedelta(days=datetime.now().weekday(),weeks=1)).strftime("%Y-%m-%d %H:00:00")
    last_week_query=r"select * from store_status where timestamp_utc::timestamp between '"+last_monday+r"' and '"+last_sunday+r"' ;"

    queries=[last_hour_query,last_day_query,last_week_query]
    return queries

def data_extraction(connection):


    #queries=queries()
    conn=connection
    data_status_columns=["store_id", "status", "timestamp_utc"]
    
    # For the purpose of demostration, all queries are static in time. But the function for dynamic queries is also implemented and is currently disable using comment

    week_query=r"select * from store_status where timestamp_utc::date between '2023-01-15' and '2023-01-25'"
    #week_query=queries[2]
    data_status_week= postgresql_to_dataframe(conn,week_query, data_status_columns)
    
    day_query=r"select * from store_status where timestamp_utc::date = '2023-01-22' ;"
    #day_query=queries[1]
    data_status_day= postgresql_to_dataframe(conn,day_query , data_status_columns)

    hour_query=r"select * from store_status where timestamp_utc::timestamp between '2023-01-25 06:00:00' and '2023-01-25 07:00:00';"
    #hour_query=queries[0]
    data_status_hour= postgresql_to_dataframe(conn,hour_query, data_status_columns)
   
    
    
    data_menu_columns=["store_id", "day", "start_time_local","end_time_local"]
    data_menu= postgresql_to_dataframe(conn, "select * from menu_hours;", data_menu_columns)

    data_timezone_columns=['store_id','timezone_str']
    data_timezone= postgresql_to_dataframe(conn, "select * from store_timezone;",data_timezone_columns)

    data=[data_status_week,data_status_day, data_status_hour ,data_menu,data_timezone]

    return data
    

   

def combining(data,period):
        
    data_menu=data[3].drop_duplicates()
    data_timezone=data[4].drop_duplicates()
    status_df=data[period].drop_duplicates()

    data_status=status_df.merge(data_timezone, on="store_id")
    data_status['timezone_str'].replace({np.nan:'America/Chicago'})

    data_status['timestamp']=0
    for i in range(0,data_status['store_id'].count()):
        data_status['timestamp'][i]=data_status['timestamp_utc'][i].tz_convert(data_status['timezone_str'][i])

    data_status['day']=0
    for i in range(0,data_status['store_id'].count()):
        data_status['timestamp'][i]=pd.to_datetime(data_status['timestamp'][i])
        data_status['day'][i]= data_status['timestamp'][i].weekday()

    data_status=data_status.merge(data_menu, on=['store_id','day'])
    data_status['timestamp']=data_status['timestamp'].replace({r'([\-,\+]\d+[:]\d+)':''},regex=True)
    data_status['hour']=0
    
    for i in range(0,data_status['store_id'].count()):
        data_status['hour'][i]=data_status['timestamp'][i].hour
        
    data_status['hour']=pd.to_datetime(data_status['hour'], format='%H')

    data_status['start_time_local']=pd.to_datetime(data_status['start_time_local'],format="%H:%M:%S")
    data_status['end_time_local']=pd.to_datetime(data_status['start_time_local'],format="%H:%M:%S")
    data_status=data_status[ (data_status['hour']>= data_status['start_time_local']) & (data_status['hour']<=data_status['end_time_local'])].reset_index(drop=True)
    
    data_status['status_inactive']=0
    data_status['status_inactive']=pd.get_dummies(data_status['status'])['inactive']
    data_status['status_active']=0
    data_status['status_active']=pd.get_dummies(data_status['status'])['active']
    data_status=data_status.groupby(['store_id']).agg({'status_active':sum,'status_inactive':sum}).reset_index()

    if period==0:
        data_status.rename(columns={'status_active':'uptime_last_week(in hours)', 'status_inactive':'downtime_last_week(in hours)'},inplace=True)

    if period==1:
        data_status.rename(columns={'status_active':'uptime_last_day(in hours)', 'status_inactive':'downtime_last_day(in hours)'},inplace=True)

    if period==2:
        data_status['status_active']=data_status['status_active']*60;
        data_status['status_inactive']=data_status['status_inactive']*60;
        data_status.rename(columns={'status_active':'uptime_last_hour(in minutes)', 'status_inactive':'downtime_last_hour(in minutes)'},inplace=True)
    
    return data_status



app = FastAPI()




@app.get("/trigger_report")
async def trigger_report():
    conn = connect(param_dic)
    data=data_extraction(conn)
    
    week=0
    day=1
    hour=2

    data_status_week= combining(data,week)
    data_status_day= combining(data,day)
    data_status_hour= combining(data,hour)

    store_status=data_status_week.merge(data_status_day.merge(data_status_hour, on='store_id'), on='store_id')
    
    
    stream = io.StringIO()
    store_status.to_csv(stream, index = False)

    filename=datetime.now().strftime("%Y%m%d-%H0000")+'.csv'
    
    response = StreamingResponse(iter([stream.getvalue()]),
                                 media_type="text/csv"                            
                                )
    response.headers["Content-Disposition"] = "attachment;filename=export.csv"

    app.state.response=response
    return print("Report Complete")

@app.get("/get_report")
async def get_report():
    return app.state.response

    

