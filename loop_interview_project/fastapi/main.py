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


# Connection for PostgreSQL using psycopg3
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

#Defining Queries to get data from PostgreSQL database based on current time
def queries():
    current_time=datetime.now().strftime("%Y-%m-%d %H:00:00")

    last_hour= (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
    last_hour_query= r"select * from store_status where timestamp_utc::timestamp between '"+last_hour+r"' and '"+current_time+r"' ;"

    last_day=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:00:00")
    last_day_query=r"select * from store_status where timestamp_utc::timestamp between '"+last_day+r"' and '"+current_time+r"' ;"

    last_sunday=(datetime.now() - timedelta(days=datetime.now().weekday()-6,weeks=1)).strftime("%Y-%m-%d %H:00:00")
    last_monday=(datetime.now() - timedelta(days=datetime.now().weekday(),weeks=1)).strftime("%Y-%m-%d %H:00:00")
    last_week_query=r"select * from store_status where timestamp_utc::timestamp between '"+last_monday+r"' and '"+last_sunday+r"' ;"

    queries=[last_week_query,last_day_query,last_hour_query]
    return queries


# Defining Static Queries for Demonstration Purpose
def queries_demo():
    last_week_query=f"select * from store_status where timestamp_utc::date between '2023-01-15' and '2023-01-25';"
    last_day_query=f"select * from store_status where timestamp_utc::date = '2023-01-22';"
    last_hour_query=f"select * from store_status where timestamp_utc::timestamp between '2023-01-25 06:00:00' and '2023-01-25 07:00:00';"

    queries=[last_week_query,last_day_query,last_hour_query]
    return queries
    

# Loading data from database
def data_extraction(connection):


    queries=queries_demo()  #Calling Static Queries

    conn=connection
    data_status_columns=["store_id", "status", "timestamp_utc"]
       
    week_query=queries[0]
    data_status_week= postgresql_to_dataframe(conn,week_query, data_status_columns)
    
    
    day_query=queries[1]
    data_status_day= postgresql_to_dataframe(conn,day_query , data_status_columns)

    
    hour_query=queries[2]
    data_status_hour= postgresql_to_dataframe(conn,hour_query, data_status_columns)
   
    
    # Loading Data for menu and timezone
    data_menu_columns=["store_id", "day", "start_time_local","end_time_local"]
    data_menu= postgresql_to_dataframe(conn, "select * from menu_hours;", data_menu_columns)

    data_timezone_columns=['store_id','timezone_str']
    data_timezone= postgresql_to_dataframe(conn, "select * from store_timezone;",data_timezone_columns)

    data=[data_status_week,data_status_day, data_status_hour ,data_menu,data_timezone]

    return data
    

   
# Processing Data

def combining(data,period):

    #Drop Duplicates    
    data_menu=data[3].drop_duplicates()
    data_timezone=data[4].drop_duplicates()
    status_df=data[period].drop_duplicates()

    #Merging Store data with respective timezones
    data_status=status_df.merge(data_timezone, on="store_id")
    data_status['timezone_str'].replace({np.nan:'America/Chicago'})
    
    #Localizing store activity time with respective timezone
    data_status['timestamp_test1']=data_status.apply(lambda row: row['timestamp_utc'].tz_convert(row['timezone_str']), axis=1)

    #Extracting weekday from each activity record for merging with business hours of each store on respective weekday
    data_status['day']=data_status.apply(lambda row: row['timestamp_test1'].weekday(), axis=1)

    #Merging activity data with business hours data
    data_status=data_status.merge(data_menu, on=['store_id','day'])

    #Extracting 'hour' data from activity
    data_status['hour']=data_status.apply(lambda row: row['timestamp_test1'].hour, axis=1)        
    data_status['hour']=pd.to_datetime(data_status['hour'], format='%H')

    #Converting Data type
    data_status['start_time_local']=pd.to_datetime(data_status['start_time_local'],format="%H:%M:%S")
    data_status['end_time_local']=pd.to_datetime(data_status['start_time_local'],format="%H:%M:%S")

    #Cleaning data for records restricted to business hours
    data_status=data_status[ (data_status['hour']>= data_status['start_time_local']) & (data_status['hour']<=data_status['end_time_local'])].reset_index(drop=True)
    
    #Creating dummy variable to calculate hours
    data_status['status_inactive']=0
    data_status['status_inactive']=pd.get_dummies(data_status['status'])['inactive']
    data_status['status_active']=0
    data_status['status_active']=pd.get_dummies(data_status['status'])['active']

    #Calculating active and inactive hours
    data_status=data_status.groupby(['store_id']).agg({'status_active':sum,'status_inactive':sum}).reset_index()

    #Renaming Columns
    if period==0:
        data_status.rename(columns={'status_active':'uptime_last_week(in hours)', 'status_inactive':'downtime_last_week(in hours)'},inplace=True)

    if period==1:
        data_status.rename(columns={'status_active':'uptime_last_day(in hours)', 'status_inactive':'downtime_last_day(in hours)'},inplace=True)

    if period==2:
        #Converting hours to minutes for hourly analysis
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

    #Merging data for week, day, hour
    store_status=data_status_week.merge(data_status_day.merge(data_status_hour, on='store_id'), on='store_id').sort_values('store_id')
    
    
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

    

