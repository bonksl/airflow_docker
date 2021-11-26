import boto3
import uuid
import os
import pandas as pd
from io import BytesIO
import pandas as pd
import os
from os import getenv
from dotenv import load_dotenv
import mygeotab as mg

import psycopg2
from io import StringIO
import config2

from datetime import datetime, timedelta
import time

from psycopg2 import OperationalError, errorcodes, errors
from sqlalchemy import create_engine

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def main():

    print("Starting")
    # GETTING CREDENTIALS
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    client = mg.API(username=config2.EMAIL, 
                    password=config2.PASSWORD,
                    database=config2.DBNAME)
    client.authenticate();

    # IMPORT FLEET
    fleet = pd.read_csv('/opt/airflow/dags/scripts/full_fleet.csv')



    # PULLING DAILY LOGS
    logs=pd.DataFrame()
    df_logs_temp=pd.DataFrame()
    start_time = time.time()
    today = datetime.today()
    yesterday = today-timedelta(1)

    print(f"------Start Time = {today.year}-{today.month}-{today.day} {today.hour}:{today.minute}:{today.second}------")
    print(" ")
    print(" ")
    print("Pulling LOGS from API")
    for x in fleet.DeviceId:
        log = client.get('LogRecord',
                           search={'fromDate':str(yesterday)[:10],
                                   'toDate':str(yesterday)[:10]+" 23:59:59.999",
                                   'deviceSearch':{'id':x},
                                   #'diagnosticSearch':{'id':'diagnosticEngineRoadSpeedId'}
                                   })
        logs_df=pd.DataFrame(log)
        logs=logs.append(logs_df)
    print(" ")
    print(" ")
    print("Finish pulling LOGS from API")
    logs['device']=logs.device.astype('str').apply(lambda x:x.strip("{,},:,',id, '"))
    #templogs=logs.copy()
    logs.dropna(inplace=True)
    logs.sort_values(by='dateTime',inplace=True)
    logs.to_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_logs.csv')
    print(" ")
    print(" ")
    print(f"Finish writing {str(yesterday)[:10]}, Logs")
    print(" ")
    print(" ")







    # PULLING TRIP TABLES
    print("Pulling trips from API")
    print(" ")
    print(" ")
    trips=pd.DataFrame()
    df_trip_temp=pd.DataFrame()
    for x in fleet.DeviceId:
        trip = client.get('Trip',
                           search={'fromDate':str(yesterday)[:10],
                                   'toDate':str(yesterday)[:10]+" 23:59:59.999",
                                   'deviceSearch':{'id':x},
                                   #'diagnosticSearch':{'id':'diagnosticEngineRoadSpeedId'}
                                   })
        trip_df=pd.DataFrame(trip)
        trips=trips.append(trip_df)
    print("Finished pulling trips from API")
    print(" ")
    print(" ")
    df_trip_temp=trips[['distance','workStopDuration','workDrivingDuration','nextTripStart','device']]
    df_trip_temp['device']=df_trip_temp.device.astype('str').apply(lambda x:x.strip("{,},:,',id, '"))
    df_trip_temp.rename(columns={'workStopDuration': 'workstopduration','workDrivingDuration': 'workdrivingduration',
                                'nextTripStart': 'nexttripstart','device':'deviceid'}, inplace=True)
    df_trip_temp.to_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_trips.csv')
    print(f"Finished writing {str(yesterday)[:10]} trips table")
    print(" ")
    print(" ")








    # PULLING FUEL USED
    print("Pulling FUEL USED from API")
    print(" ")
    print(" ")
    totalFuelUsedIds=pd.DataFrame()
    for x in fleet.DeviceId:
        totalFuelUsedId = client.get('StatusData',
                           search={'fromDate':str(yesterday)[:10],
                                   'toDate':str(yesterday)[:10]+" 23:59:59.999",
                                   'deviceSearch':{'id':x},
                                   'diagnosticSearch':{'id':'diagnosticTotalFuelUsedId'}
                                   })
        totalFuelUsedId_df=pd.DataFrame(totalFuelUsedId)
        totalFuelUsedIds=totalFuelUsedIds.append(totalFuelUsedId_df)

    print("Finished pulling FUEL USED from API")
    print(" ")
    print(" ")
    df_fuel_temp=totalFuelUsedIds[['data','dateTime','device']]
    df_fuel_temp['device']=df_fuel_temp.device.astype('str').apply(lambda x:x.strip("{,},:,',id, '"))
    df_fuel_temp.rename(columns={'dateTime': 'datetime', 'device': 'deviceid'}, inplace=True)
    df_fuel_temp.to_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_fuelused.csv')
    print(f"Finished writing {str(yesterday)[:10]} fuel used table")
    print(" ")
    print(" ")







    # PULLING HARSH BRAKES
    print("Pulling harshbrakes from API")
    print(" ")
    print(" ")
    harshbrakess=pd.DataFrame()
    for x in fleet.DeviceId:
        harshbrakes = client.get('ExceptionEvent',search={'fromDate':str(yesterday)[:10],
                                           'toDate':str(yesterday)[:10]+" 23:59:59.999",
                                           'deviceSearch':{'id':x},
                                           #'deviceSearch':{'id':'b46'},
                                           'ruleSearch':{'id':'ruleHarshBrakingId'}
                                           },
                      resultsLimit=100000)
        harshbrakes_df=pd.DataFrame(harshbrakes)
        harshbrakess=harshbrakess.append(harshbrakes_df)

    print("Finished pulling trips from API")
    print(" ")
    print(" ")

    harshbrakess['device']=harshbrakess.device.astype('str').apply(lambda x:x.strip("{,},:,',id, '"))
    harshbrakess.rename(columns={'activeFrom': 'dateTime'}, inplace=True)
    harshbrakess.dropna(inplace=True)
    harshbrakess=harshbrakess[['dateTime','distance','device']]
    harshbrakess.sort_values(by='dateTime', inplace=True)

    temp_merge_df = pd.merge_asof(harshbrakess, logs, on='dateTime', by='device')
    temp_merge_df.rename(columns={'dateTime':'activefrom'}, inplace=True)
    temp_merge_df.to_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_harshbrakes_logs.csv')

    print(f"Finished writing {str(yesterday)[:10]} harshbrakes table")
    print(" ")
    print(" ")








    # PULLING SEATBELT
    print("Pulling seatbelt from API")
    print(" ")
    print(" ")

    # CREATING SEATBELT LOGS
    seatbelt_df=pd.DataFrame()
    # To get fuel data, use the following example, more diagnosticSearch id can be found in pdf
    for x in fleet.DeviceId:
        seatbelts = client.get('StatusData',search={'fromDate':str(yesterday)[:10],
                                           'toDate':str(yesterday)[:10]+" 23:59:59.999",
                                           'deviceSearch':{'id':x},
                                           #'deviceSearch':{'id':'b46'},
                                           'diagnosticSearch':{'id':'diagnosticDriverSeatbeltId'}
                                           },
                      resultsLimit=100000)
        seatbelt_temp=pd.DataFrame(seatbelts)
        seatbelt_df=seatbelt_df.append(seatbelt_temp)
    print("Finished pulling seatbelt from API")
    print(" ")
    print(" ")

    seatbelt_df.device=seatbelt_df.device.astype('str').apply(lambda x:x.strip("{,},:,',id, '"))
    seatbelt_df.rename(columns={'activeFrom': 'dateTime'}, inplace=True)
    seatbelt_df.dropna(inplace=True)
    seatbelt_df = seatbelt_df[['data','dateTime','device']]
    seatbelt_df.sort_values(by='dateTime', inplace=True)

    temp_seatbeltlog = pd.merge_asof(seatbelt_df, logs, on='dateTime', by='device')
    temp_seatbeltlog.rename(columns={'dateTime': 'datetime'}, inplace=True)
    temp_seatbeltlog[temp_seatbeltlog.data==1].to_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_seatbelt_logs.csv')

    print(f"Finished writing {str(yesterday)[:10]} seatbelt table")
    print(" ")
    print(" ")



if __name__ == '__main__':
    main()










