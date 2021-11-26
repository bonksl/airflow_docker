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

from psycopg2 import OperationalError, errorcodes, errors
from sqlalchemy import create_engine

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import timeit
from datetime import datetime, timedelta
import time


def main():

	start_time = time.time()
	today = datetime.today()
	yesterday = today-timedelta(1)
	print(f"------Start Time = {today.year}-{today.month}-{today.day} {today.hour}:{today.minute}:{today.second}------")
	print(" ")
	print(" ")









	# TRIPS TO POSTGRES
	trips = pd.read_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_trips.csv')
	#start database connection
	connection = psycopg2.connect(
	    host = config2.host,
	    port = 5432,
	    user = config2.user,
	    password = config2.passwd,
	    database= config2.db_name
	    )
	cursor=connection.cursor()

	connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
	    config2.user,
	    config2.passwd,
	    config2.host,
	    config2.db_name)

	trips.drop(columns='Unnamed: 0', inplace=True)

	def to_alchemy(df):
	    """
	    Using a dummy table to test this call library
	    """
	    engine = create_engine(connect)
	    df.to_sql(
	        'trips', 
	        con=engine, 
	        index=False, 
	        if_exists='append'
	    )
	    print("to_sql() done (sqlalchemy)")
	    
	to_alchemy(trips)









	# TOTAL FUEL TO POSTGRES
	totalfuelused = pd.read_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_fuelused.csv')
	totalfuelused = totalfuelused[['data','datetime','deviceid']]

	def to_alchemy(df):
	    """
	    Using a dummy table to test this call library
	    """
	    engine = create_engine(connect)
	    df.to_sql(
	        'totalfuelused', 
	        con=engine, 
	        index=False, 
	        if_exists='append'
	    )
	    print("to_sql() done (sqlalchemy)")
	    
	to_alchemy(totalfuelused)









	# seatbelt TO POSTGRES
	seatbelt = pd.read_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_seatbelt_logs.csv')
	seatbelt.drop(columns='Unnamed: 0', inplace=True)
	seatbelt=seatbelt[['data','datetime','device']]
	#start database connection
	connection = psycopg2.connect(
	    host = config2.host,
	    port = 5432,
	    user = config2.user,
	    password = config2.passwd,
	    database= config2.db_name
	    )
	cursor=connection.cursor()

	connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
	    config2.user,
	    config2.passwd,
	    config2.host,
	    config2.db_name)

	def to_alchemy(df):
	    """
	    Using a dummy table to test this call library
	    """
	    engine = create_engine(connect)
	    df.to_sql(
	        'seatbelt', 
	        con=engine, 
	        index=False, 
	        if_exists='append'
	    )
	    print("to_sql() done (sqlalchemy)")
	    
	to_alchemy(seatbelt)










	# HARSHBRAKE TO POSTGRES
	harshbrakes = pd.read_csv(f'/opt/airflow/dags/daily/{str(yesterday)[:10]},_harshbrakes_logs.csv')
	harshbrakes.drop(columns='Unnamed: 0', inplace=True)
	harshbrakes=harshbrakes[['activefrom','distance','device']]
	#start database connection
	connection = psycopg2.connect(
	    host = config2.host,
	    port = 5432,
	    user = config2.user,
	    password = config2.passwd,
	    database= config2.db_name
	    )
	cursor=connection.cursor()

	connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
	    config2.user,
	    config2.passwd,
	    config2.host,
	    config2.db_name)

	def to_alchemy(df):
	    """
	    Using a dummy table to test this call library
	    """
	    engine = create_engine(connect)
	    df.to_sql(
	        'harshbrake', 
	        con=engine, 
	        index=False, 
	        if_exists='append'
	    )
	    print("to_sql() done (sqlalchemy)")
	    
	to_alchemy(harshbrakes)


if __name__ == '__main__':
    main()