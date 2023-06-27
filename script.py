#https://api.nasa.gov/DONKI/CMEAnalysis?mostAccurateOnly=true&speed=500&halfAngle=30&catalog=ALL&api_key=DEMO_KEY
#API from https://api.nasa.gov/

import requests
import json
import pandas as pd
from pandas.core.reshape.merge import string
from datetime import datetime
from tabulate import tabulate
import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine

#environment variables
load_dotenv()

AWS_REDSHIFT_USER = os.getenv('AWS_REDSHIFT_USER')
AWS_REDSHIFT_PASSWORD = os.getenv('AWS_REDSHIFT_PASSWORD')
AWS_REDSHIFT_HOST = os.getenv('AWS_REDSHIFT_HOST')
AWS_REDSHIFT_PORT = os.getenv('AWS_REDSHIFT_PORT')
AWS_REDSHIFT_DBNAME = os.getenv('AWS_REDSHIFT_DBNAME')
AWS_REDSHIFT_SCHEMA = os.getenv('AWS_REDSHIFT_SCHEMA')
AWS_REDSHIFT_URL = "postgresql+psycopg2://"+AWS_REDSHIFT_USER+":"+AWS_REDSHIFT_PASSWORD+"@"+AWS_REDSHIFT_HOST+":"+AWS_REDSHIFT_PORT+"/"+AWS_REDSHIFT_DBNAME
DRIVER_PATH = os.getenv('DRIVER_PATH')

#some auxiliary functions
def date_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%dT%H:%MZ').strftime('%Y-%m-%d')

def time_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%dT%H:%MZ').strftime('%H:%M:%S')

#get todays date
today = datetime.today().strftime('%Y-%m-%d')

result = requests.get('https://api.nasa.gov/DONKI/CMEAnalysis?startDate=2023-06-10&endDate='+today+'&mostAccurateOnly=true&speed=500&halfAngle=30&catalog=ALL&api_key=DEMO_KEY')

#handle api response
if result.status_code == 200:
    data = json.loads(result.text)
    print('Data retrieved from API:')
    print(data)
else:
    print(f"Error retrieving data, status code: {response.status_code}")

# Connect to Redshift using psycopg2
conn = psycopg2.connect(
    host=AWS_REDSHIFT_HOST,
    port=AWS_REDSHIFT_PORT,
    dbname=AWS_REDSHIFT_DBNAME,
    user=AWS_REDSHIFT_USER,
    password=AWS_REDSHIFT_PASSWORD
)
engine= create_engine(AWS_REDSHIFT_URL)
cursor = conn.cursor()
cursor.execute(f"""
create table if not exists {AWS_REDSHIFT_SCHEMA}.coronal_mass_ejection(
datetime_event text not null,
latitude float not null,
longitude float not null,
halfAngle float not null,
speed float not null,
type_event text not null,
isMostAccurate boolean not null,
associatedCMEID varchar(28),
note varchar(600),
catalog_event text,
link text,
date_event date not null,
time_event time not null,
id int identity(1,1),
primary key(id))
distkey(date_event)
compound sortkey(id, date_event,time_event);
""")
conn.commit()
cursor.close()
print("Table created!")

#load data into DataFrame
dataFrame = pd.DataFrame(data)

#rename some columns and some format for database upload
dataFrame.rename(columns = {'time21_5':'datetime_event', 'type':'type_event', 'catalog':'catalog_event'}, inplace = True)

dataFrame['date_event'] = dataFrame['datetime_event'].apply(date_convert)
dataFrame['time_event'] = dataFrame['datetime_event'].apply(time_convert)

dataFrame = dataFrame.astype({'latitude':'int','longitude':'int','halfAngle':'int','speed':'int'})
dataFrame = dataFrame.astype({'datetime_event':'string','type_event':'string','catalog_event':'string','associatedCMEID':'string','link':'string'})
dataFrame = dataFrame.drop_duplicates()

print('\nData frame information:')
print(tabulate(dataFrame, headers='keys', tablefmt='psql')) 

#upload to redshift
import redshift_connector

redshiftConn = redshift_connector.connect(
   host=AWS_REDSHIFT_HOST,
   database=AWS_REDSHIFT_DBNAME,
   port=5439,
   user=AWS_REDSHIFT_USER,
   password=AWS_REDSHIFT_PASSWORD
)

redshiftConn.autocommit = True
cursor = redshiftConn.cursor()
cursor.write_dataframe(dataFrame, 'amchavezaltamirano_coderhouse.coronal_mass_ejection')
print("Data inserted!")
#check that the information is in the db
cursor.execute("select * from amchavezaltamirano_coderhouse.coronal_mass_ejection;")
result = cursor.fetchall()
print(tabulate(result, headers='keys', tablefmt='psql'))
