#https://api.nasa.gov/DONKI/CMEAnalysis?mostAccurateOnly=true&speed=500&halfAngle=30&catalog=ALL&api_key=DEMO_KEY
#API from https://api.nasa.gov/

import requests
import json
import pandas as pd
from pandas.core.reshape.merge import string
from datetime import datetime
from tabulate import tabulate

def date_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%dT%H:%MZ').strftime('%Y-%m-%d')

def time_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%dT%H:%MZ').strftime('%H:%M:%S')

#get todays date
today = datetime.today().strftime('%Y-%m-%d')

#get data from api
response = requests.get('https://api.nasa.gov/DONKI/CMEAnalysis?startDate=2019-01-01&endDate='+today+'&mostAccurateOnly=true&speed=500&halfAngle=30&catalog=ALL&api_key=DEMO_KEY')

#handle api response
if response.status_code == 200:
    data = json.loads(response.text)
    print('Data retrieved from API:')
    print(data)
else:
    print(f"Error retrieving data, status code: {response.status_code}")

#load data into DataFrame
dataFrame = pd.DataFrame(data)

#rename some columns and some format for database upload
dataFrame.rename(columns = {'time21_5':'datetime_event', 'type':'type_event', 'catalog':'catalog_event'}, inplace = True)

dataFrame['date_event'] = dataFrame['datetime_event'].apply(date_convert)
dataFrame['time_event'] = dataFrame['datetime_event'].apply(time_convert)

dataFrame = dataFrame.astype({'latitude':'int','longitude':'int','halfAngle':'int','speed':'int'})
dataFrame = dataFrame.astype({'datetime_event':'string','type_event':'string','catalog_event':'string','associatedCMEID':'string','link':'string'})

print('\nData frame information:')
print(tabulate(dataFrame, headers='keys', tablefmt='psql')) 

#upload to redshift, remember to execute "TableCreation.sql" first
import redshift_connector

conn = redshift_connector.connect(
   host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
   database='data-engineer-database',
   port=5439,
   user='dummyUsername',
   password='dummyPassword'
)

conn.autocommit = True
cursor = conn.cursor()
cursor.write_dataframe(dataFrame, 'amchavezaltamirano_coderhouse.coronal_mass_ejection')
#check that the information is in the db
cursor.execute("select * from amchavezaltamirano_coderhouse.coronal_mass_ejection;")
result = cursor.fetchall()
print(tabulate(result, headers='keys', tablefmt='psql'))