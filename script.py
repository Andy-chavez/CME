#!pip install psycopg2-binary
import requests
import json
from datetime import datetime
from os import environ as env
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql.functions import when, lit, col
import psycopg2
from pyspark.sql.functions import col, udf, substring
from pyspark.sql.types import StringType

AWS_REDSHIFT_USER = env["REDSHIFT_USER"]
AWS_REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
AWS_REDSHIFT_HOST = env['REDSHIFT_HOST']
AWS_REDSHIFT_PORT = env['REDSHIFT_PORT']
AWS_REDSHIFT_DBNAME = env['REDSHIFT_DB']
AWS_REDSHIFT_SCHEMA = 'amchavezaltamirano_coderhouse'
DRIVER_PATH = "/home/coder/working_dir/spark_drivers/postgresql-42.5.2.jar"
REDSHIFT_URL = f"jdbc:postgresql://{AWS_REDSHIFT_HOST}:{AWS_REDSHIFT_PORT}/{AWS_REDSHIFT_DBNAME}?user={AWS_REDSHIFT_USER}&password={AWS_REDSHIFT_PASSWORD}"
env['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {DRIVER_PATH} --jars {DRIVER_PATH} pyspark-shell'
env['SPARK_CLASSPATH'] = DRIVER_PATH

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

# Create SparkSession 
spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Conexion entre Pyspark y Redshift") \
        .config("spark.jars", DRIVER_PATH) \
        .config("spark.executor.extraClassPath", DRIVER_PATH) \
        .getOrCreate()

# Connect to Redshift using psycopg2
conn = psycopg2.connect(
    host=AWS_REDSHIFT_HOST,
    port=AWS_REDSHIFT_PORT,
    dbname=AWS_REDSHIFT_DBNAME,
    user=AWS_REDSHIFT_USER,
    password=AWS_REDSHIFT_PASSWORD
)

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
note varchar(256),
catalog_event varchar(256),
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

#some auxiliary functions
def date_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%dT%H:%MZ').strftime('%Y-%m-%d')

def time_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%dT%H:%MZ').strftime('%H:%M:%S')

df = spark.createDataFrame(data)
df.printSchema()
df.show()
#Some transformations
df = df.dropna()
df = df.dropDuplicates(['time21_5'])
df = df.withColumnRenamed('time21_5','datetime_event')
df = df.withColumnRenamed('type','type_event')
df = df.withColumnRenamed('catalog','catalog_event')
dateUDF = udf(lambda x:date_convert(x),StringType()) 
timeUDF = udf(lambda x:time_convert(x),StringType())
df = df.withColumn('date_event', dateUDF(df.datetime_event))
df = df.withColumn('time_event', timeUDF(df.datetime_event))
df = df.withColumn('datetime_event', col('datetime_event').cast('string'))
df = df.withColumn('datetime_event', col('datetime_event').cast('string'))
df = df.withColumn('datetime_event', col('datetime_event').cast('string'))
df = df.withColumn('note', substring(col("note"),0,255))
df = df.withColumn('type_event', col('type_event').cast('string'))
df = df.withColumn('catalog_event', col('catalog_event').cast('string'))
df = df.withColumn('link', col('link').cast('string'))
df = df.withColumn('associatedCMEID', col('associatedCMEID').cast('string'))
df = df.withColumn('latitude', col('latitude').cast('int'))
df = df.withColumn('longitude', col('longitude').cast('int'))
df = df.withColumn('halfAngle', col('halfAngle').cast('int'))
df = df.withColumn('speed', col('speed').cast('int'))

df.show()
df.write \
    .format("jdbc") \
    .option("url", REDSHIFT_URL) \
    .option("dbtable", f"amchavezaltamirano_coderhouse.coronal_mass_ejection") \
    .option("user", AWS_REDSHIFT_USER) \
    .option("password", AWS_REDSHIFT_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Data inserted!")
print("Retrieving data from database...")
query = f"select * from amchavezaltamirano_coderhouse.coronal_mass_ejection"
data = spark.read \
    .format("jdbc") \
    .option("url", REDSHIFT_URL) \
    .option("dbtable", f"({query}) as tmp_table") \
    .option("user", AWS_REDSHIFT_USER) \
    .option("password", AWS_REDSHIFT_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()
data.show()