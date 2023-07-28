import requests
import json
import sys
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import col, lit, udf, substring
from pyspark.sql.types import StringType

from commons import ETL_Spark


class ETL_CME(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = sys.argv[1]
        print(self.process_date)

    def run(self):
        self.execute(self.process_date)

    def extract(self):
        print(">>> Extracting data:")
        date = datetime.strptime(self.process_date, "%Y-%m-%d")
        week = (date - timedelta(days=7)).strftime(
            "%Y-%m-%d"
        )  # look for info uploaded during previous week

        print(date)
        print(week)
        apiCall = (
            "https://api.nasa.gov/DONKI/CMEAnalysis?startDate="
            + week
            + "&endDate="
            + self.process_date
            + "&mostAccurateOnly=true&speed=500&halfAngle=30&catalog=ALL&api_key=DEMO_KEY"
        )
        print(">>Executing api call -> " + apiCall)
        result = requests.get(apiCall)
        # handle api response
        if result.status_code == 200:
            data = json.loads(result.text)
            print("Data retrieved from API:")
            print(data)
        else:
            data = []
            raise Exception(f"Error retrieving data, status code: {result.status_code}")

        df = self.spark.createDataFrame(data)
        df.printSchema()
        df.show()

        return df

    def transform(self, df_original):
        print(">>> Transforming data:")

        dateUDF = udf(lambda x: date_convert(x), StringType())
        timeUDF = udf(lambda x: time_convert(x), StringType())

        df = df_original.dropna()
        df = df.dropDuplicates(["time21_5"])
        df = df.withColumnRenamed("time21_5", "datetime_event")
        df = df.withColumn("datetime_event", col("datetime_event").cast("string"))
        df = df.withColumnRenamed("type", "type_event")
        df = df.withColumn("type_event", col("type_event").cast("string"))
        df = df.withColumnRenamed("catalog", "catalog_event")
        df = df.withColumn("catalog_event", col("catalog_event").cast("string"))
        df = df.withColumn("date_event", dateUDF(df.datetime_event).cast("string"))
        df = df.withColumn("time_event", timeUDF(df.datetime_event).cast("string"))
        df = df.withColumn("note", substring(col("note"), 0, 255))
        df = df.withColumn("link", col("link").cast("string"))
        df = df.withColumn("associatedCMEID", col("associatedCMEID").cast("string"))
        df = df.withColumn("latitude", col("latitude").cast("int"))
        df = df.withColumn("longitude", col("longitude").cast("int"))
        df = df.withColumn("halfAngle", col("halfAngle").cast("int"))
        df = df.withColumn("speed", col("speed").cast("int"))

        df.printSchema()
        df.show()

        return df

    def load(self, df_final):
        print(">>> Loading data into redshift:")

        # add process_date column
        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write.format("jdbc").option("url", env["REDSHIFT_URL"]).option(
            "dbtable", f"{env['REDSHIFT_SCHEMA']}.coronal_mass_ejection"
        ).option("user", env["REDSHIFT_USER"]).option(
            "password", env["REDSHIFT_PASSWORD"]
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

        print(">>> Data loaded succesfully")


# some auxiliary functions
def date_convert(date_to_convert):
    return datetime.strptime(date_to_convert, "%Y-%m-%dT%H:%MZ").strftime("%Y-%m-%d")


def time_convert(date_to_convert):
    return datetime.strptime(date_to_convert, "%Y-%m-%dT%H:%MZ").strftime("%H:%M:%S")


if __name__ == "__main__":
    print("Running ETL script")
    etl = ETL_CME()
    etl.run()
