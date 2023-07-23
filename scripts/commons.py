from os import environ as env
from psycopg2 import connect
from pyspark.sql import SparkSession

# env variables
REDSHIFT_HOST = env["REDSHIFT_HOST"]
REDSHIFT_PORT = env["REDSHIFT_PORT"]
REDSHIFT_DB = env["REDSHIFT_DB"]
REDSHIFT_USER = env["REDSHIFT_USER"]
REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
REDSHIFT_URL = env["REDSHIFT_URL"]


class ETL_Spark:

    DRIVER_PATH = env["DRIVER_PATH"]
    JDBC_DRIVER = "org.postgresql.Driver"

    def __init__(self, job_name=None):
        print(">>> Starting ETL execution")

        env["SPARK_CLASSPATH"] = self.DRIVER_PATH

        env[
            "PYSPARK_SUBMIT_ARGS"
        ] = f"--driver-class-path {self.DRIVER_PATH} --jars {self.DRIVER_PATH} pyspark-shell"

        # spark session
        self.spark = (
            SparkSession.builder.master("local[1]")
            .appName("ETL Spark" if job_name is None else job_name)
            .config("spark.jars", self.DRIVER_PATH)
            .config("spark.executor.extraClassPath", self.DRIVER_PATH)
            .getOrCreate()
        )

        try:
            # connect to redshift
            print(">>> Conecting to Redshift")
            self.conn_redshift = connect(
                host=REDSHIFT_HOST,
                port=REDSHIFT_PORT,
                database=REDSHIFT_DB,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
            )
            self.cur_redshift = self.conn_redshift.cursor()
            print(">>  Connected to Redshift")
            # close connection
            self.cur_redshift.close()
            self.conn_redshift.close()
        except:
            print(">>> Connection to Redshift failed")

    def execute(self):

        print(">>> [execute] Ejecutando ETL...")

        # Extract
        df_api = self.extract()

        # Transform
        df_transformed = self.transform(df_api)

        # Load
        self.load(df_transformed)

    def extract(self):
        print(">>> Extracting data from API")

    def transform(self, df_original):
        print(">>> Transforming data")

    def load(self, df_final):
        print(">>> Loading data")
