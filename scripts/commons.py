from os import environ as env
from psycopg2 import connect
from pyspark.sql import SparkSession
from airflow.models import Variable

# env variables
REDSHIFT_HOST = env["REDSHIFT_HOST"]
REDSHIFT_PORT = env["REDSHIFT_PORT"]
REDSHIFT_DB = env["REDSHIFT_DB"]
REDSHIFT_USER = env["REDSHIFT_USER"]
REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
REDSHIFT_URL = env["REDSHIFT_URL"]

email_success = "[OK] CME ETL finished successfully"
email_failure = "[ERROR] CME ETL"

import smtplib


def send_error():
    send_email(email_failure, f"DAG finished with status: {email_failure}")


def send_success():
    send_email(email_success, f"DAG finished with status: {email_success}")


def send_email(subject, body_text):
    try:
        x = smtplib.SMTP("smtp.gmail.com", 587)
        x.starttls()
        x.login(Variable.get("SMTP_EMAIL_FROM"), Variable.get("SMTP_PASSWORD"))
        message = "Subject: {}\n\n{}".format(subject, body_text)
        x.sendmail(
            Variable.get("SMTP_EMAIL_FROM"), Variable.get("SMTP_EMAIL_TO"), message
        )
        print("Email sent")
    except Exception as exception:
        print(exception)
        print("Error sending email")


def check_max_speed(df):
    check_max_number(
        df, df.speed, "CME_MAX_SPEED", "CME ETL MAX SPEED EXCEEDED", "Max speed"
    )


def check_max_half_angle(df):
    check_max_number(
        df,
        df.halfAngle,
        "CME_MAX_HALF_ANGLE",
        "CME ETL MAX HALF ANGLE EXCEEDED",
        "Max half angle",
    )


def check_max_number(df, column, variable, subject, body_part):
    max = Variable.get(variable)
    temp_df = df.filter(column > max)
    if temp_df.count() > 0:
        body_text = f"{body_part} of {max} exceeded for {temp_df.count()} record(s)"
        send_email(subject, body_text)


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

    def execute(self, process_date: str):
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
