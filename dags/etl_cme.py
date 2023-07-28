# DAG

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS amchavezaltamirano_coderhouse.coronal_mass_ejection(
    datetime_event VARCHAR(256) NOT NULL,
    type_event VARCHAR(256) NOT NULL,
    catalog_event VARCHAR(256),
    date_event VARCHAR(256),
    time_event VARCHAR(256),
    note VARCHAR(256),
    link VARCHAR(256),
    isMostAccurate BOOLEAN NOT NULL,
    associatedCMEID VARCHAR(256),
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    halfAngle FLOAT NOT NULL,
    speed FLOAT NOT NULL,
    id INT IDENTITY(1,1),
    process_date VARCHAR(256),
    primary key(id))
distkey(process_date)
compound sortkey(process_date);
"""

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM amchavezaltamirano_coderhouse.coronal_mass_ejection WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


defaul_args = {
    "owner": "Andrea Chavez",
    "start_date": datetime(2023, 7, 15),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_cme",
    default_args=defaul_args,
    description="ETL Entrega 3 de la tabla CME - coronal_mass_ejection",
    schedule_interval="@weekly",
    catchup=False,
) as dag:
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    spark_etl_cme = SparkSubmitOperator(
        task_id="spark_etl_cme",
        application="/opt/airflow/scripts/ETL_CME.py",
        conn_id="spark_default",
        dag=dag,
        driver_class_path="/tmp/drivers/postgresql-42.5.2.jar",
        application_args=['{{ ti.xcom_pull(key="process_date") }}'],
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_cme
