# DAG

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta

QUERY_CREATE_TABLE = """
create table if not exists amchavezaltamirano_coderhouse.coronal_mass_ejection(
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

    spark_etl_cme = SparkSubmitOperator(
        task_id="spark_etl_cme",
        application="/opt/airflow/scripts/ETL_CME.py",
        conn_id="spark_default",
        dag=dag,
        driver_class_path="/tmp/drivers/postgresql-42.5.2.jar",
    )

    get_process_date_task >> create_table >> spark_etl_cme
