# CME
Proyect for Data Engineering course, retrieve of Coronal Mass Ejection data from NASA API

## Steps
1. Execute the following command to create the "dags", "logs" and "posgres_data" folders 
```bash
mkdir -p logs,plugins,postgres_data
```
2. Create an .env file with the following structure:
```bash
REDSHIFT_HOST=...
REDSHIFT_PORT=5439
REDSHIFT_DB=...
REDSHIFT_USER=...
REDSHIFT_SCHEMA=...
REDSHIFT_PASSWORD=...
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
```
3. Download airflow and spark imagines (below were provided by the course´s teacher).
```bash
docker-compose pull lucastrubiano/airflow:airflow_2_6_2
docker-compose pull lucastrubiano/spark:spark_3_4_1
```
4. Start docker.
```bash
docker-compose up --build
```
5. Once the service is up and running, go to `http://localhost:8080/`.
6. In `Admin -> Connections` create a Redshift connection with the following details:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
7. In `Admin -> Connections` create a Spark connection with the following details:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
8. In `Admin -> Variables` create a new variable with the following details:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar`
9. Execute DAG `etl_cme`.
10. To check that the data is also in the db, you can execute "select * from amchavezaltamirano_coderhouse.coronal_mass_ejection;"
