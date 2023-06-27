# CME
Proyect for Data Engineering course, retrieve of Coronal Mass Ejection data from NASA API

## Steps
1. Create an .env file with the following structure:
```
REDSHIFT_USER=your-user
REDSHIFT_PASSWORD=your-password
REDSHIFT_HOST=data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=data-engineer-database
```
2. Run "script.py" inside the Docker container provided by the teachers
3. To check that the data is also in the db, you can execute "select * from amchavezaltamirano_coderhouse.coronal_mass_ejection;"
