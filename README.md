# CME
Proyect for Data Engineering course

## Steps
1. Create an .env file with the following structure:
```
AWS_REDSHIFT_USER=your-user
AWS_REDSHIFT_PASSWORD=your-password
AWS_REDSHIFT_HOST=data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
AWS_REDSHIFT_PORT=5439
AWS_REDSHIFT_DBNAME=data-engineer-database
AWS_REDSHIFT_SCHEMA=your-schema
```
2. Run "script.py"
3. To check that the data is also in the db, you can execute "select * from amchavezaltamirano_coderhouse.coronal_mass_ejection;"
