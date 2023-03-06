# Apache-Airflow-Project
This is an Apache Airflow ETL project on a historic temperature dataset.
Firstly, all packages and dependencies needed to make the DAG run are imported.
The dataset, which is in a csv format, is stored in an Amazon S3 bucket and will be accessed using already established credentials that will  enable access to the bucket and file path of the aforementioned data set. 

Transformations are done to the dataset where and when they are needed before it is loaded to the POSTGRES Database. To see to this, we need to have created a connection to the POSTGRES Database using already created credentials from RDS instance. One of the dependencies that aids a DAG's connection to the database is the PostgresHook. Basically, Hooks are event-based functions present in Postgres. 

Some analysis, like the average maximum temperature in the different cities, was done on the inserted data, and the ensuing result was stored in the S3 bucket for record purposes.


