# Exploring Historical Temperature Data with Apache Airflow and amazon S3



![image](https://user-images.githubusercontent.com/91758523/229606385-b3a760b8-6e49-4db0-9e70-54769c107b0b.png)
Figure 1: A representation of the workflow

This is an Apache Airflow ETL project done with a historic temperature dataset. Dataset was loaded from S3 bucket .
Firstly, all packages and dependencies needed to make the DAG run are imported to the script.

## Installation

* Python 3.x
* Docker Desktop
* Airflow Image pulled and ready to run.
    ** Start the Airflow web server and scheduler on your docker desktop app:
      * Access the Airflow web UI at http://localhost:8080/ and turn on the ecommerce_webscraping DAG.



The dataset, which is in a csv format, is stored in an Amazon S3 bucket and will be accessed using already established credentials that will  enable access to the bucket and file path of the aforementioned data set. 

Transformations are done to the dataset where and when they are needed before it is loaded to the POSTGRES Database. To see to this, we need to have created a connection to the POSTGRES Database using already created credentials from RDS instance. One of the dependencies that aids a DAG's connection to the database is the PostgresHook. Basically, Hooks are event-based functions present in Postgres. 

Some analysis, like the average maximum temperature in the different cities, was done on the inserted data set, and the ensuing result was stored in the S3 bucket for record purposes.


![image](https://user-images.githubusercontent.com/91758523/236909786-eacc642c-142c-4a95-be10-5f737714c68d.png)
Figure 2: An image of a part of the dataset uploaded to the PostgreDB
