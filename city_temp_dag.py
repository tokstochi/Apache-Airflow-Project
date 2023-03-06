from airflow import DAG

from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook

#Import packages and dependencies
import pickle
import json
import pandas as pd
import boto3
from botocore.client import Config
import os
from io import StringIO
import psycopg2
import psycopg2.extras as extras
import sqlalchemy
import logging

client = boto3.client(
        's3',
        aws_access_key_id='AKIAT7ZRNBQYFOWQSREW',
        aws_secret_access_key='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')

default_args = {
    'owner' : 'toks',
    'retries' : 2,
    'retry_delay' : timedelta(minutes=5)
}




def read_and_convert_csv_to_dataframe():
    #Client creation with aws credentials
    
   
    obj = client.get_object(Bucket = "detochukwuokafor-deliverystream-s3", Key = "temp-folder/city_temperature.csv")

    data = obj['Body']

    data2 = data.read().decode('utf-8')

    df = pd.read_csv(StringIO(data2), low_memory=False)
#pd.read_csv(io.BytesIO(uploaded['train.csv']), low_memory=False)

    df1 = pd.DataFrame(df)
    df1 = df1.to_json()
    return df1    

df2 = read_and_convert_csv_to_dataframe()

def checks_and_transformations(dataframe):
    
    dataframe = pd.read_json(dataframe)
    dataframe.head()
    dataframe.dtypes
    
    #check for nan in the dataframe
    dataframe.isnull().values.any()
    
    #nan count in the entire dataframe
    dataframe.isnull().sum()
    dataframe.shape[0]
    dataframe=dataframe.drop(['State'], axis=1)
    #check for rows with missing values
     
    null_data = dataframe[dataframe.isnull().any(axis=1)]
    dataframe = dataframe.to_json()
    return dataframe

checks = checks_and_transformations(df2)

def create_server_connection(df, table):
    pg_hook = PostgresHook(
        conn_id = 'postgres_default',
        schema = 'd2b_assessment'
    )
    pg_conn=pg_hook.get_conn()

    df=pd.read_json(df)

    tuples = [list(row) for row in df.itertuples(index=False)]

    cols = ','.join(list(df.columns))
    print(cols)

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    cursor = pg_conn.cursor()

    extras.execute_values(cursor, query, tuples)

    print(cols)
    pg_conn.commit()
    pg_conn.rollback()
    cursor.close()
    print("the dataframe is inserted")
    cursor.close()

    #return pg_conn
conn= create_server_connection(checks, 'byroneji4734_staging.cities_temperature')

def max_avg_temp_query():
    df11 = """(
    SELECT MAX(avgtemperature) as avgtemp,year
    FROM byroneji4734_staging.city_temperature 
    GROUP BY city_temperature.year
    ORDER BY avgtemp DESC
    )
    """
    return df11

max_avgtemp = max_avg_temp_query()

def read_and_convert_query_to_dataframe(query):

    pg_hook = PostgresHook(
        conn_id = 'postgres_default',
        schema = 'd2b_assessment'
    )
    pg_conn=pg_hook.get_conn()
    cursor = pg_conn.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        #get column names
        col_names = [i[0] for i in cursor.description ]
        result = pd.DataFrame(result,columns=col_names)
        result = result.to_json()
        return result
    except Error as err:
        print(f"Error: '{err}'")

dfmax_temp = read_and_convert_query_to_dataframe(max_avgtemp)

        
def upload_to_dataframe_bucket_as_csv(file_name, bucket, object_name):
    #bucket = "detochukwuokafor-deliverystream-s3"
    file_name =pd.read_json(file_name)
    save_path = "temp-folder/analysedData/"
    """Upload a file to an S3 bucket

    :param df_file: Dataframe to process
    :param file_name: Desired name of file in the bucket

    """

    # Upload the file
    
    
    csv_buffer = StringIO()
    file_name.to_csv(csv_buffer)
    #df_csv=df_file.to_csv
    s3_resource = boto3.resource('s3',
        aws_access_key_id='AKIAT7ZRNBQYFOWQSREW',
        aws_secret_access_key='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'
                                )
    
    try:
        s3_resource.Object(bucket , "temp-folder/analysedData/" +object_name+".csv").put(Body=csv_buffer.getvalue())
    #response = client.upload_to_dataframe_bucket_as_csv(file_name, bucket, object_name)
        print('Successfully uploaded dataframe as CSV to S3 bucket')
    except :
        #logging.error(e)
        print('failed to upload csv to s3')
    return 

upload_to_dataframe_bucket_as_csv(dfmax_temp,"detochukwuokafor-deliverystream-s3",'max_temperature_of_the_year')






with DAG(
    default_args=default_args,
    dag_id='first_airflow_dag',
    description = 'city temperature dag',
    start_date = datetime(2023, 2, 5),
    schedule_interval=None
    
)as dag:

    task1 = PythonOperator(
        task_id='get_data_set_in_S3',
        python_callable= read_and_convert_csv_to_dataframe
    )

    task2 = PythonOperator(
        task_id = 'checks_and_transformation',
        python_callable= checks_and_transformations,
        op_args= [df2]
    )

    task3 = PostgresOperator(
        task_id = 'postgres_connection_table_creation',
        postgres_conn_id = 'postgres_default',
        sql ="""

            CREATE TABLE IF NOT EXISTS byroneji4734_staging.cities_temperature(
                region character(124) COLLATE pg_catalog."default" NOT NULL,
                country character(124) COLLATE pg_catalog."default" NOT NULL,
                city character(124) COLLATE pg_catalog."default" NOT NULL,
                month integer,
                day integer,
                year integer,
                avgtemperature double precision
            )
        """
    )

    task4 =PythonOperator(
        task_id = 'postgres_connection',
        python_callable= create_server_connection,
        op_args= [checks, 'byroneji4734_staging.cities_temperature']

    )

    task5 =PythonOperator(
         task_id = 'max_temp_query',
         python_callable= max_avg_temp_query
    #     op_args= [checks]

    )

    task6 =PythonOperator(
        task_id = 'sql_query_to_df',
        python_callable=read_and_convert_query_to_dataframe,
        op_args=[max_avgtemp]
    )

    task7 =PythonOperator(
        task_id = 'upload_max_temp_dataframe_to_s3',
        python_callable=upload_to_dataframe_bucket_as_csv,
        op_args=[dfmax_temp, "detochukwuokafor-deliverystream-s3", 'max_temperature_of_the_year']
    )

task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7