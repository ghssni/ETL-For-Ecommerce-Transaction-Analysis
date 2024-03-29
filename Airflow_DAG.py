'''
=================================================
Milestone 3

Nama  : Ghassani Nurbaningtyas
Batch : FTDS-003-SBY


This program was created to automate the transformation and load of data from PostgreSQL to ElasticSearch. 
The dataset used is a detailed description of consumer preferences and purchasing behavior. 
This includes demographic information, purchase history, product preferences, and preferred shopping channels (online or offline). 
This dataset is critical for businesses looking to customize their strategies to meet customer needs and improve their shopping experience, ultimately driving sales and loyalty.
=================================================
'''

# Import Libraries
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
import re

def queryPostgresql():
    '''
    This function used to take data from postgreSQL then save as raw data to CSV file
    Step:
    - Give airflow access to our database in postgreSQL,
    - then airflow will take the all the data from table_m3
    - airflow will save the data as data_raw.csv in dags folder

    Input: PostgreSQL database access
    Output: raw_data.csv
    
    Call Example:
        queryPostgresql()
    '''
    # Give airflow PostgreSQL database access 
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    # Connect to PostgreSQL using the access given
    conn=db.connect(conn_string)
    # Query to take all data table_m3
    df=pd.read_sql("select * from table_m3",conn)
    # Save raw data from postgresql
    df.to_csv('/opt/airflow/dags/P2M3_Tyas_data_raw.csv', index=False)
    print("-------Data Saved------")
 
def cleanRaw():
    '''
    This function used to cleaning the raw data then save as clean data to CSV file.
    Step:
    - Input raw data
    - Change all columns name to lowercase
    - Remove symbol and whitespaces in column name and change to "_"
    - Drop data duplicate
    - Handling missing value, fill with "None"
    - Save as clean data

    Input: raw_data.csv
    Output: clean_data.csv
    
    Call Example:
        cleanRaw()
    '''
    df=pd.read_csv('/opt/airflow/dags/P2M3_Tyas_data_raw.csv')
    # Lower all columns name
    df.columns=[x.lower() for x in df.columns]
    # Remove symbol and whitespaces in column name then replace with '_'
    df.columns=[re.sub(r'[^\w]+', '_',x) for x in df.columns]
    # Drop duplicate
    df = df.drop_duplicates()
    # Handling Missing Value fill with 'None'
    df = df.fillna('None')
    # Save clean data
    df.to_csv('/opt/airflow/dags/P2M3_Tyas_data_clean.csv',index=False)
 
def insertElasticsearch():
    '''
    This function used to insert clean data to elastic search dengan diubah dulu CSV to JSON
    Step:
    - Input clean data 
    - Change data to JSON
    - Input JSON to Elasticsearch

    Input: clean_data.csv
    Output: Successfully input data to Elasticsearch

    Call Example:
        insertElasticsearch()
    '''
    # Elasticsearch
    es = Elasticsearch("http://Elasticsearch:9200") 

    # Load File
    df=pd.read_csv('/opt/airflow/dags/P2M3_Tyas_data_clean.csv',index_col=False)
    
    # Input to Elastic Search
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="milestone",doc_type="doc",body=doc)
        print(res)

# Set DAG owner, initiall run time, and retries times if run failed
default_args = {
    'owner': 'Tyas', 
    'start_date': dt.datetime(2024, 3, 21) - timedelta(hours=7),
    'retries': 1
}
# Set DAG name, DAG description, and schedule interval
with DAG('MyDBdag',
         default_args=default_args,
         schedule_interval='30 6 * * *',      
         ) as dag:
    # Task 1: get raw data from PostgreSQL
    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=queryPostgresql)
    # Task 2: cleaning the data from raw data file
    cleanData = PythonOperator(task_id='cleaningData',
                                 python_callable=cleanRaw)
    # Task 3: insert clean data to elasticsearch
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insertElasticsearch)
# Task flow for airflow
getData >> cleanData >> insertData