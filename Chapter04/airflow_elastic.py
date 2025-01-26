# Importing necessary modules for date and time handling
import datetime as dt
from datetime import timedelta

# Importing modules from Airflow for creating a DAG and defining tasks
from airflow import DAG
from airflow.operators.bash import BashOperator  # (Not used in the current script but available for Bash tasks)
from airflow.operators.python import PythonOperator  # For executing Python functions as tasks

# Importing modules for database and Elasticsearch operations
import pandas as pd  # For data manipulation and saving to CSV
import psycopg2 as db  # For connecting to PostgreSQL databases
from elasticsearch import Elasticsearch  # For inserting data into Elasticsearch

# Function to query data from PostgreSQL
def queryPostgresql():
    # Connection string to PostgreSQL database
    conn_string = "dbname = 'dataengineering' host = 'localhost' user = 'postgres' password = 'ollie'"
    conn = db.connect(conn_string)  # Establishing connection to the database
    # Querying data from the 'users' table
    df = pd.read_sql("SELECT name, city FROM users", conn)
    # Saving the queried data to a CSV file
    df.to_csv("/home/chris/workspace/github.com/saxton-chris/DataEngineeringWithPython/output/flows_output/Chapter04/postgresqldata.csv")
    print("-------Data Saved-------")  # Logging confirmation

# Function to insert data into Elasticsearch
def insertElasticsearch():
    # Connecting to Elasticsearch
    es = Elasticsearch(hosts="http://localhost:9200")
    # Reading data from the previously saved CSV file
    df = pd.read_csv("/home/chris/workspace/github.com/saxton-chris/DataEngineeringWithPython/output/flows_output/Chapter04/postgresqldata.csv")
    # Iterating through the rows of the DataFrame
    for _, r in df.iterrows:  # Note: `.iterrows()` should be used with parentheses, not without
        # Converting each row to JSON format
        doc = r.to_json()
        # Indexing the document in Elasticsearch under the 'frompostgresql' index
        res = es.index(index='frompostgresql', body=doc)
        print(res)  # Logging the response from Elasticsearch

# Default arguments for the DAG
default_args = {
    'owner': 'chris-saxton',  # Owner of the DAG
    'start_date': dt.datetime(2025, 1, 16),  # Start date of the DAG
    'retries': 1,  # Number of retries for failed tasks
    'retry_delay': dt.timedelta(minutes=5),  # Delay between retries
}

# Defining the DAG
with DAG('MyDBdag',  # Name of the DAG
         default_args=default_args,  # Applying default arguments
         schedule_interval=timedelta(minutes=5),  # Schedule interval for DAG execution
         # Alternative option: '0 * * * *' for every hour
         ) as dag:
    
    # Task to query data from PostgreSQL
    getData = PythonOperator(task_id='QueryPostgresSQL',  # Unique task ID
                             python_callable=queryPostgresql)  # Function to execute

    # Task to insert data into Elasticsearch
    insertData = PythonOperator(task_id='InsertDataElasticsearch',  # Unique task ID
                                python_callable=insertElasticsearch)  # Function to execute

# Setting task dependencies: `getData` must run before `insertData`
getData >> insertData
