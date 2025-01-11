import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd

def CSVtoJson():
    df  = pd.read_csv('~/workspace/github.com/saxton-chris/DataEngineeringWithPython/output/data.csv')
    for i, r in df.iterrows():
        print(r['name'])

    df.to_json('./fromAirflow.JSON', orient='records')

default_args = {
    'owner': 'chrissaxton',
    'start_date': dt.datetime(2025, 1, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('MyCSVDAG',
         default_args = default_args,
         schedule = timedelta(minutes=5),
         # '0 * * * *',
         ) as dag:
    
    print_starting = BashOperator(task_id='starting',
                                  bash_command='echo "I am reading the CSV now....."')
    
    CSVJSON = PythonOperator(task_id = 'convertCSVtoJSON',
                             python_callable=CSVtoJson)
    
print_starting >> CSVJSON
