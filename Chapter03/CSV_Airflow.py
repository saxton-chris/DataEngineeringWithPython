import datetime as dt  # Importing datetime for date and time manipulation
from datetime import timedelta  # Importing timedelta for specifying time intervals

from airflow import DAG  # Importing DAG class from Airflow
from airflow.operators.bash import BashOperator  # BashOperator for running bash commands
from airflow.operators.python import PythonOperator  # PythonOperator for running Python functions

import pandas as pd  # Importing pandas for data manipulation
import os  # Importing os for environment and file path management

# Define the Python function to convert CSV to JSON
def CSVtoJson():
    # File paths
    input_file = os.path.expanduser("~/workspace/github.com/saxton-chris/DataEngineeringWithPython/output/Chapter03/data.csv")
    output_file = os.path.expanduser("~/workspace/github.com/saxton-chris/DataEngineeringWithPython/output/Chapter03/fromAirflow.JSON")
    
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(input_file)
        
        # Log each name in the DataFrame (or use Airflow's logger)
        for name in df['name']:
            print(name)
        
        # Convert the DataFrame to JSON format and save it to a file
        df.to_json(output_file, orient='records', indent=4)
        print(f"JSON file successfully created at {output_file}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# Define the default arguments for the DAG
default_args = {
    'owner': 'chris-saxton',  # Owner of the DAG
    'start_date': dt.datetime(2025, 1, 11),  # Start date of the DAG
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
with DAG(
    'MyCSVDAG',  # Name of the DAG
    default_args=default_args,  # Apply default arguments
    schedule=timedelta(minutes=5),  # Schedule the DAG to run every 5 minutes
) as dag:
    
    # Task 1: BashOperator to print a starting message
    print_starting = BashOperator(
        task_id='starting',  # Unique ID for the task
        bash_command='echo "I am reading the CSV now....."'  # Bash command to run
    )
    
    # Task 2: PythonOperator to execute the CSV to JSON conversion function
    CSVJSON = PythonOperator(
        task_id='convertCSVtoJSON',  # Unique ID for the task
        python_callable=CSVtoJson  # Specify the Python function to call
    )
    
    # Set the task dependencies
    print_starting >> CSVJSON  # `print_starting` must complete before `CSVJSON` starts
