import psycopg2 as db
import pandas as pd

# Define the connection string to connect to the PostgreSQL database
conn_string = "dbname = 'dataengineering' host = 'localhost' user = 'postgres' password = 'ollie'"

# Establish a connection to the database
conn = db.connect(conn_string)

# Use pandas to execute a SQL query and fetch the data into a DataFrame
df = pd.read_sql("SELECT * FROM users", conn)

# Save the DataFrame as a JSON file in records orientation
df.to_json("../output/data_ch04.json", orient='records')
