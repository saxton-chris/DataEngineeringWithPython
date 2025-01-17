import pandas as pd
from sqlalchemy import create_engine

# Define the connection string to connect to the PostgreSQL database
conn_string = "postgresql+psycopg2://postgres:*****@localhost/dataengineering"

# Create a SQLAlchemy engine
engine = create_engine(conn_string)

# Establish a connection to the database and fetch data into a DataFrame
with engine.connect() as conn:
    # Use pandas to execute a SQL query and fetch the data into a DataFrame
    df = pd.read_sql_query("SELECT * FROM users", conn)

    # Save the DataFrame as a JSON file in records orientation
    df.to_json("../output/data_ch04.json", orient='records', indent=4)
