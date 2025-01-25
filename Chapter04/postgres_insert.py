import psycopg2 as db
from faker import Faker

# Initialize the Faker library for generating fake data
fake = Faker()

# Generate 1000 rows of fake data to load into a PostgreSQL database
data = [(i, fake.name(), fake.street_address(), fake.city(), fake.zipcode()) for i in range(2, 1002)]

data_for_db = tuple(data)

# Connection string to connect to the PostgreSQL database
conn_string = "dbname = 'dataengineering' host = 'localhost' user = 'postgres' password = 'postgres'"

# Establish a connection to the database
with db.connect(conn_string) as conn:
    with conn.cursor() as cur:
        # Build and check the SQL query for inserting data
        query = "INSERT INTO users (id, name, street, city, zip) VALUES (%s, %s, %s, %s, %s)"
        print(cur.mogrify(query, data_for_db[1]))  # Print a sample query to verify correctness

        # Execute the query with the generated data and commit the transaction
        cur.executemany(query, data_for_db)
        conn.commit()
