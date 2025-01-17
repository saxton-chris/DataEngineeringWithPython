import psycopg2 as db
from faker import Faker

# Initialize the Faker library for generating fake data
fake = Faker()
data = []
i = 2

# Generate 1000 rows of fake data to load into a PostgreSQL database
for r in range(1000):
    data.append((i, fake.name(), fake.street_address(), fake.city(), fake.zipcode()))
    i += 1

data_for_db = tuple(data)

#print(data)
#print(data_for_db)

# Connection string to connect to the PostgreSQL database
conn_string = "dbname = 'dataengineering' host = 'localhost' user = 'postgres' password = '*****'"

# Establish a connection to the database
conn = db.connect(conn_string)
cur = conn.cursor()

#print(conn)

# Build and check the SQL query for inserting data
query = "INSERT INTO users (id, name, street, city, zip) VALUES (%s, %s, %s, %s, %s)"
print(cur.mogrify(query, data_for_db[1])) # Print a sample query to verify correctness

# Execute the query with the generated data and commit the transaction
cur.executemany(query, data_for_db)
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
