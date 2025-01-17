import psycopg2 as db
from faker import Faker

fake = Faker()
data = []
i = 2

# generate data to load into Postgres DB
for r in range(1000):
    data.append((i, fake.name(), fake.street_address(), fake.city(), fake.zipcode()))
    i += 1

data_for_db = tuple(data)

#print(data)
#print(data_for_db)

# Connecting to the DB
conn_string = "dbname = 'dataengineering' host = 'localhost' user = 'postgres' password = 'ollie'"
conn = db.connect(conn_string)
cur = conn.cursor()

#print(conn)

# Build and check the query
query = "INSERT INTO users (id, name, street, city, zip) VALUES (%s, %s, %s, %s, %s)"
print(cur.mogrify(query, data_for_db[1]))

# Insert into Postgres DB - users table
cur.executemany(query, data_for_db)
conn.commit()
