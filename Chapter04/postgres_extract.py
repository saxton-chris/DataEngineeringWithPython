import psycopg2 as db

# Define the connection string to connect to the PostgreSQL database
conn_string = conn_string = "dbname = 'dataengineering' host = 'localhost' user = 'postgres' password = 'ollie'"

# Establish a connection to the database
conn = db.connect(conn_string)
cur = conn.cursor()

# Query to select all data from the 'users' table
query = "SELECT * FROM users"
cur.execute(query)

# Print each record retrieved by the query
for record in cur:
    print(record)

# Print the total number of rows fetched by the query
print(cur.rowcount)

# Open a file to write the data
f = open("../output/data_ch04.csv", "w")

# Use copy_to to write the 'users' table data to a CSV file
cur.copy_to(f, 'users', sep=',')
f.close()
