import psycopg2 as db

# Define the connection string to connect to the PostgreSQL database
conn_string = "dbname = 'dataengineering' host = 'localhost' user = 'postgres' password = 'ollie'"

# Establish a connection to the database
with db.connect(conn_string) as conn:
    with conn.cursor() as cur:
        # Query to select all data from the 'users' table
        query = "SELECT * FROM users"
        cur.execute(query)

        # Fetch all records and print them
        records = cur.fetchall()
        for record in records:
            print(record)

        # Print the total number of rows fetched by the query
        print(len(records))

        # Write the data to a CSV file
        with open("../output/data_ch04.csv", "w") as f:
            cur.copy_to(f, 'users', sep=',')
