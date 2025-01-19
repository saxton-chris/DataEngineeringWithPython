from faker import Faker     # Import the Faker library for generating fake data
import csv                  # Import the csv library for handling CSV file operations

# Open the file 'data.csv' for writing in the specified relative path
output = open("../output/Chapter03/data.csv", "w")

# Create an instance of the Faker class to generate fake data
fake = Faker()

# Define the header for the CSV file
header = ["name", "age", "street", "city", "state", "zip", "lng", "lat"]

# Create a CSV writer object to write data into the file
mywriter = csv.writer(output)

# Write the header row into the CSV file
mywriter.writerow(header)


for r in range(1000):
    # Create a single row of fake data
    row = [
        fake.name(),  # Generate a random name
        fake.random_int(min=18, max=80, step=1),  # Generate a random age between 18 and 80
        fake.street_address(),  # Generate a random street address
        fake.city(),  # Generate a random city
        fake.state(),  # Generate a random state
        fake.zipcode(),  # Generate a random ZIP code
        fake.longitude(),  # Generate a random longitude
        fake.latitude(),  # Generate a random latitude
    ]
    # Write the generated row into the CSV file
    mywriter.writerow(row)


# Close the file after writing all data
output.close()
