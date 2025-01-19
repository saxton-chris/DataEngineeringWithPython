from faker import Faker     # Import the Faker library for generating fake data
import csv                  # Import the csv library for handling CSV file operations

# Define the output file path
output_path = "../output/Chapter03/data.csv"

# Create an instance of the Faker class to generate fake data
fake = Faker()

# Define the header for the CSV file
header = ["name", "age", "street", "city", "state", "zip", "lng", "lat"]

# Generate 1000 rows of fake data
data_rows = [
    [
        fake.name(),                      # Generate a random name
        fake.random_int(min=18, max=80),  # Generate a random age between 18 and 80
        fake.street_address(),            # Generate a random street address
        fake.city(),                      # Generate a random city
        fake.state(),                     # Generate a random state
        fake.zipcode(),                   # Generate a random ZIP code
        fake.longitude(),                 # Generate a random longitude
        fake.latitude(),                  # Generate a random latitude
    ]
    for _ in range(1000)
]

# Write the data to the CSV file
with open(output_path, "w", newline='') as output_file:
    writer = csv.writer(output_file)
    writer.writerow(header)  # Write the header row
    writer.writerows(data_rows)  # Write all data rows at once
