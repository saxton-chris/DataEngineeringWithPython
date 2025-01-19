from faker import Faker  # Import the Faker library for generating fake data
import json  # Import the json library for handling JSON operations

# Define the output file path
output_path = "../output/Chapter03/data.json"

# Create an instance of the Faker class to generate fake data
fake = Faker()

# Generate 1000 records of fake data
records = [
    {
        'name': fake.name(),                     # Generate a random name
        'age': fake.random_int(min=18, max=80),  # Generate a random age between 18 and 80
        'street': fake.street_address(),         # Generate a random street address
        'city': fake.city(),                     # Generate a random city
        'state': fake.state(),                   # Generate a random state
        'zip': fake.zipcode(),                   # Generate a random ZIP code
        'lng': float(fake.longitude()),          # Generate a random longitude and cast to float
        'lat': float(fake.latitude())            # Generate a random latitude and cast to float
    }
    for _ in range(1000)
]

# Create the complete data dictionary
alldata = {'records': records}

# Write the data to the JSON file with indentation for readability
with open(output_path, "w") as output_file:
    json.dump(alldata, output_file, indent=4)