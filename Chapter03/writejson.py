from faker import Faker  # Import the Faker library for generating fake data
import json  # Import the json library for handling JSON operations

# Open a JSON file for writing in the specified relative path
output = open("../output/Chapter03/data.json", "w")

# Create an instance of the Faker class to generate fake data
fake = Faker()

# Initialize an empty dictionary to store all the records
alldata = {}
alldata['records'] = []  # Create a key 'records' with an empty list to store data

# Generate 1000 records of fake data
for x in range(1000):
    # Create a dictionary for a single data record
    data = {
        'name': fake.name(),  # Generate a random name
        'age': fake.random_int(min=18, max=80, step=1),  # Generate a random age between 18 and 80
        'street': fake.street_address(),  # Generate a random street address
        'city': fake.city(),  # Generate a random city
        'state': fake.state(),  # Generate a random state
        'zip': fake.zipcode(),  # Generate a random ZIP code
        'lng': float(fake.longitude()),  # Generate a random longitude and cast to float
        'lat': float(fake.latitude())   # Generate a random latitude and cast to float
    }
    # Append the generated data to the 'records' list
    alldata['records'].append(data)

# Write the complete data dictionary to the JSON file
json.dump(alldata, output)

# Close the file after writing all data
output.close()