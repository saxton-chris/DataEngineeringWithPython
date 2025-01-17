from elasticsearch import Elasticsearch
from elasticsearch import helpers
from faker import Faker

# Create a Faker instance
fake = Faker()

# Connect to Elasticsearch
# Connect to the Elasticsearch cluster using HTTP
# Note: Ensure Elasticsearch is running and accessible on localhost:9200
es = Elasticsearch(hosts="http://localhost:9200")

# Create a list of 1000 fake documents to index into Elasticsearch
# Each document contains a name, street, city, and zip code
actions = [
    {
        "_index": "users", # Specify the target index name
        "_source": {
            "name": fake.name(),
            "street": fake.street_address(),
            "city": fake.city(),
            "zip": fake.zipcode()
        }
    }
    for x in range(1000)
]

# Use Elasticsearch helpers to perform bulk indexing
try:
    # The bulk helper indexes all documents in a single call for efficiency
    res = helpers.bulk(es, actions)
    print("Documents indexed successfully!")
    # Uncomment the line below if you want to inspect the result details
    # print(res)
except Exception as e:
    # Catch and print any errors that occur during indexing
    print(f"Error indexing document: {e}")
