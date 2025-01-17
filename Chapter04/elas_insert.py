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
def generate_actions(batch_size=1000):
    """Generate actions for bulk indexing."""
    return [
        {
            "_index": "users", # Specify the target index name
            "_source": {
                "name": fake.name(),
                "street": fake.street_address(),
                "city": fake.city(),
                "zip": fake.zipcode()
            }
        }
        for _ in range(batch_size)
    ]

# Use Elasticsearch helpers to perform bulk indexing
def index_documents(es_client, actions):
    """Index documents using Elasticsearch bulk API."""
    try:
        # The bulk helper indexes all documents in a single call for efficiency
        res = helpers.bulk(es_client, actions)
        print("Documents indexed successfully!")
        # Uncomment the line below if you want to inspect the result details
        # print(res)
    except Exception as e:
        # Catch and print any errors that occur during indexing
        print(f"Error indexing document: {e}")

if __name__ == "__main__":
    # Generate actions and index them in Elasticsearch
    actions = generate_actions(batch_size=1000)
    index_documents(es, actions)
