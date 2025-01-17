from elasticsearch import Elasticsearch
from faker import Faker

# Create a Faker instance
fake = Faker()

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],
    verify_certs=False,  # Disable SSL verification for development (use cautiously)
    ssl_show_warn=False  # Suppress SSL warnings
)

# Create a fake document
doc = {
    "name": fake.name(),
    "street": fake.street_address(),
    "city": fake.city(),
    "zip": fake.zipcode()  # Corrected method name
}

# Index the document in Elasticsearch
try:
    res = es.index(index="users", body=doc)
    print("Document indexed successfully!")
    print(f"Document ID: {res['_id']}")
except Exception as e:
    print(f"Error indexing document: {e}")
