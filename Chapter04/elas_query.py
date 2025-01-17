from elasticsearch import Elasticsearch

# Connect to Elasticsearch
# Connect to the Elasticsearch cluster using HTTP
# Note: Ensure Elasticsearch is running and accessible on localhost:9200
es = Elasticsearch(hosts="http://localhost:9200")

doc = {"query": {"match_all": {}}}

res = es.search(index="users", body=doc, size=10)

print(res['hits']['hits'])
