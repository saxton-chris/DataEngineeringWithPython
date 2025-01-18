from elasticsearch import Elasticsearch

# Establish a connection to the Elasticsearch cluster
es = Elasticsearch(hosts="http://localhost:9200")
print("CONNECTION ESTABLISHED")

# Perform an initial search query to create a scroll context
res = es.search(
    index = 'users',                                    # The target index to search
    scroll = '20m',                                     # Keep the search context alive for 20 minutes
    body = {"query": {"match_all": {}}, "size": 500}    # Match all documents, retrieving 500 at a time
)
print("\nRESULTS GATHERED")

# Extract the scroll ID and total number of documents from the initial response
sid = res['_scroll_id']                 # Scroll ID to fetch subsequent batches
size = res['hits']['total']['value']    # Total number of documents matching the query
print(f"\nID AND SIZE GATHERED SIZE: {size}")

# Use a loop to scroll through all matching documents
while (size > 0):
    print(size)                                         # Print the size of the current batch
    res = es.scroll(scroll_id = sid, scroll = '20m')    # Fetch the next batch of results using the scroll ID
    sid = res['_scroll_id']                             # Update the scroll ID for the next iteration
    size = len(res['hits']['hits'])                     # Number of documents in the current batch
    
    # Print the source (content) of each document in the current batch
    for doc in res['hits']['hits']:
        print(doc['_source'])
print("\nWHILE LOOP DONE")
