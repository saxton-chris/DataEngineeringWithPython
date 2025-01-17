from elasticsearch import Elasticsearch

def connect_to_elasticsearch(host):
    """Establish a connection to the Elasticsearch cluster."""
    return Elasticsearch(hosts=host)

def search_documents(es_client, index_name, query):
    """Perform a search query on the specified index."""
    try:
        response = es_client.search(index=index_name, body=query)
        return response['hits']['hits']
    except Exception as e:
        print(f"Error executing search query: {e}")
        return []

if __name__ == "__main__":
    # Connect to Elasticsearch
    es = connect_to_elasticsearch("http://localhost:9200")

    # Define the search query
    doc = {
        "query": {"match_all": {}},
        "size": 10
    }

    # Perform the search and retrieve results
    results = search_documents(es, "users", doc)

    # Print the search results
    for result in results:
        print(result)
