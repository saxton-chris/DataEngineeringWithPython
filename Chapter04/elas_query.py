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

def search_with_lucene(es_client, index_name, lucene_query, size=10):
    """Perform a search query using Lucene syntax."""
    try:
        response = es_client.search(index=index_name, q=lucene_query, size=size)
        return response['hits']['hits']
    except Exception as e:
        print(f"Error executing Lucene query: {e}")
        return []

if __name__ == "__main__":
    # Connect to Elasticsearch
    es = connect_to_elasticsearch("http://localhost:9200")

    # Define the search query to retrieve all documents
    doc = {
        "query": {"match_all": {}},
        "size": 10
    }

    # Perform the search and retrieve results
    results = search_documents(es, "users", doc)

    # Print the search results
    print("All documents:")
    for result in results:
        print(result)

    # Search for a specific user by name
    print("\nSearch for Ronald Goodman:")
    doc = {"query": {"match": {"name": "Ronald Goodman"}}, "size": 10}
    results = search_documents(es, "users", doc)
    if results:
        print(results[0]['_source'])

    # Search for Ronald Goodman using Lucene syntax
    print("\nSearch for Ronald Goodman using Lucene syntax:")
    results = search_with_lucene(es, "users", "name:Ronald Goodman", size=10)
    if results:
        print(results[0]['_source'])

    # Search for a city, including partial matches
    print("\nSearch for cities matching Jamesberg:")
    doc = {"query": {"match": {"city": "Jamesberg"}}, "size": 10}
    results = search_documents(es, "users", doc)
    print("Jamesberg results:")
    for result in results:
        print(result)

    # Search for a city and filter by zip code
    print("\nSearch for Jamesberg filtered by zip code:")
    doc = {
        "query": {
            "bool": {
                "must": {"match": {"city": "Jamesberg"}},
                "filter": {"term": {"zip": "63792"}}
            }
        },
        "size": 10
    }
    results = search_documents(es, "users", doc)
    print("Filtered Jamesberg results:")
    for result in results:
        print(result)
