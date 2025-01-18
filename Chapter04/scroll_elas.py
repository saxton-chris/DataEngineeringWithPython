from elasticsearch import Elasticsearch

def connect_to_elasticsearch(host):
    """Establish a connection to the Elasticsearch cluster."""
    return Elasticsearch(hosts=host)

def initialize_scroll(es_client, index_name, query, scroll_time='20m', batch_size=500):
    """Initialize a scroll context to retrieve documents in batches."""
    return es_client.search(index=index_name, scroll=scroll_time, body={**query, "size": batch_size})

def scroll_through_documents(es_client, scroll_id, scroll_time='20m'):
    """Retrieve documents in batches using the scroll API."""
    try:
        while True:
            response = es_client.scroll(scroll_id=scroll_id, scroll=scroll_time)
            hits = response['hits']['hits']

            if not hits:
                break

            for doc in hits:
                print(doc['_source'])

            scroll_id = response['_scroll_id']

        es_client.clear_scroll(scroll_id=scroll_id)
        print("Scroll operation completed.")

    except Exception as e:
        print(f"Error during scroll operation: {e}")

if __name__ == "__main__":
    # Connect to Elasticsearch
    es = connect_to_elasticsearch("http://localhost:9200")
    print("CONNECTION ESTABLISHED")

    # Define the search query
    query = {"query": {"match_all": {}}}

    # Initialize the scroll operation
    try:
        res = initialize_scroll(es, "users", query, scroll_time='20m', batch_size=500)
        print("\nRESULTS GATHERED")

        # Extract the scroll ID and start scrolling
        sid = res['_scroll_id']
        total_size = res['hits']['total']['value']
        print(f"\nTOTAL DOCUMENTS: {total_size}")

        # Scroll through the documents
        scroll_through_documents(es, sid, scroll_time='20m')

    except Exception as e:
        print(f"Error initializing scroll: {e}")
