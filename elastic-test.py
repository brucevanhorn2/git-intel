from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

try:
    health = es.cluster.health()
    print("Connected to Elasticsearch:", health["status"])
except Exception as e:
    print("Connection failed:", e)
