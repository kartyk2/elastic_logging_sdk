from elasticsearch import Elasticsearch
from datetime import datetime
import socket


class ElasticClient:
    """
    Thin wrapper around official Elasticsearch v8 client.
    """

    def __init__(self, hosts, project, environment):

        self.project = project
        self.environment = environment
        self.hostname = socket.gethostname()

        self.client = Elasticsearch(hosts)

        self._validate_connection()

    # -------------------------------------------------
    # Validate connection
    # -------------------------------------------------

    def _validate_connection(self):
        try:
            if not self.client.ping():
                raise RuntimeError("Elasticsearch cluster not reachable")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Elasticsearch: {e}")

    # -------------------------------------------------
    # Generate monthly index
    # -------------------------------------------------

    def index_name(self):
        today = datetime.utcnow().strftime("%Y.%m")
        return f"logs-{self.project}-{self.environment}-{today}"

    # -------------------------------------------------
    # Index single document
    # -------------------------------------------------

    def index(self, document: dict):

        try:
            if "@timestamp" not in document:
                document["@timestamp"] = datetime.utcnow().isoformat()

            self.client.index(
                index=self.index_name(),
                document=document
            )

        except Exception as e:
            print("Elasticsearch indexing error:", e)

    # -------------------------------------------------
    # Optional bulk support
    # -------------------------------------------------

    def bulk_index(self, documents: list):

        if not documents:
            return

        actions = []

        for doc in documents:
            if "@timestamp" not in doc:
                doc["@timestamp"] = datetime.utcnow().isoformat()

            actions.append({
                "index": {"_index": self.index_name()}
            })
            actions.append(doc)

        try:
            self.client.bulk(body=actions)
        except Exception as e:
            print("Bulk indexing error:", e)

    # -------------------------------------------------
    # Health
    # -------------------------------------------------

    def health(self):
        return self.client.cluster.health()