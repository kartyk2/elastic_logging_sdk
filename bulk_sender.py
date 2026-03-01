import threading
import time
import json

class BulkSender:

    def __init__(self, elastic_client, batch_size=100, flush_interval=2):
        self.elastic_client = elastic_client
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.lock = threading.Lock()
        self.running = True

        self.thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.thread.start()

    def add(self, record_json):
        with self.lock:
            self.buffer.append(record_json)

            if len(self.buffer) >= self.batch_size:
                self._flush()

    def _flush(self):
        if not self.buffer:
            return

        actions = []
        index_name = self.elastic_client.index_name()

        for record in self.buffer:
            actions.append({
                "index": {"_index": index_name}
            })
            actions.append(record)

        try:
            self.elastic_client.bulk_index(actions)
        except Exception as e:
            print("Elastic bulk error:", e)

        self.buffer = []

    def _flush_loop(self):
        while self.running:
            time.sleep(self.flush_interval)
            with self.lock:
                self._flush()

    def stop(self):
        self.running = False
        self.thread.join()
        with self.lock:
            self._flush()