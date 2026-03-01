from formatter import JSONFormatter
from elastic import ElasticClient
import logging
import logging.handlers
import json
import multiprocessing
from datetime import datetime


def create_log_queue():
    return multiprocessing.Queue(-1)


def start_listener(queue, config, elastic_config=None, enable_console=True):

    # -------------------------------
    # File Handler (JSON)
    # -------------------------------
    if config.rotation == "size":
        file_handler = logging.handlers.RotatingFileHandler(
            config.log_file_path(),
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
        )
    else:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            config.log_file_path(),
            when=config.rotation,
            backupCount=config.backup_count,
            utc=config.utc,
        )

    json_formatter = JSONFormatter()
    file_handler.setFormatter(json_formatter)

    handlers = [file_handler]

    # -------------------------------
    # Console Handler (Readable)
    # -------------------------------

    if enable_console:
        console_handler = logging.StreamHandler()

        console_formatter = logging.Formatter(
            "%(asctime)s | %(processName)s | %(levelname)s | %(message)s"
        )

        console_handler.setFormatter(console_formatter)
        handlers.append(console_handler)

    # -------------------------------
    # Elasticsearch Handler
    # -------------------------------
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if elastic_config:

        es = ElasticClient(
            hosts=elastic_config["hosts"],
            project=config.project,
            environment=config.environment,
        )

        class ElasticHandler(logging.Handler):

            def emit(self, record):
                try:
                    record_json = json.loads(json_formatter.format(record))
                    es.index(record_json)
                except Exception:
                    pass

        handlers.append(ElasticHandler())
    # -------------------------------
    # Queue Listener
    # -------------------------------

    listener = logging.handlers.QueueListener(queue, *handlers)
    listener.start()

    return listener
