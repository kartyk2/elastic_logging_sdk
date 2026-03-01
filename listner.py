import logging
import logging.handlers
import multiprocessing
from formatter import JSONFormatter


def create_log_queue():
    return multiprocessing.Queue(-1)


def start_listener(queue, config):

    if config.rotation == "size":
        handler = logging.handlers.RotatingFileHandler(
            config.log_file_path(),
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
        )
    else:
        handler = logging.handlers.TimedRotatingFileHandler(
            config.log_file_path(),
            when=config.rotation,
            backupCount=config.backup_count,
            utc=config.utc,
        )

    handler.setFormatter(JSONFormatter())

    listener = logging.handlers.QueueListener(queue, handler)
    listener.start()
    return listener
