import logging
import logging.handlers

def configure_worker_logger(queue, level=logging.INFO):
    root = logging.getLogger()
    root.setLevel(level)

    root.handlers = []

    handler = logging.handlers.QueueHandler(queue)
    root.addHandler(handler)

def get_logger(name=None):
    return logging.getLogger(name)