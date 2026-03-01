import contextvars
import socket
import os
import uuid

correlation_id_var = contextvars.ContextVar("correlation_id", default=None)
job_id_var = contextvars.ContextVar("job_id", default=None)

hostname = socket.gethostname()
pid = os.getpid()

def set_correlation_id(value=None):
    if value is None:
        value = str(uuid.uuid4())
    correlation_id_var.set(value)
    return value

def get_correlation_id():
    return correlation_id_var.get()

def set_job_id(value):
    job_id_var.set(value)

def get_job_id():
    return job_id_var.get()

def get_runtime_context():
    return {
        "hostname": hostname,
        "pid": pid,
        "correlation_id": get_correlation_id(),
        "job_id": get_job_id(),
    }