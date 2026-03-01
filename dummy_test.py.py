import multiprocessing
import random
import time

from logger import  get_logger
from config import LoggingConfig
from context import (
    set_correlation_id,
    set_job_id,
)

from listner import create_log_queue, start_listener
from logger import configure_worker_logger, get_logger


# -----------------------------------
# Worker
# -----------------------------------


def worker(worker_id, queue):
    configure_worker_logger(queue)
    logger = get_logger(f"worker-{worker_id}")

    for i in range(5):
        job_id = f"job-{worker_id}-{i}"

        set_correlation_id()
        set_job_id(job_id)

        logger.info(f"Starting job iteration {i}")

        time.sleep(random.uniform(0.1, 0.4))

        if random.choice([True, False]):
            try:
                1 / 0
            except Exception:
                logger.exception("Simulated failure")

        logger.info(f"Completed job iteration {i}")

    logger.info("Worker finished execution")


# -----------------------------------
# Main
# -----------------------------------

if __name__ == "__main__":

    config = LoggingConfig(project="dummy-service", environment="dev", log_dir="./logs")

    log_queue = create_log_queue()
    listener = start_listener(log_queue, config)

    configure_worker_logger(log_queue)
    main_logger = get_logger("main")

    main_logger.info("Starting multiprocessing logging test")

    processes = []

    for i in range(4):
        p = multiprocessing.Process(target=worker, args=(i, log_queue))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    main_logger.info("All workers completed")

    listener.stop()

    print("\n✔ Dummy multiprocessing test complete.")
    print("✔ Check ./logs/dummy-service-dev.log\n")
