import os
from watchtower import CloudWatchLogHandler
import logging


def get_custom_logger(job_run_id):
    # Set up a custom logger so splink logs are written into a single place away from spark logs but within the same log group
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
    lsn = f"{job_run_id}_custom"
    cw = CloudWatchLogHandler(
        log_group="/aws-glue/jobs/logs-v2", stream_name=lsn, send_interval=4
    )
    slog = logging.getLogger("splink")
    slog.setLevel(logging.INFO)
    slog.handlers = []
    slog.addHandler(cw)
    slog.info("hello from the custom logger")
    return slog
