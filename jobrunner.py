#!/usr/bin/env python3
import argparse
import logging
import sys


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

from job_configs.uk_citizens_max_groupsize_20 import (
    uk_citizens_max_groupsize_20,
)
from job_configs.uk_citizens_max_groupsize_20_splink_v2 import (
    uk_citizens_max_groupsize_20_splink_v2,
)

from job_configs.uk_citizens_max_groupsize_20_splink_v1 import (
    uk_citizens_max_groupsize_20_splink_v1,
)

from job_runner_utils.run_job import run_job


jobs = {
    **uk_citizens_max_groupsize_20,
    **uk_citizens_max_groupsize_20_splink_v2,
    **uk_citizens_max_groupsize_20_splink_v1,
}

for k, j in jobs.items():
    j["job_path"] = k


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="data_linking job runner")

    parser.add_argument(
        "--job_path", type=str, help="Relative path to folder with job.py"
    )

    parser.add_argument(
        "--snapshot_date", type=str, help="snapshot date as string e.g. 2020-01-01"
    )

    parser.add_argument(
        "--version", type=str, help="snapshot date as string e.g. 2020-01-01"
    )

    parser.add_argument("--trial_run", action="store_true", help="")

    parser.add_argument("--log_job_def", action="store_true", help="")

    args = parser.parse_args()

    print("Your argument values are:")
    print(f'job_path is string "{args.job_path}"')
    print(f'snapshot_date is string "{args.snapshot_date}"')
    print(f"trial_run is boolean {args.trial_run}")

    # Retrieve job args from dict
    if args.job_path:

        job_args = jobs[args.job_path]

        if args.trial_run:
            job_args["trial_run"] = "true"
            job_args["allocated_capacity"] = 3
        else:
            job_args["trial_run"] = "false"

        if args.log_job_def:
            job_args["log_job_def"] = True

        if args.version:
            job_args["version"] = args.version

        if args.snapshot_date:
            job_args["snapshot_date"] = args.snapshot_date

        print(job_args)
        run_job(**job_args)
    else:
        for k in jobs.keys():
            print(f"python jobrunner.py --job_path {k} --trial_run")
