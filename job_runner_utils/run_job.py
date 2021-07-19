import pprint
import json
import sys
from etl_manager.etl import GlueJob
import os
import subprocess

import boto3
import tempfile
from distutils.dir_util import copy_tree
import time

import logging
from pkg_resources import Requirement

from pkg_resources.extern.packaging.requirements import InvalidRequirement

from local_utils.clickabe_glue import clickable_glue_links
from local_utils.clickable_s3 import clickable_s3_links

logger = logging.getLogger(__name__)


def _set_up_temp_folder(job_path):
    # Copy the job and shared job resources into the right folder

    temp_dir = tempfile.TemporaryDirectory()

    tmpdirname = temp_dir.name
    job_base_folder = os.path.basename(job_path)
    job_dir_name = os.path.join(tmpdirname, job_base_folder)
    copy_tree(job_path, job_dir_name)
    copy_tree("shared_job_resources/", os.path.join(tmpdirname, "shared_job_resources"))

    check_job_exists_and_compile(job_dir_name)

    return temp_dir, job_dir_name


def check_job_exists_and_compile(job_dir_name):
    # Check there are no compile errors before submitting
    source = open(os.path.join(job_dir_name, "job.py"), "r").read() + "\n"
    compile(source, os.path.join(job_dir_name, "job.py"), "exec")


def get_python_modules(modules_list: list, remove_python_modules: list = []):
    """Turn a list of python modules into the format needed for the
    --additional-python-modules job argument with validation

    Args:
        modules_list (list): list of modules like ['wordninja','splink==0.2.5']
        remove_python_modules: list of modules to remove from defaults, e.g. if you want to use splink from a zip you'd specify ['splink']

    Returns:
        str: 'wordninja,splink==0.2.5'
    """

    if type(remove_python_modules) != list:
        raise ValueError(
            f"remove_python_modules should be a list, you passed {remove_python_modules} of "
            f"type {type(remove_python_modules)}"
        )

    modules = {
        "watchtower": "0.8.0",
        "splink": "1.0.5",
        "dataengineeringutils3": "1.1.0",
        "altair": "4.1.0",
        "typeguard": "2.10.0",
        "pydbtools": "2.0.2",
    }

    # Overwrite defaults with options provided by user
    for m in modules_list:

        ma = m.split("==")

        if len(ma) == 2:
            modules[ma[0]] = ma[1]
        if len(ma) == 1:
            modules[ma[0]] = None

    modules_list = []

    for k, v in modules.items():
        if k not in remove_python_modules:
            if v:
                modules_list.append(f"{k}=={v}")
            else:
                modules_list.append(f"{k}")

    # Check modules have een specified corectly
    for r in modules_list:
        try:
            Requirement.parse(r)
        except InvalidRequirement:
            raise ValueError(f"{r} is not a valid way of specifying a python module")
        pass

    return ",".join(modules_list)


def run_job(
    job_path,
    role,
    snapshot_date,
    version,
    list_python_modules=[],
    allocated_capacity=3,
    job_name=None,
    spark_ui=False,
    trial_run=False,
    log_job_def=False,
    additional_job_args={},
    remove_python_modules=[],
):

    if job_name is None:
        job_name = job_path.replace("/", "_")

    temp_folder, job_temp_folder = _set_up_temp_folder(job_path)

    # where should etl manager upload the code/jars etc to?
    etl_manager_bucket = "alpha-splink-synthetic-data"

    commit_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
    commit_hash = commit_hash.decode("ascii").strip()

    job_arguments = {
        "--snapshot_date": snapshot_date,
        "--commit_hash": commit_hash,
        "--trial_run": trial_run,
        "--version": version,
        "--job_path": job_path,
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-metrics": "",
    }

    job_arguments["--additional-python-modules"] = get_python_modules(
        list_python_modules, remove_python_modules
    )

    if spark_ui:
        ui_args = {
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": f"s3://alpha-splink-synthetic-data/glue_logs/{job_name}",
        }
        job_arguments = {**job_arguments, **ui_args}

    # Allows user to optionally pass in arbitrary job args
    job_arguments = {**job_arguments, **additional_job_args}

    job = GlueJob(
        job_temp_folder,
        bucket=etl_manager_bucket,
        job_role=role,
        job_arguments=job_arguments,
    )

    job.job_name = job_name
    job.allocated_capacity = allocated_capacity

    start_time = time.time()
    try:
        job.run_job()

        logger.info("Job arguments are:")
        logger.info(json.dumps(job_arguments, indent=4))
        if log_job_def:
            logger.info("ETL_manager job defininition is:")
            logger.info(json.dumps(job._job_definition(), indent=4))
            logger.info("Initial Glue Job status is:")
            logger.info(pprint.pprint(job.job_status, indent=2))

        clickable_glue_links(job, logger)
        clickable_s3_links(job_arguments, logger)

        job.wait_for_completion()

    finally:

        elapsed_seconds = time.time() - start_time
        elapsed_hours = elapsed_seconds / (60 * 60)
        cost = (
            elapsed_hours * allocated_capacity * 0.34
        )  # 0.34 is cost per dpu hour in £

        logger.info(
            f"Job took {elapsed_seconds/60:,.2f} minutes with {allocated_capacity} workers"
        )
        logger.info(f"Cost of job was £{cost:,.2f}")

        try:
            # Job logs
            client = boto3.client("logs", "eu-west-1")

            logStreamName = job.job_status["JobRun"]["Id"] + "_custom"
            response = client.get_log_events(
                logGroupName="/aws-glue/jobs/logs-v2", logStreamName=logStreamName
            )
            messages = [e["message"] for e in response["events"]]

            logger.info("GLUE CUSTOM LOG:")
            logger.info("\n" + "\n".join(messages))
        except client.exceptions.ClientError:
            logger.info("errror retrieving custom logs")

        if job.job_status["JobRun"]["JobRunState"] == "SUCCEEDED":
            job.cleanup()
        else:
            logger.info(f"Job status: {job.job_status['JobRun']['JobRunState']}")
