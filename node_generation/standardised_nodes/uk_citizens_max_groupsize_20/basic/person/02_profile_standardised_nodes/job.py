import sys
from dataengineeringutils3.s3 import write_local_file_to_s3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import os


from constants import get_paths_from_job_path
from custom_logger import get_custom_logger

from pyspark.sql import functions as f

from spark_config import add_udfs

from splink.profile import (
    column_combination_value_frequencies_chart,
    column_value_frequencies_chart,
)

sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()
spark = glue_context.spark_session

add_udfs(spark)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "job_path",
        "snapshot_date",
        "commit_hash",
        "trial_run",
        "version",
    ],
)

SNAPSHOT_DATE = args["snapshot_date"]


# Set up a custom logger than outputs to its own stream, grouped within the job run id
# to separate out custom logs from general spark logs
custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {SNAPSHOT_DATE}")

trial_run = args["trial_run"] == "true"

VERSION = args["version"]

paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")


input_path = paths["standardised_nodes_path"]
df = spark.read.parquet(input_path)


charts_dir = paths["charts_directory_path"]

potential_blocking_rules = [
    ["outward_postcode_std", "dob"],
    ["inward_postcode_std", "outward_postcode_std", "forename1_dm"],
    ["inward_postcode_std", "outward_postcode_std", "surname_dm"],
    ["forename1_dm", "surname_dm"],
    ["forename1_dm", "occupation", "dob_year"],
]

chart = column_combination_value_frequencies_chart(
    potential_blocking_rules, df, spark, top_n=50, bottom_n=20
)

chart_name = "potential_blocking_rules.html"
chart.save(chart_name)
col_val_s3_path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, col_val_s3_path, overwrite=True)


# This has been run so doesn't need to be run again
cols_to_profile = [
    "uncorrupted_record",
    "id",
    "dob",
    "birth_place",
    "gender",
    "occupation",
    "unique_id",
    "surname_std",
    "forename1_std",
    "forename2_std",
    "forename3_std",
    "forename4_std",
    "forename5_std",
    "outward_postcode_std",
    "inward_postcode_std",
    "dob_year_month",
    "dob_month_day",
    "dob_year_day",
    "dob_year",
    "dob_month",
    "dob_day",
    "surname_dm",
    "forename1_dm",
    "forename2_dm",
]

chart = column_combination_value_frequencies_chart(
    cols_to_profile, df, spark, top_n=50, bottom_n=20
)

chart_name = "col_val_freqs.html"
chart.save(chart_name)
col_val_s3_path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, col_val_s3_path, overwrite=True)
