import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import os

from pyspark.sql.functions import lit

from dataengineeringutils3.s3 import write_json_to_s3, write_local_file_to_s3
from splink import Splink

from constants import (
    get_paths_from_job_path,
    parse_path,
)
from lineage import curried_blocked_comparisons_to_s3, curried_scored_comparisons_to_s3
from custom_logger import get_custom_logger
from spark_config import add_udfs

from splink.estimate import estimate_u_values
from splink.model import Model
from splink.maximisation_step import run_maximisation_step
from splink.settings import Settings

from splink_settings import settings

sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()
spark = glue_context.spark_session

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

trial_run = args["trial_run"] == "true"
if trial_run:
    PARALLELISM = 30
else:
    PARALLELISM = 500

spark.conf.set("spark.sql.shuffle.partitions", PARALLELISM)
spark.conf.set("spark.default.parallelism", PARALLELISM)

add_udfs(spark)


# Set up a custom logger than outputs to its own stream, grouped within the job run id
# to separate out custom logs from general spark logs
custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {args['snapshot_date']}")

custom_log.info(args)

job_name = parse_path(args["job_path"])["job_name"]

# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"], args["snapshot_date"], args["version"], trial_run=trial_run
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")

PERSON_STANDARDISED_NODES_PATH = paths["standardised_nodes_path"]
person_standardised_nodes = spark.read.parquet(PERSON_STANDARDISED_NODES_PATH)

if trial_run:
    target_rows = 1e6
else:
    target_rows = 1e8

count_for_log = person_standardised_nodes.count()
custom_log.info(f"The count of source nodes is {count_for_log:,.0f}")

blocked_comparisons_to_s3 = curried_blocked_comparisons_to_s3(
    paths, custom_log, PARALLELISM
)
scored_comparisons_to_s3 = curried_scored_comparisons_to_s3(paths, custom_log)

# Estimate u params for all columns from cartesian product
settings_with_u_dict = estimate_u_values(
    settings,
    person_standardised_nodes,
    spark,
    target_rows=target_rows,
    fix_u_probabilities=True,
)


s3_output_path = os.path.join(paths["training_models_path"], "settings_with_u.json")
write_json_to_s3(settings_with_u_dict, s3_output_path)

custom_log.info(f"Written settings dict to to {s3_output_path}")
