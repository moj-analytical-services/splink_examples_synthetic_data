# Load in params, combine, and produce final edges

import sys
import os
import json

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

import pyspark.sql.functions as f

from dataengineeringutils3.s3 import (
    write_local_file_to_s3,
    delete_s3_folder_contents,
)
from splink import Splink
from splink.diagnostics import splink_score_histogram


from constants import (
    get_paths_from_job_path,
)
from lineage import (
    curried_blocked_comparisons_to_s3,
    curried_scored_comparisons_to_s3,
    curried_persist_model_charts,
)
from custom_logger import get_custom_logger
from spark_config import add_udfs


sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()
spark = glue_context.spark_session

args = getResolvedOptions(
    sys.argv,
    [
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

paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
    blocking_group="combine_blocks",
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")

with open("model.json") as file:
    settings = json.load(file)

PERSON_STANDARDISED_NODES_PATH = paths["standardised_nodes_path"]

person_standarised_nodes = spark.read.parquet(PERSON_STANDARDISED_NODES_PATH)

persist_model_settings = curried_persist_model_charts(paths, custom_log)
blocked_comparisons_to_s3 = curried_blocked_comparisons_to_s3(
    paths, custom_log, PARALLELISM
)
scored_comparisons_to_s3 = curried_scored_comparisons_to_s3(paths, custom_log)

settings["max_iterations"] = 0
settings["retain_intermediate_calculation_columns"] = False
settings["retain_matching_columns"] = True

linker = Splink(
    settings,
    person_standarised_nodes,
    spark,
    save_state_fn=persist_model_settings,
    break_lineage_blocked_comparisons=blocked_comparisons_to_s3,
    break_lineage_scored_comparisons=scored_comparisons_to_s3,
)

df_e = linker.get_scored_comparisons()
df_e = df_e.repartition(100)
count_for_log = df_e.count()
custom_log.info(f"The count of df_e is {count_for_log:,.0f}")

df_e = df_e.withColumn("commit_hash", f.lit(args["commit_hash"]))
df_e.write.mode("overwrite").parquet(paths["edges_path"])

custom_log.info(f"edges writen to: {paths['edges_path']}")

if "temp_files" in paths["blocked_tempfiles_path"]:
    delete_s3_folder_contents(paths["blocked_tempfiles_path"])

if "temp_files" in paths["scored_tempfiles_path"]:
    delete_s3_folder_contents(paths["scored_tempfiles_path"])


# Persist final version of charts into 'charts' folder
chart_name = "final_splink_charts_edge_generation.html"
linker.model.all_charts_write_html_file(filename=chart_name, overwrite=True)
charts_dir = paths["charts_directory_path"]
path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, path, overwrite=True)

# Persist histogram of splink score
df_e = spark.read.parquet(paths["edges_path"])

chart = splink_score_histogram(df_e, spark, 100)
chart_name = "splink_scores_histogram.html"
chart.save(chart_name)

charts_dir = paths["charts_directory_path"]
path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, path, overwrite=True)
