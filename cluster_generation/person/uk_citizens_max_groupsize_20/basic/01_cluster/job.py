import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from graphframes import GraphFrame

import pyspark.sql.functions as f

from constants import get_paths_from_job_path
from cluster_utils import clusters_at_thresholds

from custom_logger import get_custom_logger


sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()

sc.setCheckpointDir("s3://alpha-splink-synthetic-data/temp_files/checkpointdir/")

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

# Set up a custom logger than outputs to its own stream, grouped within the job run id
# to separate out custom logs from general spark logs
custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {args['snapshot_date']}")


# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"], args["snapshot_date"], args["version"], trial_run=trial_run
)
for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")


# # Source nodes is the original list of people - no guarantee the edges will include every person.
df_source_nodes = spark.read.parquet(paths["source_nodes_path"])

# Only need the list of IDs for the cluster output, will not pull in any details
df_source_nodes = df_source_nodes.select(["unique_id", "source_dataset"])


df_source_nodes = df_source_nodes.withColumn("commit_hash", f.lit(args["commit_hash"]))

df_edges = spark.read.parquet(paths["edges_path"])

df_edges.createOrReplaceTempView("df_edges")
df_source_nodes.createOrReplaceTempView("df_source_nodes")


results = clusters_at_thresholds(
    df_source_nodes,
    df_edges,
    [0.05, 0.5, 0.8, 0.99, 0.999],
    [
        "cluster_very_low",
        "cluster_low",
        "cluster_medium",
        "cluster_high",
        "cluster_very_high",
    ],
    spark,
    join_node_details=True,
    score_colname="tf_adjusted_match_prob",
)

results = results.repartition(50)
results.persist()
results.write.mode("overwrite").parquet(paths["clusters_path"])
results.createOrReplaceTempView("results")
