import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


import pyspark.sql.functions as f

from constants import get_paths_from_job_path

from custom_logger import get_custom_logger

from splink_graph.cluster_metrics import cluster_basic_stats

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
    PARALLELISM = 20
else:
    PARALLELISM = 100


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


df_edges = spark.read.parquet(paths["edges_path"])
df_edges.createOrReplaceTempView("df_edges")

df_clusters = spark.read.parquet(paths["clusters_path"])
df_clusters.createOrReplaceTempView("df_clusters")

sql = """
select
    unique_id_l as src,
    unique_id_r as dst,
    tf_adjusted_match_prob as weight,
    cluster_medium as cluster_id
from df_edges
left join
df_clusters on
df_edges.unique_id_l = df_clusters.unique_id

"""
df = spark.sql(sql)

basic_stats = cluster_basic_stats(df)

out_path = paths["graph_analytics_path"]
out_path = os.path.join(out_path, "cluster_metrics")

basic_stats = basic_stats.repartition(1)
basic_stats.write.parquet(out_path)
