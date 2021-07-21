import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import json

import pyspark.sql.functions as f

from constants import get_paths_from_job_path

from custom_logger import get_custom_logger

from splink_graph.cluster_metrics import cluster_basic_stats, cluster_main_stats

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


def getShowString(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 20, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


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
    cast(df_c_1.cluster_medium as int) as cluster_id
from df_edges

left join
df_clusters df_c_1 on
df_edges.unique_id_l = df_c_1.unique_id

left join
df_clusters as df_c_2 on
df_edges.unique_id_r = df_c_2.unique_id

where df_c_1.cluster_medium = 13

and df_c_1.cluster_medium = df_c_2.cluster_medium
"""
df = spark.sql(sql)

custom_log.info(getShowString(df))
schema = df.schema.jsonValue()
custom_log.info(json.dumps(schema, indent=4))

cluster_basic_stats_df = cluster_basic_stats(df)

out_path_root = paths["graph_analytics_path"]

out_path = os.path.join(out_path_root, "basic_cluster_metrics")
cluster_basic_stats_df = cluster_basic_stats_df.repartition(1)
cluster_basic_stats_df.write.mode("overwrite").parquet(out_path)


cluster_main_stats_df = cluster_main_stats(df)

custom_log.info(getShowString(cluster_main_stats_df))
# cluster_all_stats_df = cluster_basic_stats_df.join(
#     cluster_main_stats_df, on=["cluster_id"], how="left"
# )


# out_path = os.path.join(out_path_root, "main_cluster_metrics")
# cluster_main_stats_df = cluster_main_stats_df.repartition(1)
# cluster_main_stats_df.write.mode("overwrite").parquet(out_path)

# alueError: Vertex ID column id missing from vertex DataFrame, which has columns: unique_id,source_dataset,commit_hash
