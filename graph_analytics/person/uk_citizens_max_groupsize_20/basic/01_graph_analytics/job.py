import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import json

from normalise_prob import probability_to_normalised_bayes_factor

import pyspark.sql.functions as f

from constants import get_paths_from_job_path

from custom_logger import get_custom_logger

from splink_graph.cluster_metrics import (
    cluster_basic_stats,
    cluster_main_stats,
    cluster_eb_modularity,
)
from splink_graph.node_metrics import eigencentrality
from splink_graph.edge_metrics import edgebetweeness

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
    PARALLELISM = 40
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


df_edges = probability_to_normalised_bayes_factor(df_edges, "tf_adjusted_match_prob")
df_edges.createOrReplaceTempView("df_edges")

df_clusters = spark.read.parquet(paths["clusters_path"])
df_clusters.createOrReplaceTempView("df_clusters")

custom_log.info(f"{df_edges.count()} =df_edges.count()")
custom_log.info(f"{df_clusters.count()} =df_clusters.count()")

# where  df_c_1.cluster_medium = 13
sql = """
select
    unique_id_l as src,
    unique_id_r as dst,
    match_score_norm as weight,
    df_c_1.cluster_medium as cluster_id
from
    df_edges

left join
    df_clusters df_c_1
    on df_edges.unique_id_l = df_c_1.unique_id

left join
    df_clusters as df_c_2
    on df_edges.unique_id_r = df_c_2.unique_id

where
    df_c_1.cluster_medium = df_c_2.cluster_medium
"""

df = spark.sql(sql)

custom_log.info(f"{df.count()} =df.count()")


cluster_basic_stats_df = cluster_basic_stats(df)

cluster_main_stats_df = cluster_main_stats(df)


cluster_eb_modularity_df = cluster_eb_modularity(df, distance_colname="weight")

cluster_all_stats_df = cluster_basic_stats_df.join(
    cluster_main_stats_df, on=["cluster_id"], how="left"
).join(cluster_eb_modularity_df, on=["cluster_id"], how="left")


out_path_root = paths["graph_analytics_path"]

out_path = os.path.join(out_path_root, "all_cluster_metrics")
cluster_all_stats_df = cluster_all_stats_df.repartition(1)
cluster_all_stats_df.write.mode("overwrite").parquet(out_path)

custom_log.info(f"Written cluster_all_stats_df")

# node_df = eigencentrality(df, distance_colname="weight")
# out_path = os.path.join(out_path_root, "node_metrics")
# node_df = node_df.repartition(1)
# node_df.write.mode("overwrite").parquet(out_path)

# custom_log.info(f"Written node_df")


# edge_metrics_df = edgebetweeness(df, distance_col="weight")

# df_edges.createOrReplaceTempView("df_edges")
# edge_metrics_df.createOrReplaceTempView("edge_metrics_df")

# sql = """
# select
#     em.*,
#     e.match_score_norm,
#     e.tf_adjusted_match_prob,
#     e.unique_id_l,
#     e.unique_id_r

# from
# edge_metrics_df as em
# left join df_edges as e
# on
# em.src = e.unique_id_l and em.dst = e.unique_id_r

# union all


# select
#     em.*,
#     e.match_score_norm,
#     e.tf_adjusted_match_prob,
#     e.unique_id_l,
#     e.unique_id_r

# from
# edge_metrics_df as em
# left join df_edges as e
# on
# em.src = e.unique_id_r and em.dst = e.unique_id_l

# """

# edge_metrics_df = spark.sql(sql)
# edge_metrics_df = edge_metrics_df.repartition(10)
# out_path = os.path.join(out_path_root, "edge_metrics")
# edge_metrics_df.write.mode("overwrite").parquet(out_path)

# custom_log.info(f"Written edge_metrics_df")
