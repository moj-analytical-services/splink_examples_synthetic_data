import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


from normalise_prob import probability_to_normalised_bayes_factor

from cluster_utils import (
    format_edges_and_clusters_df_for_use_in_splink_graph,
    get_all_cluster_metrics,
    cluster_counts,
    cluster_stability_statistics_raw,
    cluster_stability_statistics_specific_threshold,
)


from constants import get_paths_from_job_path, get_graph_analytics_path, parse_path

from custom_logger import get_custom_logger


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


df_edges = probability_to_normalised_bayes_factor(
    df_edges, "tf_adjusted_match_prob", "weight"
)
df_edges.createOrReplaceTempView("df_edges")

df_clusters = spark.read.parquet(paths["clusters_path"])
df_clusters.createOrReplaceTempView("df_clusters")

custom_log.info(f"{df_edges.count()} =df_edges.count()")
custom_log.info(f"{df_clusters.count()} =df_clusters.count()")


cluster_colnames = [
    "cluster_very_very_low",
    "cluster_very_low",
    "cluster_quite_low",
    "cluster_low",
    "cluster_medium",
    "cluster_high",
    "cluster_very_high",
]

cluster_thresholds = [0.01, 0.1, 0.25, 0.5, 0.8, 0.99, 0.999]

name_thres = list(zip(cluster_colnames, cluster_thresholds))[-5:]
# name_thres = list(zip(cluster_colnames, cluster_thresholds))[-2:]


# Cluster stability statistics
cc_df = cluster_counts(df_clusters, cluster_colnames)
cluster_stability_raw_df = cluster_stability_statistics_raw(cc_df, cluster_colnames)
cluster_stability_raw_df.persist()

# Add cluster stability statistics.  This is computed once at the start and then needs to be
# aggregated then joined on to cluster_all_stats_df

for cluster_colname, cluster_threshold in name_thres:

    parsed_args = parse_path(args["job_path"])
    out_path = get_graph_analytics_path(
        parsed_args["entity"],
        parsed_args["dataset_or_datasets"],
        parsed_args["job_name"],
        args["snapshot_date"],
        args["version"],
        "cluster",
        cluster_colname,
        trial_run=trial_run,
    )

    custom_log.info(f"Path would be {out_path}")

    fil = f"tf_adjusted_match_prob > {cluster_threshold}"
    df_splink_graph = format_edges_and_clusters_df_for_use_in_splink_graph(
        df_edges, df_clusters, cluster_colname, "weight", fil, spark
    )

    # Use Splink Graph to get dataframe of cluster metrics at this threshold
    # e.g. cluster_id = 10, density=0.8, diameter = 3 etc
    cluster_metrics_splink_graph_df = get_all_cluster_metrics(df_splink_graph)

    # Join on cluster stability statistics at this level
    # This is a dataframe like this:
    # cluster_medium, avg_cluster_stdev
    cluster_stab = cluster_stability_statistics_specific_threshold(
        cluster_stability_raw_df, cluster_colname
    )

    # Join splink graph stats to cluster stab
    join_cond = (
        cluster_metrics_splink_graph_df["cluster_id"] == cluster_stab[cluster_colname]
    )
    cluster_metrics_inc_stability_df = cluster_stab.join(
        cluster_metrics_splink_graph_df, on=join_cond, how="inner"
    )

    cluster_metrics_inc_stability_df = cluster_metrics_inc_stability_df.repartition(1)
    cluster_metrics_inc_stability_df.write.mode("overwrite").parquet(out_path)

    custom_log.info(
        f"Written cluster_metrics_inc_stability_df for {cluster_colname} threshold {cluster_threshold}"
    )
