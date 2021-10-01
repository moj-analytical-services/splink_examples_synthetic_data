import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


from cluster_truth_utils import label_est_clusters_fp_fn


from constants import (
    get_paths_from_job_path,
    parse_path,
    get_cluster_truth_path,
    get_graph_analytics_path,
)

from custom_logger import get_custom_logger


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

custom_log = get_custom_logger(args["JOB_RUN_ID"])
custom_log.info(f"Snapshot date is {args['snapshot_date']}")


# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
    labelling_exercise="qa_2021",
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")

custom_log.info("----")
parsed_args = parse_path(args["job_path"])
for k, v in parsed_args.items():
    custom_log.info(f"{k:<50} {v}")


common_args = {
    "entity": "person",
    "dataset_or_datasets": parsed_args["dataset_or_datasets"],
    "job_name": parsed_args["job_name"],
    "snapshot_date": args["snapshot_date"],
    "version": args["version"],
    "trial_run": trial_run,
}

graph_analytics_path_args = {
    **common_args,
    "metric_entity_type": "cluster",
}


cluster_truth_path_args = {
    "labelling_exercise": "synth_data",
    **common_args,
}


df_edges = spark.read.parquet(paths["edges_path"])
df_edges.createOrReplaceTempView("df_edges")

df_clusters = spark.read.parquet(paths["clusters_path"])
df_clusters.createOrReplaceTempView("df_clusters")

cluster_colnames = [
    "cluster_very_very_low",
    "cluster_very_low",
    "cluster_quite_low",
    "cluster_low",
    "cluster_medium",
    "cluster_high",
    "cluster_very_high",
]


# for cluster_colname in cluster_colnames[-5:]:
for cluster_colname in cluster_colnames[-5:]:

    ga_path = get_graph_analytics_path(
        **graph_analytics_path_args, cluster_colname=cluster_colname
    )
    out_path = get_cluster_truth_path(
        **cluster_truth_path_args, cluster_colname=cluster_colname
    )
    cluster_metrics_inc_stability_df = spark.read.parquet(ga_path)

    nodes_with_est_and_gt_cluster = df_clusters.selectExpr(
        "unique_id", "cluster as ground_truth_cluster", cluster_colname
    )

    df_fp_fn = label_est_clusters_fp_fn(
        nodes_with_est_and_gt_cluster, cluster_colname, False
    )

    results = cluster_metrics_inc_stability_df.join(
        df_fp_fn, on=cluster_colname, how="left"
    )
    results.write.mode("overwrite").parquet(out_path)
