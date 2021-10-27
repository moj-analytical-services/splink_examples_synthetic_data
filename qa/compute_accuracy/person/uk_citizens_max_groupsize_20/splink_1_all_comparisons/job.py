import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


import pyspark.sql.functions as f
from pyspark.sql import Window

from splink.truth import (
    labels_with_splink_scores,
    roc_chart,
    precision_recall_chart,
    _get_score_colname,
)
from dataengineeringutils3.s3 import write_local_file_to_s3

from constants import get_paths_from_job_path

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

PARALLELISM = 200
spark.conf.set("spark.sql.shuffle.partitions", PARALLELISM)
spark.conf.set("spark.default.parallelism", PARALLELISM)


custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {args['snapshot_date']}")


# Output paths can be derived from the path
output_paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
    labelling_exercise="qa_2021",
)


for k, v in output_paths.items():
    custom_log.info(f"{k:<50} {v}")


edges_path = output_paths["edges_path"]
df_edges = spark.read.parquet(edges_path)


# Create labelled data from person id
SOURCE_NODES_PATH = output_paths["source_nodes_path"]
SOURCE_NODES_PATH = SOURCE_NODES_PATH.replace("splink_1_all_comparisons", "basic")

df_source = spark.read.parquet(SOURCE_NODES_PATH)


df_source.createOrReplaceTempView("df_source")
df_edges.createOrReplaceTempView("df_edges")

sql = """
select
    df_l.unique_id as unique_id_l,
    df_l.source_dataset as source_dataset_l,
    df_r.unique_id as unique_id_r,
    df_r.source_dataset as source_dataset_r,
    1.0 as clerical_match_score

from df_source as df_l
left join df_source as df_r

on df_l.cluster = df_r.cluster

where df_l.unique_id < df_r.unique_id

union all

select
    df_edges.unique_id_l as unique_id_l,
    df_edges.source_dataset_l as source_dataset_l,

    df_edges.unique_id_r as unique_id_r,
    df_edges.source_dataset_r as source_dataset_r,
    0.0 as clerical_match_score
    from df_edges
    where  cluster_l != cluster_r
"""
df_labels = spark.sql(sql)


df_e_with_labels = labels_with_splink_scores(
    df_labels,
    df_edges,
    "unique_id",
    spark,
    source_dataset_colname="source_dataset",
    retain_all_cols=True,
)


df_e_with_labels = df_e_with_labels.repartition(10)
df_e_with_labels = df_e_with_labels.withColumn(
    "commit_hash", f.lit(args["commit_hash"])
)


output_path = output_paths["labels_with_scores_path"]


df_e_with_labels.write.mode("overwrite").parquet(output_path)

custom_log.info(f"outputting scores with labels to to {output_path}")

PARALLELISM = 20
spark.conf.set("spark.sql.shuffle.partitions", PARALLELISM)
spark.conf.set("spark.default.parallelism", PARALLELISM)


df_e_with_labels = spark.read.parquet(output_path)

# Thin out the ROC curve by limiting the number of unique probabilities

# score_colname = _get_score_colname(df_e_with_labels)

# log2_bf_expr = f"log2({score_colname}/(1-{score_colname}))"

# expr = f"""
# case
#     when {score_colname} = 0.0 then -100
#     when {log2_bf_expr} < -20 or {log2_bf_expr} > 20 then round({log2_bf_expr},0)
#     else round({log2_bf_expr},1)
# end

# """
# df = df.withColumn("rounded", f.expr("round(d/5, 1)*5"))

df_e_with_labels = df_e_with_labels.withColumn(
    "bf_temp", f.expr("round(df_e__tf_adjusted_match_weight/2, 1)*2")
)
df_e_with_labels = df_e_with_labels.withColumn(
    "tf_adjusted_match_prob", f.expr("power(2, bf_temp)/(1+power(2,bf_temp))")
)
df_e_with_labels = df_e_with_labels.drop("bf_temp")

charts_path_roc = output_paths["charts_roc_path"]

chart = roc_chart(df_e_with_labels, spark)
chart.save("roc_chart.html")
write_local_file_to_s3(
    "roc_chart.html",
    charts_path_roc,
    overwrite=True,
)

# chart = precision_recall_chart(df_e_with_labels, spark)

# charts_path_pr = output_paths["charts_precision_recall_path"]

# chart.save("pr_chart.html")
# write_local_file_to_s3(
#     "pr_chart.html",
#     charts_path_pr,
#     overwrite=True,
# )
