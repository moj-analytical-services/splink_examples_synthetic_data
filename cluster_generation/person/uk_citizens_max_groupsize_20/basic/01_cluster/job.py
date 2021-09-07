import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from graphframes import GraphFrame

import pyspark.sql.functions as f

from constants import get_paths_from_job_path

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


# # Source nodes is the original list of people - no guarantee the edges will include every person.
df_source_nodes = spark.read.parquet(paths["source_nodes_path"])

# Only need the list of IDs for the cluster output, will not pull in any details
df_source_nodes = df_source_nodes.select(["unique_id", "source_dataset"])
df_source_nodes = df_source_nodes.withColumnRenamed("unique_id", "id")

df_source_nodes = df_source_nodes.withColumn("commit_hash", f.lit(args["commit_hash"]))

df_edges = spark.read.parquet(paths["edges_path"])

df_edges.createOrReplaceTempView("df_edges")
df_source_nodes.createOrReplaceTempView("df_source_nodes")


sql = """
select
unique_id_l as src,
unique_id_r as dst,
tf_adjusted_match_prob
from df_edges
where tf_adjusted_match_prob > {threshold}
"""

df_edges_high = spark.sql(sql.format(threshold=0.999))
df_edges_medium = spark.sql(sql.format(threshold=0.70))
df_edges_low = spark.sql(sql.format(threshold=0.5))


g_high = GraphFrame(df_source_nodes, df_edges_high)
cc_high = g_high.connectedComponents()
cc_high.createOrReplaceTempView("cc_high")

g_medium = GraphFrame(df_source_nodes, df_edges_medium)
cc_medium = g_medium.connectedComponents()
cc_medium.createOrReplaceTempView("cc_medium")

g_low = GraphFrame(df_source_nodes, df_edges_low)
cc_low = g_low.connectedComponents()
cc_low.createOrReplaceTempView("cc_low")


sql = """
select
    sn.id as unique_id,
    sn.source_dataset,
    sn.commit_hash,
    cch.component as cluster_high,
    ccm.component as cluster_medium,
    ccl.component as cluster_low
from df_source_nodes as sn
left join cc_high as cch
on sn.id = cch.id
left join cc_medium as ccm
on sn.id = ccm.id
left join cc_low as ccl
on sn.id = ccl.id
"""
results = spark.sql(sql)

results = results.repartition(50)
results.persist()
results.write.mode("overwrite").parquet(paths["clusters_path"])
results.createOrReplaceTempView("results")

sql = """
select {cluster_type}, count(*) as count_in_cluster, '{cluster_type}' as cluster_type
from results
group by {cluster_type}
order by count(*) desc
limit 5
"""

df = spark.sql(sql.format(cluster_type="cluster_low"))
custom_log.info(df._jdf.showString(5, 20, False))

df = spark.sql(sql.format(cluster_type="cluster_medium"))
custom_log.info(df._jdf.showString(5, 20, False))

df = spark.sql(sql.format(cluster_type="cluster_high"))
custom_log.info(df._jdf.showString(5, 20, False))
