import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


import pyspark.sql.functions as f


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
paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
    labelling_exercise="qa_2021",
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")


df_edges = spark.read.parquet(paths["edges_path"])
df_edges.createOrReplaceTempView("df_edges")


df_nodes = spark.read.parquet(paths["source_nodes_path"])
df_nodes.createOrReplaceTempView("df_nodes")

df_clusters = spark.read.parquet(paths["clusters_path"])
df_clusters.createOrReplaceTempView("df_clusters")


sql = """
select
    n.unique_id,
    n.cluster as real_cluster,
    c.cluster_medium

from df_clusters as c

left join df_nodes as n
    on n.unique_id = c.unique_id

order by real_cluster
"""
nodes_with_cluster_info = spark.sql(sql)
nodes_with_cluster_info.createOrReplaceTempView("nodes_with_cluster_info")


sql = """
select count(*) as real_cluster_size, count(*)*(count(*)-1)/2 as num_edges_in_real_cluster, real_cluster
from nodes_with_cluster_info
group by real_cluster
"""
real_cluster_size = spark.sql(sql)
real_cluster_size.createOrReplaceTempView("real_cluster_size")


sql = """
with grouped as (
select
    count(*) as count_nodes_in_est_clust,
    cluster_medium, real_cluster
from nodes_with_cluster_info
group by cluster_medium, real_cluster)

select
    grouped.*,
    real_cluster_size.real_cluster_size,
    real_cluster_size.num_edges_in_real_cluster
from grouped
left join real_cluster_size
on grouped.real_cluster = real_cluster_size.real_cluster


"""
grouped_nodes_with_cluster_info = spark.sql(sql)
grouped_nodes_with_cluster_info.createOrReplaceTempView(
    "grouped_nodes_with_cluster_info"
)

sql = """
select count(real_cluster) as num_real_clusters_in_est_cluster,
    collect_list(real_cluster) as real_cluster_list,
    collect_list(count_nodes_in_est_clust) as num_nodes_from_each_real_cluster,
    collect_list(real_cluster_size) as total_num_nodes_in_each_real_cluster,
    collect_list(num_edges_in_real_cluster) as total_num_edges_in_each_real_cluster,
    sum(count_nodes_in_est_clust) as num_nodes_in_est_cluster,


    case
    when size(collect_list(count_nodes_in_est_clust)) = 1L
        then 0
    else
        aggregate(collect_list(count_nodes_in_est_clust), 1L, (acc,x) -> acc * x)
    end as num_false_positive_edges,


    cluster_medium
from grouped_nodes_with_cluster_info
group by cluster_medium


"""
num_real_clusters_in_est_cluster = spark.sql(sql)
num_real_clusters_in_est_cluster.createOrReplaceTempView(
    "num_real_clusters_in_est_cluster"
)

sql = """
    select *,

          case
            when size(real_cluster_list) = 1
                and num_nodes_in_est_cluster= total_num_nodes_in_each_real_cluster[0]

            then true
            else false
            end as estimated_real_cluster_are_identical,

         size(real_cluster_list) >1 as estimated_cluster_is_too_large,

         case
            when size(real_cluster_list) = 1
                and num_nodes_in_est_cluster < total_num_nodes_in_each_real_cluster[0]
            then true
            else false
            end as estimated_cluster_is_too_small

    from num_real_clusters_in_est_cluster

"""
complete_cluster_stats = spark.sql(sql)
complete_cluster_stats.createOrReplaceTempView("complete_cluster_stats")

# now get estimated cluster size
sql = """
select
    unique_id_l,
    unique_id_r,
    cluster_r as real_cluster_r,
    cluster_l as real_cluster_l,
    df_c_1.cluster_medium as cluster_medium
from
    df_edges

left join
    df_clusters as df_c_1
    on df_edges.unique_id_l = df_c_1.unique_id

left join
    df_clusters as df_c_2
    on df_edges.unique_id_r = df_c_2.unique_id

where
    df_c_1.cluster_medium = df_c_2.cluster_medium
"""

edges_with_real_est_clusters = spark.sql(sql)
edges_with_real_est_clusters.createOrReplaceTempView("edges_with_real_est_clusters")

sql = """
select
    count(*) as num_edges_in_est_cluster,
    sum(cast(real_cluster_r != real_cluster_l as int)) as num_false_positive_edges_in_est_cluster,
    cluster_medium
from edges_with_real_est_clusters
group by cluster_medium
"""
num_edges_in_est_cluster = spark.sql(sql)
num_edges_in_est_cluster.createOrReplaceTempView("num_edges_in_est_cluster")


sql = """
select c.*,  e.num_edges_in_est_cluster, e.num_false_positive_edges_in_est_cluster
from complete_cluster_stats as c
left join
num_edges_in_est_cluster as e

on c.cluster_medium = e.cluster_medium
"""

cluster_truth_statistics = spark.sql(sql)

# Finally join to cluster graph statistics
path_root = paths["graph_analytics_path"]
ga_path = os.path.join(path_root, "all_cluster_metrics")

df_ga = spark.read.parquet(ga_path)
df_ga.createOrReplaceTempView("df_ga")


cluster_truth_statistics.createOrReplaceTempView("cluster_truth_statistics")

sql = """
select ct.*, ga.*
from cluster_truth_statistics as ct
left join df_ga as ga
on ct.cluster_medium = ga.cluster_id

"""

final = spark.sql(sql)
final = final.drop("cluster_id")

cols = list(final.columns)
cols.insert(0, cols.pop(cols.index("cluster_medium")))
final = final.select(cols)
final = final.repartition(1)


output_path = paths["cluster_truth_path"]
final.write.mode("overwrite").parquet(output_path)
