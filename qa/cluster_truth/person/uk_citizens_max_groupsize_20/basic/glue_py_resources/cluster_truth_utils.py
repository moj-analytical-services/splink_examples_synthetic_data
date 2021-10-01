from pyspark.sql.functions import expr
from pyspark.sql import DataFrame


def _get_ground_truth_cluster_size_lookup(nodes_with_est_and_gt_cluster):

    aggregates = [
        expr("count(*) as ground_truth_cluster_size"),
        expr("count(*)*(count(*)-1)/2 as num_edges_in_ground_truth_cluster"),
    ]
    df = nodes_with_est_and_gt_cluster.groupBy("ground_truth_cluster").agg(*aggregates)
    return df


def ground_truth_vs_est_cluster_nodecounts(
    nodes_with_est_and_gt_cluster, ground_truth_cluster_size, cluster_colname
):
    """
    Outputs table like:
    |cluster_medium|ground_truth_cluster|count_nodes_in_est_clust|ground_truth_cluster_size|num_edges_in_ground_truth_cluster
    """
    spark = nodes_with_est_and_gt_cluster.sql_ctx.sparkSession
    nodes_with_est_and_gt_cluster.createOrReplaceTempView(
        "nodes_with_est_and_gt_cluster"
    )
    ground_truth_cluster_size.createOrReplaceTempView("ground_truth_cluster_size")

    sql = f"""
    with grouped as (
    select
        {cluster_colname},
        ground_truth_cluster,
        count(*) as count_nodes_in_est_clust
    from nodes_with_est_and_gt_cluster
    group by {cluster_colname}, ground_truth_cluster)

    select
        grouped.*,
        ground_truth_cluster_size.ground_truth_cluster_size,
        ground_truth_cluster_size.num_edges_in_ground_truth_cluster
    from grouped
    left join ground_truth_cluster_size
    on grouped.ground_truth_cluster = ground_truth_cluster_size.ground_truth_cluster


    """
    return spark.sql(sql)


def _num_ground_truth_clusters_in_est_cluster(
    nodes_with_est_and_gt_cluster, ground_truth_cluster_size, cluster_colname
):
    """Outputs table like:
    |cluster_medium|num_ground_truth_clusters_in_est_cluster|ground_truth_cluster_list
        |num_nodes_from_each_ground_truth_cluster|total_num_nodes_in_each_ground_truth_cluster
            |total_num_edges_in_each_ground_truth_cluster|num_nodes_in_est_cluster|num_false_positive_edges|
    """
    spark = nodes_with_est_and_gt_cluster.sql_ctx.sparkSession
    gt_vs_est_nodecounts = ground_truth_vs_est_cluster_nodecounts(
        nodes_with_est_and_gt_cluster, ground_truth_cluster_size, cluster_colname
    )
    gt_vs_est_nodecounts.createOrReplaceTempView("gt_vs_est_nodecounts")
    sql = f"""
    select
        {cluster_colname},
        count(ground_truth_cluster) as num_ground_truth_clusters_in_est_cluster,
        collect_list(ground_truth_cluster) as ground_truth_cluster_list,
        collect_list(count_nodes_in_est_clust) as num_nodes_from_each_ground_truth_cluster,
        collect_list(ground_truth_cluster_size) as total_num_nodes_in_each_ground_truth_cluster,
        collect_list(num_edges_in_ground_truth_cluster) as total_num_edges_in_each_ground_truth_cluster,
        sum(count_nodes_in_est_clust) as num_nodes_in_est_cluster,

        case
        when size(collect_list(count_nodes_in_est_clust)) = 1L
            then 0
        else
            aggregate(collect_list(count_nodes_in_est_clust), 1L, (acc,x) -> acc * x)
        end as num_false_positive_edges

    from gt_vs_est_nodecounts
    group by {cluster_colname}
    """
    return spark.sql(sql)


def _flags_for_fp_fn_correct(df_gt_vs_est):
    """
    Adds the following columns to the output of num_ground_truth_clusters_in_est_cluster:
        est_cluster_is_correct
        est_cluster_contains_false_positives
        est_cluster_contains_false_negatives
    """

    est_cluster_is_correct = """
    CASE
        WHEN size(ground_truth_cluster_list) = 1
        AND num_nodes_in_est_cluster = total_num_nodes_in_each_ground_truth_cluster[0] THEN TRUE
        ELSE false
    END
    """
    df_gt_vs_est = df_gt_vs_est.withColumn(
        "est_cluster_is_correct", expr(est_cluster_is_correct)
    )

    est_cluster_contains_false_positives = "size(ground_truth_cluster_list) > 1"
    df_gt_vs_est = df_gt_vs_est.withColumn(
        "est_cluster_contains_false_positives",
        expr(est_cluster_contains_false_positives),
    )

    est_cluster_contains_false_negatives = """
    CASE
        WHEN size(ground_truth_cluster_list) = 1
        AND num_nodes_in_est_cluster < total_num_nodes_in_each_ground_truth_cluster[0] THEN TRUE
        ELSE false
    END
    """
    df_gt_vs_est = df_gt_vs_est.withColumn(
        "est_cluster_contains_false_negatives",
        expr(est_cluster_contains_false_negatives),
    )
    return df_gt_vs_est


def label_est_clusters_fp_fn(
    nodes_with_est_and_gt_cluster: DataFrame,
    cluster_colname: str,
    retain_details: bool = False,
):
    """For each estimated cluster, label whether it contains false positives, false negatives
    and whether it is correct.

    Args:
        nodes_with_est_and_gt_cluster (DataFrame): df of nodes containing ground truth cluster and estimated cluster
        cluster_colname (str): name of column containing cluster id
        retain_details (bool): if True, the returned DataFrame contains further details of differences between ground truth and estimated clusters

    Returns:
        DataFrame: df with columns:
            {cluster_colname}
            est_cluster_is_correct
            est_cluster_contains_false_positives
            est_cluster_contains_false_negatives
    """

    ground_truth_cluster_size = _get_ground_truth_cluster_size_lookup(
        nodes_with_est_and_gt_cluster
    )
    gt_vs_est = _num_ground_truth_clusters_in_est_cluster(
        nodes_with_est_and_gt_cluster, ground_truth_cluster_size, cluster_colname
    )
    df = _flags_for_fp_fn_correct(gt_vs_est)

    if not retain_details:
        cols = [
            cluster_colname,
            "est_cluster_is_correct",
            "est_cluster_contains_false_positives",
            "est_cluster_contains_false_negatives",
        ]
        df = df.select(cols)

    return df


def get_num_fp_edges(
    df_edges: DataFrame,
    df_nodes_with_clusters: DataFrame,
    cluster_colname,
    uid_col="unique_id",
    uid_col_l="unique_id_l",
    uid_col_r="unique_id_r",
    gt_cluster_colname="cluster",
):
    """Compute number of false positive edges for each estimated cluster.

    Args:
        df_edges (DataFrame):
        df_nodes_with_clusters (DataFrame): Nodes with estimated clusters
        cluster_colname (str):The name of the estimated cluster column e.g. cluster_medium
        uid_col (str, optional): The name of the unique id column in the nodes dataframe. Alternatively, a SQL expression defining a unique column. Defaults to "unique_id".
        uid_col_l (str, optional): Name of the 'left' column containing unique IDs in the edges table.  Alternatively, a SQL expression defining a unique column. Defaults to "unique_id_l".
        uid_col_r (str, optional): The name of the 'right' column containing unique IDs in the edges table.   Alternatively, a SQL expression defining a unique column. Defaults to "unique_id_r".
        gt_cluster_colname (str, optional): Column name designating ground truth cluster. Defaults to "cluster".
    """
    spark = df_edges.sql_ctx.sparkSession
    df_edges = df_edges.withColumn("___idl__", expr(uid_col_l))
    df_edges = df_edges.withColumn("___idr__", expr(uid_col_r))
    df_nodes_with_clusters = df_nodes_with_clusters.withColumn("___id__", expr(uid_col))

    df_edges.createOrReplaceTempView("df_edges")
    df_nodes_with_clusters.createOrReplaceTempView("df_nodes_with_clusters")

    sql = f"""
    select
        ___idl__,
        ___idr__,
        {gt_cluster_colname}_r as ground_truth_cluster_r,
        {gt_cluster_colname}_l as ground_truth_cluster_l,
        df_c_1.{cluster_colname} as {cluster_colname}
    from
        df_edges

    left join
        df_nodes_with_clusters as df_c_1
        on df_edges.___idl__ = df_c_1.___id__

    left join
        df_nodes_with_clusters as df_c_2
        on df_edges.___idr__ = df_c_2.___id__

    where
        df_c_1.{cluster_colname} = df_c_2.{cluster_colname}
    """

    df_edges_with_gt_vs_est = spark.sql(sql)
    df_edges_with_gt_vs_est.createOrReplaceTempView("df_edges_with_gt_vs_est")

    sql = f"""
    select
        count(*) as num_edges_in_est_cluster,
        sum(cast(ground_truth_cluster_r != ground_truth_cluster_l as int)) as num_false_positive_edges_in_est_cluster,
        {cluster_colname}
    from df_edges_with_gt_vs_est
    group by {cluster_colname}
    """
    return spark.sql(sql)
