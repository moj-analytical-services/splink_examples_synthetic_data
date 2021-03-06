from pyspark.sql.dataframe import DataFrame
from graphframes import GraphFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr


def clusters_at_thresholds(
    df_nodes: DataFrame,
    df_edges: DataFrame,
    threshold_values: list,
    cluster_colnames: list,
    spark: SparkSession,
    uid_df_nodes_col="unique_id",
    uid_col_l="unique_id_l",
    uid_col_r="unique_id_r",
    score_colname="match_probability",
    join_node_details=True,
):
    """Generated a table of clusters at one or more threshold_values
    from a table of scored edges (scored pairwise comparisons)

    Args:
        df_nodes (DataFrame): Dataframe of nodes (original records from which pairwise comparisons are derived)
        df_edges (DataFrame): Dataframe of edges (pairwise record comparisons with scores)
        threshold_values (list): A list of threshold values above which edges will be considered matches.  e.g. [0.5, 0.95, 0.99]
        cluster_colnames (list): A list of column names used for the clusters, e.g. ["cluster_low", "cluster_medium", "cluster_high"]
        spark (SparkSession): The pyspark.sql.session.SparkSession
        uid_df_nodes_col (str, optional): The name of the unique id column in the df_nodes table. Alternatively, a SQL expression defining a unique column. Defaults to "unique_id".  Used only if
            df_nodes is not None.
        uid_col_l (str, optional): Name of the 'left' column containing unique IDs in the edges table.  Alternatively, a SQL expression defining a unique column. Defaults to "unique_id_l".
        uid_col_r (str, optional): The name of the 'right' column containing unique IDs in the edges table.   Alternatively, a SQL expression defining a unique column. Defaults to "unique_id_r".
        score_colname (str, optional): The name of the score column to which the thresholds apply. Defaults to "match_probability".
        join_node_details (bool, optional):  Defaults to True.  If true, return the clusters against the full nodes table.  If false, return just IDs and clusters.

    """
    df_nodes.createOrReplaceTempView("df_nodes")
    df_edges.createOrReplaceTempView("df_edges")
    # note UNION as opposed to UNION ALL has the effect of deduping IDs

    sql = f"""
    select {uid_df_nodes_col} as id
    from df_nodes
    """
    df_nodes_id = spark.sql(sql)

    cc_thresholds = []
    for v in threshold_values:
        sql = f"""
        select
            {uid_col_l} as src,
            {uid_col_r} as dst
        from df_edges
        where {score_colname} > {v}
        """
        edges_above_thres = spark.sql(sql)
        g = GraphFrame(df_nodes_id, edges_above_thres)
        cc = g.connectedComponents()
        cc_thresholds.append(cc)

    for cc, cc_col_name in zip(cc_thresholds, cluster_colnames):
        df_nodes_id = df_nodes_id.join(cc, on=["id"], how="left")
        df_nodes_id = df_nodes_id.withColumnRenamed("component", cc_col_name)

    if join_node_details:
        df_nodes_id.createOrReplaceTempView("df_nodes_id")

        df_nodes = df_nodes.withColumn("___id__", expr(uid_df_nodes_col))
        df_nodes.createOrReplaceTempView("df_nodes")

        names = [f"df_nodes_id.{c}" for c in cluster_colnames]
        cluster_sel = ", ".join(cluster_colnames)

        sql = f"""
        select {cluster_sel}, df_nodes.*
        from df_nodes
        left join df_nodes_id
        on df_nodes_id.id = df_nodes.___id__

        """
        df_nodes = spark.sql(sql)
        df_nodes = df_nodes.drop("___id__")
    else:
        df_nodes = df_nodes_id

    return df_nodes


def cluster_counts(
    df_clustered: DataFrame, cluster_colnames: list, suffix: str = "_count"
):
    """Taking the final clustered results of record linkage at multiple thresholds,
    count the number of nodes in each cluster for each thresholds.

    These counts show how the size of clusters change when the threshold changes,
    and are used as the basis for cluster stability statistics.


    Args:
        df_clustered (DataFrame): Dataframe of nodes with clusters
        cluster_colnames (str): A list of column names used for the clusters, e.g. ["cluster_low", "cluster_medium", "cluster_high"]
        suffix (str, optional): Suffix applied to cluster_colnames for the new columns. Defaults to "_count".

    Returns:
        DataFrame: Dataframe with cluster counts, one for each distinct combination of cluster_id in
            the cluster_colnames columns.
    """
    df_clustered = df_clustered.select(cluster_colnames)
    for cluster_name in cluster_colnames:
        c_count = (
            df_clustered.groupBy(cluster_name)
            .count()
            .withColumnRenamed("count", f"{cluster_name}{suffix}")
        )
        df_clustered = df_clustered.join(c_count, on=cluster_name, how="inner")

    df_clustered = df_clustered.dropDuplicates(cluster_colnames)

    cols_with_suffix = [f"{c}{suffix}" for c in cluster_colnames]
    all_cols = cluster_colnames + cols_with_suffix
    df_clustered = df_clustered.select(all_cols)

    return df_clustered


def cluster_stability_statistics_raw(
    df_cluster_counts, cluster_colnames, suffix="_count"
):
    """Taking the table of cluster counts, compute cluster metrics
    against all thresholds.

    Outputs will then need to be aggregated within a given threshold
    for the final stability statistics.

    Args:
        df_cluster_counts (DataFrame): [description]
        cluster_colnames (list): A list of column names used for the clusters, e.g. ["cluster_low", "cluster_medium", "cluster_high"]
        suffix (str, optional): The suffix on colnames indicating their counts. Defaults to "_count".

    Returns:
        DataFrame: Dataframe with stability metrics against all thresholds
    """

    count_names = [f"{c}{suffix}" for c in cluster_colnames]
    names_expr = ", ".join(count_names)
    cc_df_m = df_cluster_counts.withColumn(
        "cluster_sizes_list", expr(f"array({names_expr})")
    )
    cc_df_m = cc_df_m.withColumn(
        "num_different_cluster_sizes", expr("size(array_distinct(cluster_sizes_list))")
    )
    cc_df_m = cc_df_m.withColumn(
        "min_max_different_abs",
        expr("array_max(cluster_sizes_list) - array_min(cluster_sizes_list)"),
    )
    cc_df_m = cc_df_m.withColumn(
        "min_max_different_rel",
        expr("array_max(cluster_sizes_list)/array_min(cluster_sizes_list)"),
    )
    sum_expr = "aggregate(cluster_sizes_list, 0, (acc, x) -> acc + cast(x as int), acc -> acc/size(cluster_sizes_list))"
    cc_df_m = cc_df_m.withColumn("cluster_mean_", expr(f"{sum_expr}"))

    diff_expr = "aggregate(cluster_sizes_list, 0.0D, (acc, x) -> acc + pow((cast(x as double) - cluster_mean_),2), acc -> pow(acc/size(cluster_sizes_list), 0.5))"
    cc_df_m = cc_df_m.withColumn("cluster_stdev", expr(f"{diff_expr}"))
    cc_df_m = cc_df_m.drop("cluster_mean_", "cluster_sizes_list")
    cc_df_m = cc_df_m.drop(*count_names)

    return cc_df_m


def cluster_stability_statistics_specific_threshold(
    df_cluster_stability_statistics_raw: DataFrame,
    cluster_colname: str,
):
    """Taking cluster_stability_statistics_raw, get
    stability statistcs for a specific threshold
    ready to join onto a summary table of clusters

    Args:
        df_cluster_stability_statistics_raw (DataFrame): Output of cluster_stability_statistics_raw()
        cluster_colname (str): The name of the cluster column to use, e.g. cluster_medium
        suffix (str, optional): [description]. Defaults to "_count".
    """

    aggs = {
        "num_different_cluster_sizes": "avg",
        "min_max_different_abs": "avg",
        "min_max_different_rel": "avg",
        "cluster_stdev": "avg",
    }

    df = df_cluster_stability_statistics_raw.groupBy(cluster_colname).agg(aggs)
    for col in df.columns:
        if "avg(" in col:
            df = df.withColumnRenamed(col, col.replace("avg(", "avg_").replace(")", ""))
    return df


def format_edges_and_clusters_df_for_use_in_splink_graph(
    df_edges: DataFrame,
    df_clusters: DataFrame,
    cluster_colname: str,
    match_weight_colname: str,
    thres_filter: str,
    spark: SparkSession,
    uid_df_clusters_col: str = "unique_id",
    uid_col_l: str = "unique_id_l",
    uid_col_r: str = "unique_id_r",
):
    """Create a dataframe formatted correctly to be processed with
    `splink_graph`.

    Splink graph needs a dataframe of edges,
    with each edge being labelled with the cluster to which it belongs

    For each edge (pairwise comparison), join on the clusters
    corresopnding to two nodes (the nodes on each side of the comparison)

    We only want to keep edges where both nodes are in the same cluster.

    Args:
        df_edges (DataFrame):
        df_clusters (DataFrame):
        cluster_colname (str): The name of the estimated cluster column e.g. cluster_medium
        match_weight_colname (str): Then name of the column containing the Splink match weight.  Will be used for the networkx weight/distance.
        thres_filter (str): A SQL expression representing the filter to apply to the edges.
            This filter should be the same filter applied when creating the clusters e.g. match_probability > 0.9
        spark (SparkSession): The pyspark.sql.session.SparkSession
        uid_df_clusters_col (str, optional): The name of the unique id column in the df_nodes table. Alternatively, a SQL expression defining a unique column. Defaults to "unique_id".  Used only if
            df_nodes is not None.
        uid_col_l (str, optional): Name of the 'left' column containing unique IDs in the edges table.  Alternatively, a SQL expression defining a unique column. Defaults to "unique_id_l".
        uid_col_r (str, optional): The name of the 'right' column containing unique IDs in the edges table.   Alternatively, a SQL expression defining a unique column. Defaults to "unique_id_r".

    Returns: A dataset in src, dst, weight, cluster_id format
    """

    df_edges = df_edges.withColumn("___idl__", expr(uid_col_l))
    df_edges = df_edges.withColumn("___idr__", expr(uid_col_r))
    df_edges = df_edges.filter(thres_filter)
    df_clusters = df_clusters.withColumn("___id__", expr(uid_df_clusters_col))

    df_edges.registerTempTable("df_edges")
    df_clusters.registerTempTable("df_clusters")

    sql = f"""
    select
        ___idl__ as src,
        ___idr__ as dst,
        {match_weight_colname} as weight,
        df_c_1.{cluster_colname} as cluster_id
    from
        df_edges

    left join
        df_clusters as df_c_1
        on df_edges.___idl__ = df_c_1.___id__

    left join
        df_clusters as df_c_2
        on df_edges.___idr__ = df_c_2.___id__

    where
        df_c_1.{cluster_colname} = df_c_2.{cluster_colname}

    """
    return spark.sql(sql)


def get_all_cluster_metrics(df_splink_graph):
    from splink_graph.cluster_metrics import (
        cluster_basic_stats,
        cluster_main_stats,
        cluster_eb_modularity,
        cluster_avg_edge_betweenness,
        number_of_bridges,
        cluster_connectivity_stats,
        cluster_assortativity,
    )

    cluster_basic_stats_df = cluster_basic_stats(df_splink_graph)

    cluster_main_stats_df = cluster_main_stats(df_splink_graph)

    # cluster_conn_stats_df = cluster_connectivity_stats(df_splink_graph)

    # cluster_num_bridges = number_of_bridges(df_splink_graph, distance_colname="weight")

    # cluster_assort_df = cluster_assortativity(df_splink_graph)

    # cluster_avg_eb_df = cluster_avg_edge_betweenness(
    #     df_splink_graph, distance_colname="weight"
    # )

    cluster_eb_modularity_df = cluster_eb_modularity(
        df_splink_graph, distance_colname="weight"
    )

    cluster_all_stats_df = (
        cluster_basic_stats_df.join(
            cluster_main_stats_df, on=["cluster_id"], how="left"
        ).join(cluster_eb_modularity_df, on=["cluster_id"], how="left")
        # .join(cluster_conn_stats_df, on=["cluster_id"], how="left")
        # .join(cluster_num_bridges, on=["cluster_id"], how="left")
        # .join(cluster_assort_df, on=["cluster_id"], how="left")
        # .join(cluster_avg_eb_df, on=["cluster_id"], how="left")
    )

    return cluster_all_stats_df
