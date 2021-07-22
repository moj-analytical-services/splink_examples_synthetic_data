from pyspark.sql import functions as f


def probability_to_normalised_bayes_factor(
    df, prob_colname, out_colname="match_score_norm"
):

    df = df.withColumn(
        "__match_score__", f.expr(f"log2({prob_colname}/(1-{prob_colname}))")
    )

    log2_bf = f"log2({prob_colname}/(1-{prob_colname}))"
    expr = f"""
    case
        when {prob_colname} = 0.0 then -40
        when {prob_colname} = 1.0 then 40
        when {log2_bf} > 40 then 40
        when {log2_bf} < -40 then -40
        else {log2_bf}
        end
    """

    df = df.withColumn("__match_score__", f.expr(expr))

    score_min = df.select(f.min("__match_score__")).collect()[0][0]
    score_max = df.select(f.max("__match_score__")).collect()[0][0]

    expr = f"""
    1 - ((__match_score__ - {score_min})/{score_max - score_min} )
    """
    df = df.withColumn(out_colname, f.expr(expr))

    df = df.drop("match_score")

    return df
