from dataengineeringutils3.s3 import write_local_file_to_s3
import functools


def persist_model_charts(model, output_paths, custom_log, final=False):
    it_num = model.iteration

    model.save_model_to_json_file("saved_model.json", overwrite=True)
    model_path = output_paths["training_models_path"]
    if final:
        s3_path = model_path + "/saved_model_final.json"
    else:
        s3_path = model_path + f"/saved_model_iteration_{it_num}.json"
    write_local_file_to_s3(
        "saved_model.json",
        s3_path,
        overwrite=True,
    )
    custom_log.info(f"model writen to: {s3_path}")

    model.all_charts_write_html_file(filename="sparklink_charts.html", overwrite=True)

    charts_path = output_paths["training_charts_path"]
    if final:
        s3_path = charts_path + "/sparklink_charts_final.html"
    else:
        s3_path = charts_path + f"/sparklink_charts_iteration_{it_num}.html"
    write_local_file_to_s3(
        "sparklink_charts.html",
        s3_path,
        overwrite=True,
    )
    custom_log.info(f"chart writen to: {s3_path}")


def curried_persist_model_charts(output_paths, custom_log):
    return functools.partial(
        persist_model_charts, output_paths=output_paths, custom_log=custom_log
    )


# Lineage breaking functions
def blocked_comparisons_to_s3(df, spark, output_paths, custom_log, parallelism):
    df = df.repartition(parallelism)
    df.write.mode("overwrite").parquet(output_paths["blocked_tempfiles_path"])
    custom_log.info(
        f"blocked tempfiles written to: {output_paths['blocked_tempfiles_path']}"
    )
    df_new = spark.read.parquet(output_paths["blocked_tempfiles_path"])
    return df_new


def scored_comparisons_to_s3(df, spark, output_paths, custom_log):
    df.write.mode("overwrite").parquet(output_paths["scored_tempfiles_path"])
    custom_log.info(
        f"scored tempfiles written to: {output_paths['scored_tempfiles_path']}"
    )
    df_new = spark.read.parquet(output_paths["scored_tempfiles_path"])
    return df_new


def curried_blocked_comparisons_to_s3(output_paths, custom_log, parallelism):
    return functools.partial(
        blocked_comparisons_to_s3,
        output_paths=output_paths,
        custom_log=custom_log,
        parallelism=parallelism,
    )


def curried_scored_comparisons_to_s3(output_paths, custom_log):
    return functools.partial(
        scored_comparisons_to_s3, output_paths=output_paths, custom_log=custom_log
    )
