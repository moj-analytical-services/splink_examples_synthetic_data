from dataengineeringutils3.s3 import s3_path_to_bucket_key
import urllib.parse

import os


def s3_path_to_clickable_url(s3_path):
    bucket, key = s3_path_to_bucket_key(s3_path)
    quo_key = urllib.parse.quote(key)
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region=eu-west-2&prefix={quo_key}/&showversions=false"


def log_clickable_chart_params(output_paths, params, custom_log):

    charts_path_s3 = output_paths["training_charts_path"]
    params_path_s3 = output_paths["training_models_path"]

    charts_click = s3_path_to_clickable_url(charts_path_s3)
    params_click = s3_path_to_clickable_url(params_path_s3)

    it_num = params.iteration

    charts_final_file = os.path.join(
        charts_path_s3, f"sparklink_charts_iteration_{it_num}.html"
    )
    params_final_file = os.path.join(
        params_path_s3, f"saved_params_iteration_{it_num}.html"
    )

    outputs = []

    outputs.append("**Charts and params**")
    outputs.append("Charts:")
    outputs.append(f"    {charts_click}")
    outputs.append(f"    avd aws s3 cp {charts_final_file} .")
    outputs.append(f"    open sparklink_charts_iteration_{it_num}.html")

    outputs.append("Params::")
    outputs.append(f"    {params_click}")
    outputs.append(f"    avd aws s3 cp {params_final_file} .")
    outputs.append("")

    custom_log.info("\n".join(outputs))
