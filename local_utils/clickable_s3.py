from dataengineeringutils3.s3 import s3_path_to_bucket_key
import urllib.parse

import os

from shared_job_resources.glue_py_resources.constants import get_paths_from_job_path

import os

from shared_job_resources.glue_py_resources.constants import (
    BUCKET_MAIN,
    BUCKET_MAIN_ROOT,
)


def _convert_to_trial_run_path(path):

    find = BUCKET_MAIN_ROOT
    replace = os.path.join("s3://", BUCKET_MAIN, "trial_runs")
    path = path.replace(find, replace)

    return path


def s3_path_to_clickable_url(s3_path):
    bucket, key = s3_path_to_bucket_key(s3_path)
    quo_key = urllib.parse.quote(key)
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region=eu-west-1&prefix={quo_key}/&showversions=false"


def output_paths_to_clickable_url(output_paths, keys_to_output):

    # Decide which to print
    retained_output_paths = {}
    if keys_to_output is None:
        retained_output_paths = output_paths
    else:
        for k, v in output_paths.items():
            if k in keys_to_output:
                retained_output_paths[k] = v

    outputs = ["***Clickable_links***"]

    for k, v in retained_output_paths.items():

        output_view_rows = True
        if k in [
            "charts_directory_path",
            "training_charts_path",
            "training_models_path",
        ]:
            output_view_rows = False

        if "trial_runs/" in v:
            trial_run_path = v
            main_path = v.replace("trial_runs/", "")
        else:
            main_path = v
            trial_run_path = _convert_to_trial_run_path(v)

        outputs.append(f"Links for {k}:")
        outputs.append("   Files:")
        outputs.append(f"       {s3_path_to_clickable_url(main_path)}")
        if output_view_rows:
            outputs.append(
                f"       avd python local_utils/view_rows.py --s3_path {main_path}"
            )
        outputs.append("   Trial run files:")
        outputs.append(f"       {s3_path_to_clickable_url(trial_run_path)}")
        if output_view_rows:
            outputs.append(
                f"       avd python local_utils/view_rows.py --s3_path {trial_run_path}"
            )
        outputs.append("")
        outputs.append("")

    return "\n".join(outputs)


def clickable_s3_links(job_arguments, logger):
    ja = job_arguments

    job_path = ja["--job_path"]

    keys_to_output = None
    if job_path.startswith("node_generation/source_nodes/"):
        keys_to_output = ["source_nodes_path"]

    if job_path.startswith("node_generation/standardised_nodes/"):
        keys_to_output = [
            "source_nodes_path",
            "standardised_nodes_path",
            "other_charts_path",
        ]

    if job_path.startswith("edge_generation/"):
        keys_to_output = [
            "standardised_nodes_path",
            "edges_path",
            "training_charts_path",
        ]

    if job_path.startswith("cluster_generation/"):
        keys_to_output = ["edges_path", "clusters_path"]

    if job_path.startswith("qa/"):
        keys_to_output = ["edges_path"]

    if keys_to_output:
        output_paths = get_paths_from_job_path(
            ja["--job_path"], ja["--snapshot_date"], ja["--version"]
        )
        message = output_paths_to_clickable_url(output_paths, keys_to_output)
        logger.info(message)
