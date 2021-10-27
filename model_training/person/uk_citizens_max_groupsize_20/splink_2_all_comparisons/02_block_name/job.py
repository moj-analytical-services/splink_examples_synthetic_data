import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from dataengineeringutils3.s3 import (
    delete_s3_folder_contents,
    read_json_from_s3,
    write_local_file_to_s3,
)
import os
from splink import Splink
from splink.settings import Settings

from clickable_path import log_clickable_chart_params

from constants import get_paths_from_job_path


from lineage import (
    curried_blocked_comparisons_to_s3,
    curried_scored_comparisons_to_s3,
    curried_persist_model_charts,
)
from custom_logger import get_custom_logger

from spark_config import add_udfs

sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()
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
    PARALLELISM = 4
else:
    PARALLELISM = 50

spark.conf.set("spark.sql.shuffle.partitions", PARALLELISM)
spark.conf.set("spark.default.parallelism", PARALLELISM)

add_udfs(spark)

custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {args['snapshot_date']}")

custom_log.info(args)

# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"], args["snapshot_date"], args["version"], trial_run=trial_run
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")


PERSON_STANDARDISED_NODES_PATH = paths["standardised_nodes_path"]
PERSON_STANDARDISED_NODES_PATH = PERSON_STANDARDISED_NODES_PATH.replace(
    "splink_2_all_comparisons", "basic"
)


person_standardised_nodes = spark.read.parquet(PERSON_STANDARDISED_NODES_PATH)


# Use the u and m probabilities estimated from the first job
model_path = paths["training_models_path"].replace("02_block_name", "00_estimate_u")
model_path = os.path.join(model_path, "settings_with_u.json")
custom_log.info(model_path)

# Import settings object used in previous job where we estimated u probabilities
settings_with_m_u_dict = read_json_from_s3(model_path)

if trial_run:
    settings_with_m_u_dict["em_convergence"] = 0.003
    settings_with_m_u_dict["max_iterations"] = 5
else:
    settings_with_m_u_dict["em_convergence"] = 0.001
    settings_with_m_u_dict["max_iterations"] = 100

settings_obj = Settings(settings_with_m_u_dict)

# Add blocking rules, remove corresponding columns
settings_obj.settings_dict["blocking_rules"] = [
    "Dmetaphone(l.forename1_std) = Dmetaphone(r.forename1_std) and"
    " Dmetaphone(l.surname_std) = Dmetaphone(r.surname_std)",
    "DmetaphoneAlt(l.forename1_std) = DmetaphoneAlt(r.forename1_std) and"
    " DmetaphoneAlt(l.surname_std) = DmetaphoneAlt(r.surname_std)",
]

settings_obj.remove_comparison_column("forename1_std")
settings_obj.remove_comparison_column("surname_std")


settings_obj.settings_dict["retain_intermediate_calculation_columns"] = False
settings_obj.settings_dict["retain_matching_columns"] = False

count_for_log = person_standardised_nodes.count()
custom_log.info(f"The count of source nodes is {count_for_log:,.0f}")

persist_params_settings = curried_persist_model_charts(paths, custom_log)
blocked_comparisons_to_s3 = curried_blocked_comparisons_to_s3(
    paths, custom_log, PARALLELISM
)
scored_comparisons_to_s3 = curried_scored_comparisons_to_s3(paths, custom_log)

linker = Splink(
    settings_obj.settings_dict,
    person_standardised_nodes,
    spark,
    # save_state_fn=persist_params_settings,
    break_lineage_blocked_comparisons=blocked_comparisons_to_s3,
)


df_e = linker.get_scored_comparisons()
persist_params_settings(model=linker.model, final=True)

log_clickable_chart_params(paths, linker.model, custom_log)

count_for_log = df_e.count()
custom_log.info(f"The count of df_e is {count_for_log:,.0f}")

custom_log.info(f"deleting {paths['blocked_tempfiles_path']}")
custom_log.info(f"deleting {paths['scored_tempfiles_path']}")

if "temp_files" in paths["blocked_tempfiles_path"]:
    delete_s3_folder_contents(paths["blocked_tempfiles_path"])

if "temp_files" in paths["scored_tempfiles_path"]:
    delete_s3_folder_contents(paths["scored_tempfiles_path"])

# Persist final version of charts into 'charts' folder
chart_name = "final_splink_charts_01_name_block.html"
linker.model.all_charts_write_html_file(filename=chart_name, overwrite=True)
charts_dir = paths["charts_directory_path"]
path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, path, overwrite=True)
