import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from dataengineeringutils3.s3 import (
    read_json_from_s3,
    write_json_to_s3,
    write_local_file_to_s3,
)

from constants import get_paths_from_job_path
from splink.model import load_model_from_dict
from splink.combine_models import ModelCombiner, combine_cc_estimates
from splink.settings import Settings

from custom_logger import get_custom_logger

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


custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {args['snapshot_date']}")

custom_log.info(args)

# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"], args["snapshot_date"], args["version"], trial_run=trial_run
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")


pc_dob_path = paths["training_models_path"].replace(
    "03_combine_blocks",
    "01_block_postcode_dob",
)

pc_dob_path = os.path.join(pc_dob_path, "saved_model_final.json")

name_occ_path = paths["training_models_path"].replace(
    "03_combine_blocks",
    "02_name_occupation",
)

name_occ_path = os.path.join(name_occ_path, "saved_model_final.json")


#  postcode date of birth model
pc_dob_json = read_json_from_s3(pc_dob_path)
pc_dob_model = load_model_from_dict(pc_dob_json)

# name occupation model
name_occ_json = read_json_from_s3(name_occ_path)
name_occ_model = load_model_from_dict(name_occ_json)


cc_dob = name_occ_model.current_settings_obj.get_comparison_column("dob")
cc_pc = name_occ_model.current_settings_obj.get_comparison_column(
    "custom_postcode_distance_comparison"
)


# Same thing from the surname/postcode blocking job
cc_sn = pc_dob_model.current_settings_obj.get_comparison_column("surname_std")
cc_fn = pc_dob_model.current_settings_obj.get_comparison_column("forename1_std")
cc_occ = pc_dob_model.current_settings_obj.get_comparison_column("occupation")


# To get out
pc_dob_dict = {
    "name": "postcode_dob",
    "model": pc_dob_model,
    "comparison_columns_for_global_lambda": [cc_pc, cc_dob],
}


name_occ_dict = {
    "name": "name_occ",
    "model": name_occ_model,
    "comparison_columns_for_global_lambda": [cc_sn, cc_fn, cc_occ],
}


mc = ModelCombiner([pc_dob_dict, name_occ_dict])

global_settings_dict = mc.get_combined_settings_dict()

# Now we have global settings, we just need a set of blocking rules to produce potential matches

global_settings_dict["blocking_rules"] = [
    "l.forename1_dm = r.forename1_dm and l.occupation = r.occupation and l.dob_year = r.dob_year",
    "l.postcode = r.postcode and l.surname_dm = r.surname_dm",
    "l.postcode = r.postcode and l.forename1_dm = r.forename1_dm",
    "l.outward_postcode_std = r.outward_postcode_std and l.dob = r.dob",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.forename2_std = r.forename2_std",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.birth_place = r.birth_place",
]


path = os.path.join(paths["training_combined_model_path"], "combined_settings.json")
write_json_to_s3(global_settings_dict, path)

chart = mc.comparison_chart()


chart_name = "model_combiner_comparison_chart.html"
chart.save(chart_name)

charts_dir = paths["charts_directory_path"]
path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, path, overwrite=True)


s = Settings(global_settings_dict)
chart = s.bayes_factor_chart()
chart_name = "combined_bayes_factor.html"
chart.save(chart_name)

charts_dir = paths["charts_directory_path"]
path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, path, overwrite=True)
