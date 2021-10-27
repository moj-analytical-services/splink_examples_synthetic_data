#!/bin/bash
set -e
set -o pipefail


# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/00_estimate_u
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/01_block_postcode
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/02_block_name
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/03_combine_blocks
# python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons
# python jobrunner.py --job_path cluster_generation/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/01_cluster
python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons

