#!/bin/bash
set -e
set -o pipefail

#python jobrunner.py --job_path node_generation/standardised_nodes/uk_citizens_max_groupsize_20/basic/person/01_generate_standardised_nodes
#python jobrunner.py --job_path node_generation/standardised_nodes/uk_citizens_max_groupsize_20/basic/person/02_profile_standardised_nodes
#python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/00_estimate_u
#python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/01_block_postcode_dob
#python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/02_name_occupation
python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/03_combine_blocks
python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/basic
python jobrunner.py --job_path cluster_generation/person/uk_citizens_max_groupsize_20/basic/01_cluster
python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/basic
python jobrunner.py --job_path qa/cluster_truth/person/uk_citizens_max_groupsize_20/basic
python jobrunner.py --job_path graph_analytics/person/uk_citizens_max_groupsize_20/basic/01_graph_analytics

# python jobrunner.py --job_path node_generation/standardised_nodes/uk_citizens_max_groupsize_20/basic/person/01_generate_standardised_nodes
# python jobrunner.py --job_path node_generation/standardised_nodes/uk_citizens_max_groupsize_20/basic/person/02_profile_standardised_nodes
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/00_estimate_u
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/01_block_postcode_dob
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/02_name_occupation
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/basic/03_combine_blocks
# python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/basic
# python jobrunner.py --job_path cluster_generation/person/uk_citizens_max_groupsize_20/basic/01_cluster
# python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/basic
# python jobrunner.py --job_path graph_analytics/person/uk_citizens_max_groupsize_20/basic/01_graph_analytics