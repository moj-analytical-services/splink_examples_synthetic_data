import os

try:
    ROLE = os.environ["GLUE_ROLE"]
except KeyError:
    raise ValueError("You must provide a role name")

uk_citizens_max_groupsize_20 = {
    "node_generation/standardised_nodes/uk_citizens_max_groupsize_20/basic/person/01_generate_standardised_nodes": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 3,
        "version": "v01",
    },
    "node_generation/standardised_nodes/uk_citizens_max_groupsize_20/basic/person/02_profile_standardised_nodes": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
    },
    "model_training/person/uk_citizens_max_groupsize_20/basic/00_estimate_u": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
    },
    "model_training/person/uk_citizens_max_groupsize_20/basic/01_block_postcode_dob": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
    },
    "model_training/person/uk_citizens_max_groupsize_20/basic/02_name_occupation": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
    },
    "model_training/person/uk_citizens_max_groupsize_20/basic/03_combine_blocks": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 2,
        "version": "v01",
    },
    "edge_generation/person/uk_citizens_max_groupsize_20/basic": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
    },
    "cluster_generation/person/uk_citizens_max_groupsize_20/basic/01_cluster": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "list_python_modules": ["graphframes==0.6"],
        "allocated_capacity": 6,
        "version": "v01",
        "additional_job_args": {
            "--conf": "spark.jars.packages=graphframes:graphframes:0.6.0-spark2.3-s_2.11"
        },
    },
    "qa/compute_accuracy/person/uk_citizens_max_groupsize_20/basic": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 8,
        "version": "v01",
    },
}
