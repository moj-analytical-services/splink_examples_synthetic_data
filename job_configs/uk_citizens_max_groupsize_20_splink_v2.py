import os

try:
    ROLE = os.environ["GLUE_ROLE"]
except KeyError:
    raise ValueError("You must provide a role name")

uk_citizens_max_groupsize_20_splink_v2 = {
    "model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/00_estimate_u": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/01_block_postcode": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/02_block_name": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "model_training/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/03_combine_blocks": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 2,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "edge_generation/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 8,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "cluster_generation/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons/01_cluster": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "list_python_modules": ["graphframes==0.6"],
        "allocated_capacity": 6,
        "version": "v01",
        "remove_python_modules": ["splink"],
        "additional_job_args": {
            "--conf": (
                "spark.jars.packages=graphframes:graphframes:0.6.0-spark2.3-s_2.11"
            )
        },
    },
    "qa/compute_accuracy/person/uk_citizens_max_groupsize_20/splink_2_all_comparisons": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 8,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
}
