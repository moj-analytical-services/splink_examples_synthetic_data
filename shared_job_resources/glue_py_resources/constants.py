import os
import re
import datetime
import inspect
import json


BUCKET_MAIN = "alpha-splink-synthetic-data"

BUCKET_MAIN_ROOT = os.path.join("s3://", BUCKET_MAIN)


CLUSTER_ROOT = "clusters"
GRAPH_ANALYTICS_ROOT = "graph_analytics"
EDGES_ROOT = "edges"
TRAINING_ROOT = "model_training"

CHARTS_ROOT = "charts"
CONFIG_ROOT = "config"

TEMPFILES_ROOT = "temp_files"

NODES_ROOT = "nodes"
PRODUCTS_ROOT = "products"
QA_ROOT = "qa"

SOURCE_NODES_ROOT = os.path.join(NODES_ROOT, "source_nodes")
STANDARDISED_NODES_ROOT = os.path.join(NODES_ROOT, "standardised_nodes")


ENTITIES = ["person", "journey"]

# Do not change the order of items in this list
# (it's fine to add new items between existing items, just don't change the order of existing ones)
DATASETS = {
    "uk_citizens_max_groupsize_20": {
        "name": "uk_citizens_max_groupsize_20",
        "abbr": "uk_20",
    },
}


def _get_trial_run_path_string(trial_run: bool):
    if trial_run:
        tr_path = "trial_runs"
    else:
        tr_path = ""
    return tr_path


def _validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")


def _validate_version(version):
    if re.match(r"^v\d{2}$", version):
        return
    else:
        raise ValueError(
            f"Incorrect version format.  You gave {version} should match ^v\d{2}$ e.g. v02"
        )


def _validate_job_name(job_name):
    if re.match(r"^[a-z0-9_]+$", job_name):
        return
    else:
        raise ValueError(
            "job_name must be lowercase alphanumeric letters or underscores only i.e a-z0-9_ "
        )


def _validate_dataset(dataset):
    assert dataset in DATASETS.keys(), f"{dataset} not in {DATASETS.keys()}"


def _validate_dataset_or_datasets(dataset_or_datasets):
    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    for dataset in datasets:
        _validate_dataset(dataset)


def _validate_entity(entity):
    assert entity in ENTITIES, f"{entity} not in {ENTITIES}"


def _validate_all(**kwargs):

    validators = {
        "entity": _validate_entity,
        "dataset_or_datasets": _validate_dataset_or_datasets,
        "dataset": _validate_dataset,
        "snapshot_date": _validate_date,
        "version": _validate_version,
        "job_name": _validate_job_name,
    }

    for arg, fn in validators.items():
        if arg in kwargs:
            fn(kwargs[arg])


def _standardise_dataset_or_datasets_to_arr(dataset_or_datasets):
    if type(dataset_or_datasets) == str:
        return [dataset_or_datasets]
    elif type(dataset_or_datasets) == list:
        return dataset_or_datasets
    else:
        raise ValueError("dataset_or_datasets must be a str or list")


def _datasets_to_path(datasets):
    if len(datasets) == 1:
        return datasets[0]

    # We want to retain the order of DATASETS
    abbrs = []
    for k in DATASETS.keys():
        if k in datasets:
            abbrs.append(DATASETS[k]["abbr"])

    return "-".join(abbrs)


def get_source_nodes_path(dataset, entity, snapshot_date, version, trial_run=False):
    """Get full s3 path to source node data

    Args:
        dataset (str): A dataset name, for example 'mags_hocas'
        entity (str): An entity type, for example 'person', or 'journey'
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        trial_run (bool, optional): If a trial run, output to a different directory to avoid overwriting real data
    """

    _validate_all(**locals())
    tr_path = _get_trial_run_path_string(trial_run)

    path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        SOURCE_NODES_ROOT,
        dataset,
        entity,
        f"version={version}",
        f"snapshot_date={snapshot_date}",
    )

    return path


def get_standardised_nodes_path(
    dataset_or_datasets, job_name, entity, snapshot_date, version, trial_run=False
):
    """Get full s3 path to standardised node data

    Args:
        dataset_or_datasets (str or arr): An array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        job_name (str): The name of a specific linking job e.g. no_companies.
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        trial_run (bool, optional): If a trial run, output to a different directory to avoid overwriting real data
    """
    _validate_all(**locals())
    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    tr_path = _get_trial_run_path_string(trial_run)

    if len(datasets) == 1:
        snapshot_date_colname = "snapshot_date"
    else:
        snapshot_date_colname = "linking_snapshot_date"

    path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        STANDARDISED_NODES_ROOT,
        f"version={version}",
        f"input_datasets={datasets_path}",
        f"job_name={job_name}",
        f"entity={entity}",
        f"{snapshot_date_colname}={snapshot_date}",
    )

    return path


def get_training_directories(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    blocking_group,
    trial_run=False,
):
    """Get full s3 path to edges data

    Args:
        entity (str): An entity type, for example 'person', or 'journey'
        dataset_or_datasets (arr): An array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        job_name (str): The name of the job
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        trial_run (bool): If a trial run, output to a different directory to avoid overwriting real data

    """

    _validate_all(**locals())

    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    tr_path = _get_trial_run_path_string(trial_run)

    base_path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        TRAINING_ROOT,
        entity,
        f"{version}",
        f"{datasets_path}",
        f"{job_name}",
        f"{snapshot_date}",
    )

    blocking_group_path = os.path.join(base_path, "blocking_groups", blocking_group)
    training_charts_path = os.path.join(blocking_group_path, "charts")

    training_data_path = os.path.join(blocking_group_path, "data")
    training_models_path = os.path.join(blocking_group_path, "models")

    training_combined_model_path = os.path.join(base_path, "combined_model")

    paths = {
        "training_directory_path": base_path,
        "training_charts_path": training_charts_path,
        "training_models_path": training_models_path,
        "training_data_path": training_data_path,
        "training_combined_model_path": training_combined_model_path,
    }

    return paths


def get_edges_path(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    trial_run=False,
):
    """Get full s3 path to edges data

    Args:
        entity (str): An entity type, for example 'person', or 'journey'
        dataset_or_datasets (arr): An array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        job_name (str): The name of the job
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        blocking_group (str): The name of this set of blocking rules, to accomodate multiple splink jobs for
            different blocking rules e.g. dob_block
        trial_run (bool): If a trial run, output to a different directory to avoid overwriting real data

    """

    _validate_all(**locals())

    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    if len(datasets) == 1:
        snapshot_date_colname = "snapshot_date"
    else:
        snapshot_date_colname = "linking_snapshot_date"

    tr_path = _get_trial_run_path_string(trial_run)

    path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        EDGES_ROOT,
        entity,
        f"version={version}",
        f"input_datasets={datasets_path}",
        f"job_name={job_name}",
        f"{snapshot_date_colname}={snapshot_date}",
    )

    return path


def get_charts_paths(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    trial_run=False,
):
    """Get full s3 path to edges data

    Args:
        entity (str): An entity type, for example 'person', or 'journey'
        dataset_or_datasets (str or arr): A dataset (e.g. 'mags_hocas', or an array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        blocking_group (str): The name of this set of blocking rules, to accomodate multiple splink jobs for
            different blocking rules e.g. dob_block
        ptype (str): What type of information are we persisting? Either "iter_charts", "other_charts", "params"
        trial_run (bool): If a trial run, output to a different directory to avoid overwriting real data
    """

    _validate_all(**locals())

    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    tr_path = _get_trial_run_path_string(trial_run)

    base_path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        CHARTS_ROOT,
        entity,
        version,
        datasets_path,
        job_name,
        snapshot_date,
    )

    charts_paths = {
        "charts_directory_path": base_path,
        "charts_roc_path": os.path.join(base_path, "roc.html"),
        "charts_precision_recall_path": os.path.join(
            base_path, "precision_recall.html"
        ),
        "charts_histogram_path": os.path.join(base_path, "score_histogram.html"),
        "charts_final_model_bayes_path": os.path.join(
            base_path, "final_model_bayes_factors.html"
        ),
    }

    return charts_paths


def _get_tempfiles_paths(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    blocking_group,
    trial_run=False,
):
    """Get full s3 path to edges data

    Args:
        entity (str): An entity type, for example 'person', or 'journey'
        dataset_or_datasets (str or arr): A dataset (e.g. 'mags_hocas', or an array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        job_name (str): The name of a specific linking job e.g. no_companies.
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        blocking_group (str): The name of this set of blocking rules, to accomodate multiple splink jobs for
            different blocking rules e.g. dob_block
        trial_run (bool): If a trial run, output to a different directory to avoid overwriting real data

    """

    _validate_all(**locals())
    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    if len(datasets) == 1:
        snapshot_date_colname = "snapshot_date"
    else:
        snapshot_date_colname = "linking_snapshot_date"

    now = datetime.datetime.now()
    today_string = now.strftime("%Y%m%d")

    tr_path = _get_trial_run_path_string(trial_run)

    paths = {}
    for ptype in ["blocked", "scored"]:
        path = os.path.join(
            BUCKET_MAIN_ROOT,
            tr_path,
            TEMPFILES_ROOT,
            f"run_{today_string}",
            entity,
            ptype,
            f"version={version}",
            f"input_datasets={datasets_path}",
            f"job_name={job_name}",
            f"{snapshot_date_colname}={snapshot_date}",
            f"blocking_group={blocking_group}",
        )
        paths[f"{ptype}_tempfiles_path"] = path

    return paths


def get_cluster_path(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    trial_run=False,
):
    """Get full s3 path to clusters data

    Args:
        entity (str): An entity type, for example 'person', or 'journey'
        datasets (arr): An array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        job_name (str): The name of the job
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        trial_run (bool): If a trial run, output to a different directory to avoid overwriting real data

    """

    _validate_all(**locals())
    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    if len(datasets) == 1:
        snapshot_date_colname = "snapshot_date"
    else:
        snapshot_date_colname = "linking_snapshot_date"

    tr_path = _get_trial_run_path_string(trial_run)

    path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        CLUSTER_ROOT,
        entity,
        f"version={version}",
        f"input_datasets={datasets_path}",
        f"job_name={job_name}",
        f"{snapshot_date_colname}={snapshot_date}",
    )

    return path


def get_graph_analytics_path(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    metric_entity_type,
    cluster_colname,
    trial_run=False,
):
    """Get full s3 path to graph_analytics data

    Args:
        entity (str): An entity type, for example 'person', or 'journey'
        datasets (arr): An array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        job_name (str): The name of the job
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        metric_entity_type (str): The type of metric, e.g. edge, node, cluster
        cluster_colname(str): The name of the cluster column e.g. cluster_medium
        trial_run (bool): If a trial run, output to a different directory to avoid overwriting real data

    """

    _validate_all(**locals())
    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    if len(datasets) == 1:
        snapshot_date_colname = "snapshot_date"
    else:
        snapshot_date_colname = "linking_snapshot_date"

    tr_path = _get_trial_run_path_string(trial_run)

    path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        GRAPH_ANALYTICS_ROOT,
        entity,
        f"version={version}",
        f"input_datasets={datasets_path}",
        f"job_name={job_name}",
        f"metric_entity_type={metric_entity_type}",
        f"cluster_colname={cluster_colname}",
        f"{snapshot_date_colname}={snapshot_date}",
    )

    return path


def get_cluster_truth_path(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    labelling_exercise,
    cluster_colname,
    trial_run=False,
):
    """Get full s3 path to cluster_truth data (clusters and whether they contain FP, FN etc)

    Args:
        entity (str): An entity type, for example 'person', or 'journey'
        datasets (arr): An array of one or more dataset - e.g. ['mags_hocas', 'crown_xhibit']
        job_name (str): The name of the job
        snapshot_date (str): A snapshot date, for example '2020-03-21'
        version (str): The version, to account for re-running jobs when codebase is updated
        cluster_colname(str): The name of the cluster column e.g. cluster_medium
        trial_run (bool): If a trial run, output to a different directory to avoid overwriting real data

    """

    _validate_all(**locals())
    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    if len(datasets) == 1:
        snapshot_date_colname = "snapshot_date"
    else:
        snapshot_date_colname = "linking_snapshot_date"

    tr_path = _get_trial_run_path_string(trial_run)

    path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        QA_ROOT,
        "cluster_truth",
        entity,
        f"version={version}",
        f"input_datasets={datasets_path}",
        f"job_name={job_name}",
        f"{snapshot_date_colname}={snapshot_date}",
        f"labelling_exercise={labelling_exercise}",
        f"cluster_colname={cluster_colname}",
    )

    return path


def get_qa_paths(
    entity,
    dataset_or_datasets,
    job_name,
    snapshot_date,
    version,
    labelling_exercise,
    trial_run=False,
):

    _validate_all(**locals())
    datasets = _standardise_dataset_or_datasets_to_arr(dataset_or_datasets)
    datasets_path = _datasets_to_path(datasets)

    if len(datasets) == 1:
        snapshot_date_colname = "snapshot_date"
    else:
        snapshot_date_colname = "linking_snapshot_date"

    paths = {}
    tr_path = _get_trial_run_path_string(trial_run)

    folders = [
        "clerical_labels",
        "sample_for_labelling",
        "cached_potential_matches_for_labelling",
        "labels_with_scores",
    ]
    for f in folders:
        path = os.path.join(
            BUCKET_MAIN_ROOT,
            tr_path,
            QA_ROOT,
            f,
            entity,
            f"version={version}",
            f"input_datasets={datasets_path}",
            f"job_name={job_name}",
            f"{snapshot_date_colname}={snapshot_date}",
            f"labelling_exercise={labelling_exercise}",
        )

        paths[f"{f}_path"] = path

    path = os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        QA_ROOT,
        "clerical_labels",
        entity,
        f"version={version}",
        f"input_datasets={datasets_path}",
    )
    paths["clerical_labels_base_path"] = path

    return paths


def get_product_path(
    product_group, product_name, snapshot_date, version, trial_run=False
):
    _validate_all(**locals())

    tr_path = _get_trial_run_path_string(trial_run)

    return os.path.join(
        BUCKET_MAIN_ROOT,
        tr_path,
        PRODUCTS_ROOT,
        product_group,
        product_name,
        f"version={version}",
        f"linking_snapshot_date={snapshot_date}",
    )


def get_config_path(config_name, snapshot_date, version, trial_run=False):
    _validate_all(**locals())

    tr_path = _get_trial_run_path_string(trial_run)

    return os.path.join(
        BUCKET_MAIN_ROOT, tr_path, CONFIG_ROOT, config_name, version, snapshot_date
    )


def get_override_file_path(trial_run=False):
    tr_path = _get_trial_run_path_string(trial_run)
    return os.path.join(
        BUCKET_MAIN_ROOT, tr_path, CONFIG_ROOT, "manual_override", "dates.json"
    )


def _get_datasets_from_string(dataset_string):
    if "-" not in dataset_string:
        return [dataset_string]

    abbr_datasets = dataset_string.split("-")
    lookup = {v["abbr"]: v["name"] for v in DATASETS.values()}
    datasets = [lookup[a] for a in abbr_datasets]

    return datasets


def parse_path(path):
    first_part = path.split("/")[0]

    lookup = {
        "model_training": _parse_training_generation,
        "edge_generation": _parse_edge_generation,
        "cluster_generation": _parse_cluster_generation,
        "graph_analytics": _parse_graph_analytics,
        "node_generation": _parse_nodes_generation,
        "qa": _parse_qa,
        "other_glue_jobs": _parse_other_glue_jobs,
    }

    parsed_args = lookup[first_part](path)
    return parsed_args


def _parse_nodes_generation(path):
    parts = path.split("/")

    if parts[1] == "source_nodes":
        parsed_args = {}

        parsed_args["dataset"] = parts[2]
        parsed_args["entity"] = parts[3]

        return parsed_args

    if parts[1] == "standardised_nodes":
        parsed_args = {}

        parsed_args["dataset_or_datasets"] = _get_datasets_from_string(parts[2])
        parsed_args["job_name"] = parts[3]
        parsed_args["entity"] = parts[4]

        if len(parsed_args["dataset_or_datasets"]) == 1:
            parsed_args["dataset"] = parsed_args["dataset_or_datasets"][0]

        return parsed_args


def _parse_training_generation(path):
    parts = path.split("/")
    parsed_args = {}
    parsed_args["entity"] = parts[1]
    parsed_args["dataset_or_datasets"] = _get_datasets_from_string(parts[2])
    parsed_args["job_name"] = parts[3]
    parsed_args["blocking_group"] = parts[4]

    if len(parsed_args["dataset_or_datasets"]) == 1:
        parsed_args["dataset"] = parsed_args["dataset_or_datasets"][0]

    return parsed_args


def _parse_edge_generation(path):
    parts = path.split("/")
    parsed_args = {}
    parsed_args["entity"] = parts[1]
    parsed_args["dataset_or_datasets"] = _get_datasets_from_string(parts[2])
    parsed_args["job_name"] = parts[3]

    if len(parsed_args["dataset_or_datasets"]) == 1:
        parsed_args["dataset"] = parsed_args["dataset_or_datasets"][0]

    return parsed_args


def _parse_qa(path):
    parts = path.split("/")
    parsed_args = {}
    parsed_args["entity"] = parts[2]
    parsed_args["dataset_or_datasets"] = _get_datasets_from_string(parts[3])
    parsed_args["job_name"] = parts[4]

    if len(parsed_args["dataset_or_datasets"]) == 1:
        parsed_args["dataset"] = parsed_args["dataset_or_datasets"][0]

    return parsed_args


def _parse_other_glue_jobs(path):
    return {}


def _parse_cluster_generation(path):
    parts = path.split("/")
    parsed_args = {}
    parsed_args["entity"] = parts[1]
    parsed_args["dataset_or_datasets"] = _get_datasets_from_string(parts[2])
    parsed_args["job_name"] = parts[3]

    if len(parsed_args["dataset_or_datasets"]) == 1:
        parsed_args["dataset"] = parsed_args["dataset_or_datasets"][0]

    return parsed_args


def _parse_graph_analytics(path):
    parts = path.split("/")
    parsed_args = {}
    parsed_args["entity"] = parts[1]
    parsed_args["dataset_or_datasets"] = _get_datasets_from_string(parts[2])
    parsed_args["job_name"] = parts[3]

    if len(parsed_args["dataset_or_datasets"]) == 1:
        parsed_args["dataset"] = parsed_args["dataset_or_datasets"][0]

    return parsed_args


def _get_arguments_for_this_function(this_fn, **args):
    # Note that not all arguments will necessarily exist
    varnames = inspect.getfullargspec(this_fn)[0]
    return {v: args[v] for v in varnames if v in args.keys()}


def _get_all_paths(**args):

    fns = {
        "source_nodes_path": get_source_nodes_path,
        "standardised_nodes_path": get_standardised_nodes_path,
        "edges_path": get_edges_path,
        "training": get_training_directories,
        "charts": get_charts_paths,
        "tempfiles": _get_tempfiles_paths,
        "clusters_path": get_cluster_path,
        "qa": get_qa_paths,
        "graph_analytics_path": get_graph_analytics_path,
    }

    output_dict = {}
    for k, this_fn in fns.items():

        this_fn_args = _get_arguments_for_this_function(this_fn, **args)

        try:
            output = this_fn(**this_fn_args)
            if type(output) == str:
                output_dict[k] = this_fn(**this_fn_args)
            elif type(output) == dict:
                output_dict = {**output_dict, **output}
        except TypeError:
            # except ValueError:
            # This occurs if we don't have all the required argument for the function
            pass

    return output_dict


def get_paths_from_job_path(path, snapshot_date, version, trial_run=False, **kwargs):
    parsed_args = parse_path(path)
    parsed_args["snapshot_date"] = snapshot_date
    parsed_args["version"] = version
    parsed_args["trial_run"] = trial_run
    parsed_args = {**parsed_args, **kwargs}
    return _get_all_paths(**parsed_args)


def get_paths_from_kwargs(**kwargs):
    return _get_all_paths(**kwargs)


if __name__ == "__main__":

    job_path = "graph_analytics/person/uk_citizens_max_groupsize_20/basic/01_graph_analytics/job.py"
    job_path = "qa/cluster_truth/person/uk_citizens_max_groupsize_20/basic/job.py"
    parsed_args = parse_path(job_path)
    # print(parsed_args)

    gap = get_graph_analytics_path(
        parsed_args["entity"],
        parsed_args["dataset_or_datasets"],
        parsed_args["job_name"],
        snapshot_date="2021-01-01",
        version="v01",
        metric_entity_type="cluster",
        cluster_colname="cluster_medium",
        trial_run=False,
    )

    # # paths = get_paths_from_job_path(job_path, "2021-01-01", "v01", trial_run=True)

    # print(gap)
    # # print(json.dumps(paths, indent=4))

    clusters_path = get_cluster_truth_path(
        "person",
        "uk_citizens_max_groupsize_20",
        "basic",
        "2020-01-01",
        "v01",
        "synth_data",
        "cluster_medium",
        trial_run=False,
    )

    print(clusters_path)
