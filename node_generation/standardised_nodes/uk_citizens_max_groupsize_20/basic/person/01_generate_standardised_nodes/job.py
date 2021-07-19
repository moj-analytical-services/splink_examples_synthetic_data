import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

import pyspark.sql.functions as f
from constants import get_paths_from_job_path

from custom_logger import get_custom_logger

from spark_config import add_udfs

from splink_data_standardisation.names import standardise_names
from splink_data_standardisation.postcode import postcode_to_inward_outward


sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()
spark = glue_context.spark_session

add_udfs(spark)

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

SNAPSHOT_DATE = args["snapshot_date"]
COMMIT_HASH = args["commit_hash"]


# Set up a custom logger than outputs to its own stream, grouped within the job run id
# to separate out custom logs from general spark logs
custom_log = get_custom_logger(args["JOB_RUN_ID"])

trial_run = args["trial_run"] == "true"

VERSION = args["version"]


# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
)


for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")


df = spark.read.parquet(paths["source_nodes_path"])


keep = [c for c in df.columns if not c.startswith("num_")]
df = df.select(keep)

df = df.withColumn("unique_id", f.monotonically_increasing_id())

df = standardise_names(df, ["full_name"])


expr = """
case
    when lat is not null
        then struct(lat, lng as long)
    else null
    end
"""
df = df.withColumn("lat_lng", f.expr(expr))

df = df.drop("lat", "lng")

df = postcode_to_inward_outward(df, "postcode", drop_orig=False)


# Derive columns needed for blocking
df = df.withColumn("dob_year_month", f.expr("left(dob, 7)"))
df = df.withColumn("dob_month_day", f.expr("right(dob, 5)"))
df = df.withColumn("dob_year_day", f.expr("concat(left(dob, 4),'-',right(dob, 2))"))
df = df.withColumn("dob_year", f.expr("left(dob, 4)"))
df = df.withColumn("dob_day", f.expr("right(dob, 2)"))
df = df.withColumn("dob_month", f.expr("substring(dob, 6,2)"))

df = df.withColumn("surname_dm", f.expr("Dmetaphone(surname_std)"))
df = df.withColumn("forename1_dm", f.expr("Dmetaphone(forename1_std)"))
df = df.withColumn("forename2_dm", f.expr("Dmetaphone(forename2_std)"))


df.write.mode("overwrite").parquet(paths["standardised_nodes_path"])
custom_log.info(f'Outputted to path {paths["standardised_nodes_path"]}')
