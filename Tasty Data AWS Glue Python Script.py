import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, explode
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ======== Config ========
raw_bucket = "tastyapidump"
clean_bucket = "tastydatacleaned"

# ======== 1. Read & Clean Recipes ========
recipes_raw = spark.read.option("multiline", "true").json(f"s3://{raw_bucket}/tastyapidump.json")

recipes_df = recipes_raw.withColumn("recipe", explode(col("results"))).select("recipe.*")

recipes_clean = recipes_df.select(
    col("name"),
    col("description"),
    col("thumbnail_url"),
    col("original_video_url"),
    col("total_time_minutes")
).fillna({
    "description": "No description available",
    "original_video_url": "No video"
})

recipes_clean = recipes_clean.withColumn("has_video", (col("original_video_url") != "No video"))
recipes_clean = recipes_clean.withColumn("is_long_prep", (col("total_time_minutes") > 60))

# Write temp JSON
recipes_clean.coalesce(1).write.mode("overwrite").json(f"s3://{clean_bucket}/temp_recipes/")

# Rename to fixed file name
s3 = boto3.client('s3')
objects = s3.list_objects_v2(Bucket=clean_bucket, Prefix="temp_recipes/")
for obj in objects.get("Contents", []):
    if obj["Key"].endswith(".json"):
        s3.copy_object(
            Bucket=clean_bucket,
            CopySource=f"{clean_bucket}/{obj['Key']}",
            Key="recipes_cleaned.json"
        )
        break
# Delete temp folder
for obj in objects.get("Contents", []):
    s3.delete_object(Bucket=clean_bucket, Key=obj["Key"])

# ======== 2. Read & Clean Tags ========
tags_raw = spark.read.option("multiline", "true").json(f"s3://{raw_bucket}/tastytags.json")

tags_df = tags_raw.withColumn("tag", explode(col("results"))).select("tag.*")

tags_clean = tags_df.select(
    col("id").alias("tag_id"),
    col("name").alias("tag_name"),
    col("type").alias("tag_type")
).filter(col("tag_name").isNotNull())

# Write temp JSON
tags_clean.coalesce(1).write.mode("overwrite").json(f"s3://{clean_bucket}/temp_tags/")

# Rename to fixed file name
objects = s3.list_objects_v2(Bucket=clean_bucket, Prefix="temp_tags/")
for obj in objects.get("Contents", []):
    if obj["Key"].endswith(".json"):
        s3.copy_object(
            Bucket=clean_bucket,
            CopySource=f"{clean_bucket}/{obj['Key']}",
            Key="tags_cleaned.json"
        )
        break
# Delete temp folder
for obj in objects.get("Contents", []):
    s3.delete_object(Bucket=clean_bucket, Key=obj["Key"])

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script for node Amazon S3
AmazonS3_node1754784966792 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tastyapidump"], "recurse": True}, transformation_ctx="AmazonS3_node1754784966792")

# Script for node Amazon S3
EvaluateDataQuality().process_rows(frame=AmazonS3_node1754784966792, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754784952031", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754785826340 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1754784966792, connection_type="s3", format="glueparquet", connection_options={"path": "s3://tastydatacleaned", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1754785826340")

job.commit()