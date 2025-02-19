import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime
import hashlib
import json

# Glueジョブの初期化
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "input_bucket", "processed_bucket", "dynamodb_table"]
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# DynamoDB clientの初期化
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(args["dynamodb_table"])

# 前段の分析結果を読み込む
analysis_path = f"s3://{args['input_bucket']}/analysis_results"
latest_analysis = max(
    [
        path["path"].split("/")[-2]
        for path in spark.read.format("json")
        .load(f"{analysis_path}/*/metadata.json")
        .select("path")
        .collect()
    ]
)

# メタデータを読み込む
s3 = boto3.client("s3")
metadata_obj = s3.get_object(
    Bucket=args["input_bucket"], Key=f"analysis_results/{latest_analysis}/metadata.json"
)
metadata = json.loads(metadata_obj["Body"].read().decode("utf-8"))

# 分析結果を読み込む
word_counts_df = spark.read.parquet(f"{analysis_path}/{latest_analysis}/word_counts")

# 分析結果をDynamoDBに保存
for row in word_counts_df.rdd.collect():
    row_dict = row.asDict()
    word = row_dict["word"]
    unique_id = hashlib.md5(f"{word}_{metadata['timestamp']}".encode()).hexdigest()

    # DynamoDBに保存するアイテムを作成
    item = {
        "id": unique_id,
        "word": word,
        "count": row_dict["count"],
        "analysis_timestamp": metadata["timestamp"],
        "input_files": metadata["input_files"],
        "data_source": f"s3://{args['input_bucket']}",
    }

    table.put_item(Item=item)

# 処理済みデータを別バケットに移動
processed_path = f"s3://{args['processed_bucket']}/word_counts/{metadata['timestamp']}"
word_counts_df.write.mode("overwrite").parquet(processed_path)

job.commit()
