from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import explode, split
import sys
from datetime import datetime
import json
import boto3

# Glueジョブの初期化
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_bucket"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3からTSVファイルを読み込む
dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": "\t"},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"s3://{args['input_bucket']}/"],
        "recurse": True,
        "groupFiles": "inPartition",
        "groupSize": "1048576",
    },
)

# DataFrameに変換
df = dyf.toDF()

# review_bodyカラムが存在する場合のみ処理を実行
if "review_body" in df.columns:
    # 入力ファイルのパスを取得
    input_files = [
        path["path"]
        for path in glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [f"s3://{args['input_bucket']}/"],
                "recurse": True,
            },
            format="csv",
        )
        .toDF()
        .select("path")
        .distinct()
        .collect()
    ]

    # レビュー本文の単語出現頻度分析
    df_words = df.select(explode(split(df.review_body, " ")).alias("word"))
    word_counts = df_words.groupBy("word").count().orderBy("count", ascending=False)

    # タイムスタンプを含むパスを生成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"s3://{args['input_bucket']}/analysis_results/{timestamp}"

    # メタデータを含めて保存
    metadata = {"timestamp": timestamp, "input_files": input_files}

    # メタデータをJSONとして保存
    with open("/tmp/metadata.json", "w") as f:
        json.dump(metadata, f)

    s3 = boto3.client("s3")
    s3.upload_file(
        "/tmp/metadata.json",
        args["input_bucket"],
        f"analysis_results/{timestamp}/metadata.json",
    )

    # 単語出現頻度の結果を保存
    word_counts.write.mode("overwrite").parquet(f"{output_path}/word_counts")
else:
    print(
        "review_body column not found in the input data. Skipping word count analysis."
    )

job.commit()
