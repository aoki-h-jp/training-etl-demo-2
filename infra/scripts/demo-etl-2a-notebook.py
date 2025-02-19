import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, split
import json
import boto3
from datetime import datetime
import warnings

# ジョブパラメータの取得
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_bucket"])

# Glueコンテキストの初期化
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# データの処理ロジックをここに実装
# 特定のTSVファイルの読み込みと変換処理
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [f"s3://{args['input_bucket']}/amazon_reviews_us_Gift_Card_v1_00.tsv"],
    },
    format="csv",
    format_options={
        "separator": "\t",  # TSVファイルのため、タブ区切りを指定
        "withHeader": True,  # ヘッダー行を使用することを指定
    },
)

warnings.filterwarnings("ignore", message="DataFrame constructor is internal")

# DataFrameに変換
df = dynamic_frame.toDF()

# データフレームの情報を出力
print("データフレームのスキーマ:")
df.printSchema()
print("\nサンプルデータ（最初の5行）:")
df.show(5)

# 入力ファイルのパスを記録
input_files = [f"s3://{args['input_bucket']}/amazon_reviews_us_Gift_Card_v1_00.tsv"]

print("=== 単語出現頻度分析の開始 ===")
# レビュー本文の単語出現頻度分析
df_words = df.select(explode(split(df.review_body, " ")).alias("word"))
print("単語分割完了")

word_counts = df_words.groupBy("word").count().orderBy("count", ascending=False)
print("単語カウント完了")
print("上位10件の単語:")
word_counts.show(10)

print("=== 結果の保存処理開始 ===")
# タイムスタンプを含むパスを生成
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"s3://demo-etl-2a-processed-bucket/analysis_results/{timestamp}"
print(f"出力先パス: {output_path}")

# メタデータを含めて保存
metadata = {"timestamp": timestamp, "input_files": input_files}
print("メタデータ作成完了")

# メタデータをJSONとして保存
with open("/tmp/metadata.json", "w") as f:
    json.dump(metadata, f)
print("メタデータJSONファイル作成完了")

s3 = boto3.client("s3")
s3.upload_file(
    "/tmp/metadata.json",
    "demo-etl-2a-processed-bucket",  # バケット名を修正
    f"analysis_results/{timestamp}/metadata.json",
)
print("メタデータのS3アップロード完了")

# 単語出現頻度の結果を保存
print("単語出現頻度データの保存開始")
word_counts.write.mode("overwrite").parquet(f"{output_path}/word_counts")
print("単語出現頻度データの保存完了")

print("=== ジョブのコミット開始 ===")
job.commit()
print("=== ジョブ完了 ===")
