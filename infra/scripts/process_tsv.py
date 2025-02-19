import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime
import json

print("=== ジョブ開始 ===")

print("ジョブパラメータの取得開始")
args = getResolvedOptions(sys.argv, ["JOB_NAME", "processed_bucket", "dynamodb_table"])
print(
    f"取得したパラメータ: JOB_NAME={args['JOB_NAME']}, BUCKET={args['processed_bucket']}, TABLE={args['dynamodb_table']}"
)

print("Glueコンテキストの初期化開始")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
print("Glueコンテキストの初期化完了")

print("DynamoDBクライアントの初期化開始")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(args["dynamodb_table"])
print("DynamoDBクライアントの初期化完了")

print("S3からメタデータの読み込み開始")
s3 = boto3.client("s3")
response = s3.list_objects_v2(
    Bucket=args["processed_bucket"],
    Prefix="analysis_results/",
)
print(
    f"S3オブジェクト一覧取得完了: {len(response.get('Contents', []))}件のオブジェクトを検出"
)

print("最新の分析結果を検索開始")
latest_analysis_path = None
latest_metadata = None
for obj in sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True):
    if obj["Key"].endswith("metadata.json"):
        print(f"メタデータファイルを検出: {obj['Key']}")
        metadata_obj = s3.get_object(
            Bucket=args["processed_bucket"],
            Key=obj["Key"],
        )
        latest_metadata = json.loads(metadata_obj["Body"].read().decode("utf-8"))
        latest_analysis_path = obj["Key"].rsplit("/", 1)[0]
        print(f"最新の分析パスを設定: {latest_analysis_path}")
        break

if not latest_analysis_path:
    print("分析結果が見つかりませんでした")
    sys.exit(1)

print("Parquetファイルの読み込み開始")
word_counts_df = spark.read.parquet(
    f"s3://{args['processed_bucket']}/{latest_analysis_path}/word_counts"
)
print(f"Parquetファイルの読み込み完了: {word_counts_df.count()}行のデータを検出")

print("DynamoDBへのバッチ書き込み処理の開始")


def batch_write_items(items):
    print(f"バッチ処理開始: {len(items)}件")
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args["dynamodb_table"])

    # DynamoDBの制限（25アイテム）に合わせてサブバッチに分割
    for i in range(0, len(items), 25):
        sub_batch = items[i : i + 25]
        with table.batch_writer() as batch:
            for item in sub_batch:
                batch.put_item(Item=item)
        print(
            f"サブバッチ処理完了: {len(sub_batch)}件 ({i + 1}～{i + len(sub_batch)}件目)"
        )


# DataFrameをパーティション分割して処理
PARTITION_SIZE = 1000  # より大きなバッチサイズ
print(f"パーティションサイズ: {PARTITION_SIZE}")

# collect()の代わりにtoLocalIterator()を使用してメモリ効率を改善
total_count = word_counts_df.count()
print(f"全{total_count}行のデータを処理開始")

items_batch = []
processed_count = 0

for row in word_counts_df.toLocalIterator():
    item = {
        "id": f"word_{row['word']}",
        "word": row["word"],
        "count": int(row["count"]),
        "timestamp": datetime.now().isoformat(),
        "analysis_metadata": latest_metadata if latest_metadata else {},
    }
    items_batch.append(item)
    processed_count += 1

    # パーティションサイズに達したら書き込み
    if len(items_batch) >= PARTITION_SIZE:
        batch_write_items(items_batch)
        print(
            f"進捗: {processed_count}/{total_count} ({(processed_count / total_count * 100):.1f}%)"
        )
        items_batch = []

# 残りのアイテムを書き込み
if items_batch:
    batch_write_items(items_batch)
    print(
        f"進捗: {processed_count}/{total_count} ({(processed_count / total_count * 100):.1f}%)"
    )

print("全データの書き込み完了")

print("ジョブのコミット開始")
job.commit()
print("=== ジョブ完了 ===")
