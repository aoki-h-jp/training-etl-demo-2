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


def batch_write_items(iterator):
    try:
        print("パーティション処理開始")
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(args["dynamodb_table"])

        items_batch = []
        count = 0

        for row in iterator:
            try:
                item = {
                    "id": f"word_{row['word']}",
                    "word": row["word"],
                    "count": int(row["count"]),
                    "timestamp": datetime.now().isoformat(),
                    "analysis_metadata": latest_metadata if latest_metadata else {},
                }
                items_batch.append(item)
                count += 1

                # 25件ごとにバッチ書き込み
                if len(items_batch) >= 25:
                    try:
                        with table.batch_writer() as batch:
                            for item in items_batch:
                                batch.put_item(Item=item)
                        print(f"サブバッチ処理完了: {len(items_batch)}件")
                        items_batch = []
                    except Exception as e:
                        print(f"バッチ書き込みエラー: {str(e)}")
                        raise

            except Exception as e:
                print(f"行処理エラー: {str(e)}")
                raise

        # 残りのアイテムを書き込み
        if items_batch:
            try:
                with table.batch_writer() as batch:
                    for item in items_batch:
                        batch.put_item(Item=item)
                print(f"最終バッチ処理完了: {len(items_batch)}件")
            except Exception as e:
                print(f"最終バッチ書き込みエラー: {str(e)}")
                raise

        print(f"パーティション処理完了: 合計{count}件")

    except Exception as e:
        print(f"パーティション処理エラー: {str(e)}")
        raise


# パーティション数を設定（Sparkワーカーの数に応じて調整）
NUM_PARTITIONS = 10
print(f"パーティション数: {NUM_PARTITIONS}")

# DataFrameをパーティション分割
total_count = word_counts_df.count()
print(f"全{total_count}行のデータを処理開始")

# repartitionでデータを分割し、foreachPartitionで並列処理
print("並列処理の開始")
word_counts_df.repartition(NUM_PARTITIONS).foreachPartition(batch_write_items)
print("並列処理の完了")

# 処理件数の確認
print(f"処理完了: 合計{total_count}件")

print("ジョブのコミット開始")
job.commit()
print("=== ジョブ完了 ===")
