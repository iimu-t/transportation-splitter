#!/usr/bin/env python3

from pyspark.sql import SparkSession
import geopandas as gpd
from shapely import wkt
import numpy as np

# 定数
#S3_INPUT_PATH = "s3://overturemaps-data/splitter_output/exp_con.parquet"
S3_INPUT_PATH = "s3://overturemaps-data/splitter_results/part-00000-b33114bc-20b9-4581-aa09-7b30236fa821-c000.zstd.parquet"
OUTPUT_GEOJSON = "./output.geojson"

def main():
    """
    S3上のParquetファイルを読み込み、カラム一覧とスキーマを標準出力するスクリプト  
    さらに、GeoJSONに変換してローカルに保存します。
    """
    # SparkSessionの作成
    spark = create_spark_session()

    # S3からParquetファイルを読み込む（Spark経由）
    try:
        spark_df = spark.read.parquet(S3_INPUT_PATH)
        if "geometry" in spark_df.columns:
            spark_df = spark_df.drop("geometry")
        pandas_df = spark_df.toPandas()
    except Exception as e:
        print("Spark read error, falling back to pyarrow:", e)
        import pyarrow.parquet as pq
        table = pq.read_table(S3_INPUT_PATH)
        pandas_df = table.to_pandas()
        spark_df = None

    # カラム一覧・スキーマ出力（Spark読み込み成功時のみ）
    if spark_df is not None:
        print("=== Parquetファイルのカラム一覧 ===")
        print(spark_df.columns)
        print("=== Parquetファイルのスキーマ ===")
        spark_df.printSchema()
    else:
        print("=== pyarrowで読み込んだデータのカラム一覧 ===")
        print(pandas_df.columns)

    # geometry_wkt列からGeoDataFrameを作成（geometry_wkt列がWKT形式の文字列である前提）
    pandas_df['geometry'] = pandas_df['geometry_wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(pandas_df, geometry='geometry')

    # GeoJSONに変換してローカルに保存
    gdf.to_file(OUTPUT_GEOJSON, driver="GeoJSON")
    print(f"GeoJSONファイルは '{OUTPUT_GEOJSON}' に保存されました。")

    # SparkSessionを停止
    spark.stop()

def create_spark_session(app_name: str = "Read S3 Parquet and Print Schema") -> SparkSession:
    """
    S3アクセス設定を含むSparkSessionを作成して返します。
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    sc = spark.sparkContext
    return spark


if __name__ == "__main__":
    main()
