#!/usr/bin/env python3

from pyspark.sql import SparkSession
import geopandas as gpd
from shapely import wkt
import numpy as np
np.object = object

# 定数
S3_INPUT_PATH = "s3://overturemaps-data/splitter_results_final/part-00000-98283570-31e0-40a7-8cd7-134dbed28e31-c000.zstd.parquet"
#S3_INPUT_PATH = "s3://overturemaps-data/splitter_output/4a7fdf5fae194aa0a52a5520aa70d0d8.snappy.parquet"
OUTPUT_GEOJSON = "./output.geojson"

def main():
    """
    S3上のParquetファイルを読み込み、カラム一覧とスキーマを標準出力するスクリプト  
    さらに、GeoJSONに変換してローカルに保存します。
    """
    # SparkSessionの作成
    spark = create_spark_session()

    # S3からParquetファイルを読み込む
    df = spark.read.parquet(S3_INPUT_PATH)

    # Parquetファイルのカラム一覧を出力
    print("=== Parquetファイルのカラム一覧 ===")
    print(df.columns)

    # スキーマの詳細を出力
    print("=== Parquetファイルのスキーマ ===")
    df.printSchema()

    # Spark DataFrameをPandas DataFrameに変換
    pandas_df = df.toPandas()

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
    return spark


if __name__ == "__main__":
    main()
