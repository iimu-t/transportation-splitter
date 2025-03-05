#!/usr/bin/env python3

import awswrangler as wr
import boto3
from shapely import wkt
from tqdm import tqdm
import pandas as pd

# 各種の設定
workgroup = "dev-land-idata"
profile_name = "land-idata-dev"
athena_region = "us-west-2"
data_base = "release"
data_sorce = "overture"
session = boto3.Session(profile_name=profile_name, region_name=athena_region)

def main():
    # ハードコーディングされたS3出力パス
    output_path = "s3://overturemaps-data/splitter_output/"

    # 直接定義したポリゴンのWKT文字列
    target_area = (
        "POLYGON((139.38615527857462 35.981337866344845, "
        "139.38615527857462 35.58011976568878, "
        "139.93604831492235 35.58011976568878, "
        "139.93604831492235 35.981337866344845, "
        "139.38615527857462 35.981337866344845))"
    )

    query = f"""
    SELECT 
        *,
        ST_ASTEXT(ST_GEOMFROMBINARY(geometry)) as geometry_wkt
    FROM v2024_09_18_0
    WHERE ST_INTERSECTS(
        ST_GEOMETRYFROMTEXT('{target_area}'),
        ST_GEOMFROMBINARY(geometry)
    )
    AND theme = 'transportation'
    AND type = 'segment'
    AND subtype = 'road'
    ORDER BY id ASC
    LIMIT 100;
    """

    # Athenaからデータ取得
    df = query_overture_with_wrangler(
        query=query,
        session=session,
        data_base=data_base,
        data_sorce=data_sorce,
        workgroup=workgroup
    )

    # geometry_wkt列をshapelyオブジェクトに変換
    tqdm.pandas(desc="Converting geometries")
    df["geometry"] = df["geometry_wkt"].progress_apply(wkt.loads)

    # Parquet保存を容易にするため、geometryはWKT形式の文字列として保持する
    df["geometry_wkt"] = df["geometry"].apply(lambda geom: geom.wkt)
    # 不要なshapelyオブジェクト列を削除
    df = df.drop(columns=["geometry"])

    # awswranglerを利用してS3にParquet形式で保存
    wr.s3.to_parquet(
        df=df,
        path=output_path,
        dataset=True,
        mode="overwrite"
    )
    print(f"データは {output_path} にParquet形式で保存されました。")

def query_overture_with_wrangler(query: str, session: boto3.Session, data_base: str, data_sorce: str, workgroup: str):
    df = wr.athena.read_sql_query(
        sql=query,
        database=data_base,
        data_source=data_sorce,
        boto3_session=session,
        workgroup=workgroup,
        ctas_approach=False,
    )
    return df

if __name__ == "__main__":
    main()
