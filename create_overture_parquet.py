#!/usr/bin/env python3

import json
import awswrangler as wr
import boto3
import geopandas as gpd
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon
from tqdm import tqdm
import re
import pandas as pd

# 各種の情報の設定
workgroup = "dev-land-idata"
profile_name = "land-idata-dev"
athena_region = "us-west-2"
data_base = "release"
data_sorce = "overture"
session = boto3.Session(profile_name=profile_name, region_name=athena_region)

def main():
    # ポリゴンの簡素化とWKT形式での取得
    simplified_wkt = simplify_polygon('./japan_polygon.geojson')

    query = f"""
    WITH route_segments AS (
        -- 1. 指定した路線名のセグメントを取得
        SELECT 
            id,
            ST_ASTEXT(ST_GEOMFROMBINARY(geometry)) AS geometry_wkt,
            bbox,
            version,
            sources,
            subtype,
            class,
            names,
            connector_ids,
            connectors,
            routes,
            subclass,
            subclass_rules,
            access_restrictions,
            level_rules,
            destinations,
            prohibited_transitions,
            road_surface,
            road_flags,
            speed_limits,
            width_rules,
            theme,
            type
        FROM v2024_09_18_0
        WHERE ST_INTERSECTS(
            ST_GEOMETRYFROMTEXT('{simplified_wkt}'),
            ST_GEOMFROMBINARY(geometry)
        )
          AND theme = 'transportation'
          AND type = 'segment'
          AND subtype = 'road'
          AND json_extract_scalar(CAST(names AS JSON), '$.primary') = '京葉道路'
    ),
    expanded_connector_ids AS (
        -- セグメントに含まれるconnector_idsをリスト展開
        SELECT 
            id AS segment_id,
            connector_id
        FROM route_segments,
            UNNEST(connector_ids) AS t(connector_id)
    ),
    connector_details AS (
        -- セグメントに関連するコネクター情報を、connector_idsに基づいて取得
        SELECT 
            id AS connector_id,
            ST_ASTEXT(ST_GEOMFROMBINARY(geometry)) AS geometry_wkt,
            bbox,
            version,
            sources,
            theme,
            type
        FROM v2024_09_18_0
        WHERE theme = 'transportation'
          AND type = 'connector'
          AND id IN (
              SELECT connector_id FROM expanded_connector_ids
          )
    ),
    route_connectors AS (
        -- connector_details をセグメント側のスキーマに合わせる（存在しないカラムはNULL）
        SELECT
            connector_id AS id,
            geometry_wkt,
            bbox,
            version,
            sources,
            NULL AS subtype,
            NULL AS class,
            NULL AS names,
            NULL AS connector_ids,
            NULL AS connectors,
            NULL AS routes,
            NULL AS subclass,
            NULL AS subclass_rules,
            NULL AS access_restrictions,
            NULL AS level_rules,
            NULL AS destinations,
            NULL AS prohibited_transitions,
            NULL AS road_surface,
            NULL AS road_flags,
            NULL AS speed_limits,
            NULL AS width_rules,
            theme,
            type
        FROM connector_details
    )
    -- セグメントとコネクタを共通のスキーマで連結
    SELECT * FROM route_segments
    UNION ALL
    SELECT * FROM route_connectors
    ORDER BY id, type;
    """

    df = query_overture_with_wrangler(
        query=query,
        session=session,
        data_base=data_base,
        data_sorce=data_sorce,
        workgroup=workgroup
    )
    
    # Parquet形式でS3に保存
    s3_parquet_path = "s3://overturemaps-data/splitter_output/exp_con.parquet"
    wr.s3.to_parquet(
        df=df,
        path=s3_parquet_path,
        index=False
    )
    print(f"Parquetファイルを '{s3_parquet_path}' に保存しました。")
    
    # GeoJSON形式で保存（ローカルに保存する例）
    dataframe_to_geojson(df, 'exp_con.geojson')
    print("GeoJSONファイル 'exp_con.geojson' を保存しました。")

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

def simplify_polygon(input_path: str, tolerance: float = 0.01):
    polygon_gdf = gpd.read_file(input_path)
    polygon_geom = polygon_gdf.geometry.iloc[0]
    simplified_polygon = polygon_geom.simplify(tolerance, preserve_topology=True)
    return simplified_polygon.wkt.replace("'", "''")

def dataframe_to_geojson(input_df, output_path):
    # WKT形式のgeometryをshapelyジオメトリオブジェクトに変換
    tqdm.pandas(desc="Converting geometries")
    input_df["geometry"] = input_df["geometry_wkt"].progress_apply(wkt.loads)
    # geometry_wkt列を削除
    input_df = input_df.drop(columns=["geometry_wkt"])
    # GeoDataFrameを作成（CRSはEPSG:4326として設定）
    output_gdf = gpd.GeoDataFrame(input_df, geometry="geometry", crs="EPSG:4326")
    # GeoJSONとして出力
    output_gdf.to_file(output_path, driver="GeoJSON")

if __name__ == "__main__":
    main()
