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
            id,  -- セグメントの一意なID
            connector_ids,  -- このセグメントに接続するコネクターIDのリスト
            ST_ASTEXT(ST_GEOMFROMBINARY(geometry)) AS geometry_wkt  -- ジオメトリをWKT形式で取得
        FROM v2024_09_18_0
        WHERE ST_INTERSECTS(
            ST_GEOMETRYFROMTEXT('{simplified_wkt}'),  -- 指定した範囲（WKT）との交差判定
            ST_GEOMFROMBINARY(geometry)
        )
        AND theme = 'transportation'  -- テーマが「交通」
        AND type = 'segment'  -- セグメントデータのみ取得
        AND subtype = 'rail'  -- 鉄道データのみ取得
        AND json_extract_scalar(CAST(names AS JSON), '$.primary') = '東北新幹線'
    ),

    expanded_connector_ids AS (
        -- 2. 各セグメントの connector_ids をリスト展開
        SELECT 
            id AS segment_id,  -- 元のセグメントID
            connector_id  -- 展開されたコネクターID
        FROM route_segments,
            UNNEST(connector_ids) AS t(connector_id)  -- connector_ids の配列をフラット化
    ),

    connector_details AS (
        -- 3. コネクターIDに基づいてコネクターの詳細情報を取得
        SELECT 
            id AS connector_id,  -- コネクターの一意なID
            ST_ASTEXT(ST_GEOMFROMBINARY(geometry)) AS geometry_wkt,  -- コネクターの位置情報をWKT形式で取得
            CAST(sources AS JSON) AS sources_json  -- コネクターのデータソース情報をJSONとして取得
        FROM v2024_09_18_0
        WHERE theme = 'transportation'  -- 交通データに限定
        AND id IN (
            SELECT connector_id  -- 取得したコネクターIDに一致するものを検索
            FROM expanded_connector_ids
        )
    )

    -- 4. セグメントIDと、それに関連するコネクターの情報を取得
    SELECT 
        e.segment_id AS original_segment_id,  -- 元のセグメントID
        c.connector_id AS related_connector_id,  -- 関連するコネクターID
        c.geometry_wkt AS geometry_wkt,  -- コネクターの位置情報（WKT）
        json_format(c.sources_json) AS sources  -- コネクターのデータソース情報をJSON形式で取得
    FROM expanded_connector_ids e
    LEFT JOIN connector_details c
    ON e.connector_id = c.connector_id  -- コネクターIDで結合
    ORDER BY e.segment_id, c.connector_id;
    """

    df = query_overture_with_wrangler(
        query=query,
        session=session,
        data_base=data_base,
        data_sorce=data_sorce,
        workgroup=workgroup
    )
    
    # 'routes'列をパースする
    #df = parse_routes_column(df)
    dataframe_to_geojson(df, 'exp_con.geojson')
    #df.to_csv('test.csv', index=False)

    '''    
    # 道路名を指定してフィルタリング（例：'国道1号'を含むもの）
    road_names = ['国道16号', '八王子バイパス']  # 複数の道路名を指定
    filtered_df = df[df['routes'].apply(
        lambda routes_list: any(any(road_name in route.get('name', '') for road_name in road_names) for route in routes_list)
    )].reset_index(drop=True)
    
    # GeoJSONファイル用のパスとCSVファイル用のパスを指定
    output_path_geojson = f"./out/{road_names[0]}/{road_names[0]}_otm.geojson"
    dataframe_to_geojson(filtered_df, output_path_geojson)

    # CSVとして保存
    output_path_csv = "roads_japan.csv"
    filtered_df.to_csv(output_path_csv, index=False)
    '''

def dataframe_to_geojson(input_df, output_path):
    # WKT形式のgeometryをshapelyジオメトリオブジェクトに変換
    tqdm.pandas(desc="Converting geometries")
    input_df["geometry"] = input_df["geometry_wkt"].progress_apply(wkt.loads)

    # geometry_wkt列を削除
    input_df = input_df.drop(columns=["geometry_wkt"])

    # ジオメトリ列を明示的に指定してGeoDataFrameを作成
    output_gdf = gpd.GeoDataFrame(input_df, geometry="geometry", crs="EPSG:4326")

    # GeoJSONとして出力
    output_gdf.to_file(output_path, driver="GeoJSON")


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
    """
    ポリゴンを簡素化してWKT形式で返す関数
    :param input_path: 元のポリゴンのGeoJSONファイルパス
    :param tolerance: 簡素化の許容値（デフォルトは0.01）
    :return: 簡素化したポリゴンのWKT形式
    """
    # 元のポリゴンを読み込み
    polygon_gdf = gpd.read_file(input_path)
    polygon_geom = polygon_gdf.geometry.iloc[0]

    # ポリゴンの簡素化
    simplified_polygon = polygon_geom.simplify(tolerance, preserve_topology=True)

    # WKT形式で返す（クエリに使用）
    return simplified_polygon.wkt.replace("'", "''")


def parse_routes_column(df):
    """
    DataFrameの'routes'列をパースして、正しいJSON形式のリストに変換する。
    """
    # 'routes'列の欠損値を空文字列に置き換える
    df['routes'] = df['routes'].fillna('')

    def fix_invalid_json(s):
        if not isinstance(s, str) or not s.strip():
            return ''
        s = s.replace('None', 'null')
        s = s.replace('=', ':')
        s = re.sub(r'([{,]\s*)(\w+)\s*:', r'\1"\2":', s)
        s = re.sub(r':\s*([^"\[{][^,}\]]*)', r':"\1"', s)
        return s

    def parse_routes(s):
        if not isinstance(s, str) or not s.strip():
            return []
        fixed_s = fix_invalid_json(s)
        if not fixed_s.strip():
            return []
        try:
            return json.loads(fixed_s)
        except json.JSONDecodeError:
            return []

    # 'routes'列に関数を適用
    df['routes'] = df['routes'].apply(parse_routes)
    return df

if __name__ == "__main__":
    main()
