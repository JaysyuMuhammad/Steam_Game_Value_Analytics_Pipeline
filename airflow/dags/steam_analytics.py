"""
airflow/dags/steam_analytics.py

Daily Steam Game Value Analytics Pipeline

Layer:
    Bronze : Raw API response (JSON apa adanya, tanpa transformasi)
    Silver : Cleaned + integrated data warehouse (Star Schema) -- dim_game,
             dim_genre, dim_platform, dim_date, fact_game_snapshot,
             game_source_mapping.
    Gold   : Analytical Data Mart (mart_game_value) -- siap dikonsumsi langsung oleh Metabase.
"""

import logging
from datetime import datetime, date

from airflow.decorators import dag, task

from src.extract import steamspy, rawg
from src.transform import matching, cleaning
from src.load import minio_client, postgres


logger = logging.getLogger(__name__)


# ==========================================================================
# CONFIGURATION
# ==========================================================================

default_args = {
    "owner": "data_engineer",
    "retries": 1,
}

STEAMSPY_MAX_PAGES = 3


@dag(
    dag_id="steam_analytics_daily",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=[
        "steam_analytics",
        "medallion",
        "daily",
        "data_warehouse",
        "data_mart",
    ],
)
def steam_analytics_pipeline():

    # ======================================================================
    # TASK 1
    # SteamSpy → Bronze MinIO
    # ======================================================================

    @task
    def extract_steamspy_to_bronze(ds=None):

        logical_date = date.fromisoformat(ds)

        logger.info("Memulai extract SteamSpy untuk %s", ds)

        if minio_client.object_exists("bronze", "steamspy", logical_date):
            logger.info("Bronze SteamSpy %s sudah ada. API call dilewati.", ds)
            return

        logger.info(
            "Mengambil SteamSpy dengan %d page (~%d game).",
            STEAMSPY_MAX_PAGES, STEAMSPY_MAX_PAGES * 1000,
        )

        data = steamspy.fetch_all_paginated(max_pages=STEAMSPY_MAX_PAGES)

        if not data:
            raise RuntimeError("SteamSpy tidak mengembalikan data.")

        minio_client.write_json("bronze", "steamspy", logical_date, data)

        logger.info("Bronze SteamSpy %s berhasil disimpan. Total game: %d", ds, len(data))


    # ======================================================================
    # TASK 2
    # SteamSpy Bronze → Deteksi game baru → RAWG API → Bronze MinIO
    # ======================================================================

    @task
    def extract_rawg_to_bronze(ds=None):

        logical_date = date.fromisoformat(ds)

        logger.info("Memulai extract RAWG untuk %s", ds)

        steamspy_data = minio_client.read_json("bronze", "steamspy", logical_date)
        logger.info("SteamSpy Bronze berisi %d game.", len(steamspy_data))

        if minio_client.object_exists("bronze", "rawg", logical_date):
            logger.info("Bronze RAWG %s sudah ada. RAWG API call dilewati.", ds)
            return

        conn = postgres.get_connection()
        try:
            existing_appids = postgres.get_existing_appids(conn)
        finally:
            conn.close()

        logger.info("Existing mapping ditemukan: %d AppID.", len(existing_appids))

        unmapped_games = matching.get_unmapped_games(steamspy_data, existing_appids)
        logger.info("Game belum memiliki mapping: %d", len(unmapped_games))

        if not unmapped_games:
            logger.info("Tidak ada game baru. Membuat Bronze RAWG kosong.")
            minio_client.write_json("bronze", "rawg", logical_date, {})
            return

        logger.info("Memulai RAWG extraction untuk %d game baru.", len(unmapped_games))
        rawg_raw_responses = rawg.fetch_rawg_for_unmapped(unmapped_games)

        minio_client.write_json("bronze", "rawg", logical_date, rawg_raw_responses)

        logger.info(
            "Bronze RAWG %s berhasil disimpan. Game yang diproses: %d",
            ds, len(unmapped_games),
        )


    # ======================================================================
    # TASK 3
    # Bronze → Matching → game_source_mapping → Silver DWH (Dimensions)
    # ======================================================================

    @task
    def process_new_games_and_dimensions(ds=None):

        logical_date = date.fromisoformat(ds)

        steamspy_data = minio_client.read_json("bronze", "steamspy", logical_date)

        rawg_raw_responses = {}
        if minio_client.object_exists("bronze", "rawg", logical_date):
            rawg_raw_responses = minio_client.read_json("bronze", "rawg", logical_date)

        conn = postgres.get_connection()
        try:
            existing_appids = postgres.get_existing_appids(conn)

            unmapped_games = matching.get_unmapped_games(steamspy_data, existing_appids)
            logger.info("Game baru yang akan di-matching: %d", len(unmapped_games))

            match_records = matching.match_new_games_from_bronze(unmapped_games, rawg_raw_responses)

            steamspy_lookup = {int(game["appid"]): game for game in steamspy_data}

            if match_records:
                postgres.upsert_game_source_mapping(conn, match_records)
                postgres.upsert_dim_game(conn, match_records, steamspy_lookup)
                postgres.upsert_dim_genre_platform(conn, match_records)

            conn.commit()

            logger.info("Silver dimensions selesai. Game baru diproses: %d", len(match_records))

        finally:
            conn.close()


    # ======================================================================
    # TASK 4
    # Cleaning + Transformation → Silver DWH (Fact)
    # ======================================================================

    @task
    def process_facts_and_silver_layer(ds=None):

        logical_date = date.fromisoformat(ds)

        steamspy_data = minio_client.read_json("bronze", "steamspy", logical_date)

        conn = postgres.get_connection()
        try:
            date_key = postgres.ensure_dim_date(conn, logical_date)
            postgres.ensure_partition_exists(conn, logical_date)

            game_lookup = postgres.get_game_lookup(conn)

            silver_records = cleaning.build_silver_dataset(steamspy_data, game_lookup, logical_date)
            logger.info("Cleaning menghasilkan %d record.", len(silver_records))

            postgres.insert_fact_snapshot(conn, silver_records, game_lookup, date_key)

            conn.commit()

            logger.info("Silver DWH berhasil diproses. Fact records: %d", len(silver_records))

        finally:
            conn.close()

# ======================================================================
    # TASK 5
    # Silver DWH → Gold Data Marts (Seluruh Mart) → Metabase
    # ======================================================================

    @task
    def build_gold_data_mart(ds=None):

        logical_date = date.fromisoformat(ds)

        logger.info("Memulai build SELURUH Gold Data Mart untuk %s", ds)

        conn = postgres.get_connection()
        try:
            # UBAH BAGIAN INI: Panggil fungsi pembungkus yang baru
            postgres.build_all_gold_marts(conn, logical_date)
        finally:
            conn.close()

    # ======================================================================
    # DEPENDENCY
    # ======================================================================

    task_1 = extract_steamspy_to_bronze()
    task_2 = extract_rawg_to_bronze()
    task_3 = process_new_games_and_dimensions()
    task_4 = process_facts_and_silver_layer()
    task_5 = build_gold_data_mart()

    task_1 >> task_2 >> task_3 >> task_4 >> task_5


# ==========================================================================
# DAG INSTANCE
# ==========================================================================

dag_instance = steam_analytics_pipeline()