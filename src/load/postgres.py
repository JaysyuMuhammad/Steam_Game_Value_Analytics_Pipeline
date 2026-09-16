"""
src/load/postgres.py

Modul untuk seluruh operasi baca/tulis ke PostgreSQL Data Warehouse.
Fungsi di sini dipanggil oleh orchestration (Airflow DAG) sesuai URUTAN
yang sudah dirancang 
"""

import logging
from datetime import date, timedelta
from typing import Optional

import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_warehouse"


def get_connection():
    """
    Koneksi ke Postgres warehouse LEWAT Airflow Connection 
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_conn()


# ==============================================================================
# 1. game_source_mapping -- cek appid yang sudah dikenal, simpan hasil matching baru
# ==============================================================================

def get_existing_appids(conn) -> set[int]:
    """Ambil semua steam_appid yang SUDAH punya mapping ke RAWG."""
    with conn.cursor() as cur:
        cur.execute("SELECT steam_appid FROM game_source_mapping;")
        rows = cur.fetchall()
    appids = {row[0] for row in rows}
    logger.info("Ditemukan %d appid yang sudah punya mapping", len(appids))
    return appids


def upsert_game_source_mapping(conn, match_records: list[dict]) -> None:
    """
    Simpan hasil entity resolution (dari matching.match_new_games_from_bronze)
    ke game_source_mapping. steam_appid bersifat UNIQUE, jadi kalau appid
    yang sama sudah ada (kasus jarang, misal re-run), datanya di-update.

    rawg_rating & metacritic ikut disimpan di sini sebagai CACHE -- ini
    yang dipakai get_game_lookup() untuk mengisi fact_game_snapshot setiap
    hari TANPA perlu memanggil RAWG API lagi.
    """
    if not match_records:
        logger.info("Tidak ada mapping baru untuk disimpan.")
        return

    rows = [
        (
            r["steam_appid"], r["rawg_id"], r["steam_name"], r["rawg_name"],
            r["match_status"], r["similarity_score"],
            r.get("rawg_rating"), r.get("rawg_metacritic"),
        )
        for r in match_records
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO game_source_mapping
                (steam_appid, rawg_id, steam_name, rawg_name, match_status,
                 similarity_score, rawg_rating, metacritic)
            VALUES %s
            ON CONFLICT (steam_appid) DO UPDATE SET
                rawg_id = EXCLUDED.rawg_id,
                rawg_name = EXCLUDED.rawg_name,
                match_status = EXCLUDED.match_status,
                similarity_score = EXCLUDED.similarity_score,
                rawg_rating = EXCLUDED.rawg_rating,
                metacritic = EXCLUDED.metacritic
            """,
            rows,
        )
    conn.commit()
    logger.info("Berhasil upsert %d baris ke game_source_mapping", len(rows))


# ==============================================================================
# 2. dim_game -- identitas game + cache rating/metacritic RAWG
# ==============================================================================

def upsert_dim_game(conn, match_records: list[dict], steamspy_lookup: dict[int, dict]) -> None:
    """
    Simpan game BARU (hasil matching hari ini) ke dim_game. Hanya
    match_status 'exact' dan 'high_confidence' yang otomatis masuk sebagai
    enrichment penuh -- 'review' dan 'unmatched' tetap tercatat di
    game_source_mapping (audit trail), tapi TIDAK otomatis dibuatkan
    dim_game supaya tidak mencemari data dengan kemungkinan game yang salah.

    """
    reliable_records = [r for r in match_records if r["match_status"] in ("exact", "high_confidence")]

    if not reliable_records:
        logger.info("Tidak ada game baru dengan status reliable untuk dimasukkan ke dim_game.")
        return

    rows = []
    for r in reliable_records:
        appid = r["steam_appid"]
        steamspy_info = steamspy_lookup.get(appid, {})
        release_date = _parse_release_date(r.get("rawg_released"))

        rows.append((
            appid,
            r["rawg_id"],
            r["steam_name"],
            steamspy_info.get("developer"),
            steamspy_info.get("publisher"),
            release_date,
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dim_game
                (steam_appid, rawg_id, game_name, developer, publisher, release_date)
            VALUES %s
            ON CONFLICT (steam_appid) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    logger.info("Berhasil insert %d game baru ke dim_game", len(rows))


def get_game_lookup(conn) -> dict[int, dict]:
    """
    Ambil semua game dari dim_game, JOIN dengan game_source_mapping untuk
    ambil cache rawg_rating & metacritic. Dipakai oleh
    cleaning.build_silver_dataset() untuk mengisi fact_game_snapshot
    TANPA perlu memanggil RAWG lagi.

    Return: {steam_appid: {"game_key":.., "rawg_rating":.., "metacritic":..}}
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dg.steam_appid, dg.game_key, gsm.rawg_rating, gsm.metacritic
            FROM dim_game dg
            LEFT JOIN game_source_mapping gsm ON dg.steam_appid = gsm.steam_appid;
        """)
        rows = cur.fetchall()

    lookup = {
        row[0]: {"game_key": row[1], "rawg_rating": row[2], "metacritic": row[3]}
        for row in rows
    }
    logger.info("get_game_lookup: %d game dimuat dari dim_game", len(lookup))
    return lookup


def _parse_release_date(release_date_str: Optional[str]):
    if not release_date_str:
        return None
    try:
        return date.fromisoformat(release_date_str)
    except ValueError:
        return None


# ==============================================================================
# 3. dim_genre / dim_platform + bridge table
# ==============================================================================

def upsert_dim_genre_platform(conn, match_records: list[dict]) -> None:
    """
    Untuk game baru yang reliable, pecah genres/platforms (nested dari RAWG)
    menjadi dim_genre, dim_platform, dan bridge table-nya.
    """
    from src.transform.cleaning import extract_genre_names, extract_platform_names

    reliable_records = [r for r in match_records if r["match_status"] in ("exact", "high_confidence")]
    if not reliable_records:
        return

    with conn.cursor() as cur:
        for r in reliable_records:
            cur.execute("SELECT game_key FROM dim_game WHERE steam_appid = %s;", (r["steam_appid"],))
            row = cur.fetchone()
            if row is None:
                continue
            game_key = row[0]

            for genre_name in extract_genre_names(r.get("rawg_genres")):
                cur.execute(
                    "INSERT INTO dim_genre (genre_name) VALUES (%s) "
                    "ON CONFLICT (genre_name) DO NOTHING RETURNING genre_key;",
                    (genre_name,),
                )
                genre_key_row = cur.fetchone()
                if genre_key_row is None:
                    cur.execute("SELECT genre_key FROM dim_genre WHERE genre_name = %s;", (genre_name,))
                    genre_key_row = cur.fetchone()
                genre_key = genre_key_row[0]

                cur.execute(
                    "INSERT INTO bridge_game_genre (game_key, genre_key) VALUES (%s, %s) "
                    "ON CONFLICT DO NOTHING;",
                    (game_key, genre_key),
                )

            for platform_name in extract_platform_names(r.get("rawg_platforms")):
                cur.execute(
                    "INSERT INTO dim_platform (platform_name) VALUES (%s) "
                    "ON CONFLICT (platform_name) DO NOTHING RETURNING platform_key;",
                    (platform_name,),
                )
                platform_key_row = cur.fetchone()
                if platform_key_row is None:
                    cur.execute("SELECT platform_key FROM dim_platform WHERE platform_name = %s;", (platform_name,))
                    platform_key_row = cur.fetchone()
                platform_key = platform_key_row[0]

                cur.execute(
                    "INSERT INTO bridge_game_platform (game_key, platform_key) VALUES (%s, %s) "
                    "ON CONFLICT DO NOTHING;",
                    (game_key, platform_key),
                )

    conn.commit()
    logger.info("Selesai upsert dim_genre/dim_platform + bridge table untuk %d game baru", len(reliable_records))


# ==============================================================================
# 4. dim_date
# ==============================================================================

def ensure_dim_date(conn, snapshot_date: date) -> int:
    """Pastikan snapshot_date sudah ada di dim_date. Return date_key-nya."""
    date_key = int(snapshot_date.strftime("%Y%m%d"))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_date (date_key, full_date, day_of_week, week_of_year, month, month_name, quarter, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO NOTHING;
            """,
            (
                date_key, snapshot_date, snapshot_date.strftime("%A"),
                int(snapshot_date.strftime("%W")), snapshot_date.month,
                snapshot_date.strftime("%B"), (snapshot_date.month - 1) // 3 + 1,
                snapshot_date.year,
            ),
        )
    conn.commit()
    return date_key


# ==============================================================================
# 5. Partitioning otomatis untuk fact_game_snapshot
# ==============================================================================

def ensure_partition_exists(conn, snapshot_date: date) -> None:
    """
    Pastikan partisi bulan untuk snapshot_date sudah ada di fact_game_snapshot.
    Dipanggil sebelum insert_fact_snapshot().
    """
    month_start = snapshot_date.replace(day=1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    partition_name = f"fact_game_snapshot_{month_start.year}_{month_start.month:02d}"

    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {partition_name}
            PARTITION OF fact_game_snapshot
            FOR VALUES FROM (%s) TO (%s);
        """, (month_start, next_month))
    conn.commit()
    logger.info("Partisi %s dipastikan tersedia", partition_name)


# ==============================================================================
# 6. fact_game_snapshot
# ==============================================================================

def insert_fact_snapshot(conn, silver_records: list[dict], game_lookup: dict[int, dict], date_key: int) -> None:
    """
    Insert seluruh record hasil cleaning.build_silver_dataset() ke
    fact_game_snapshot. game_key didapat dari game_lookup (appid -> game_key).
    """
    rows = []
    skipped = 0

    for r in silver_records:
        appid = r["steam_appid"]
        game_info = game_lookup.get(appid)
        if game_info is None:
            skipped += 1
            continue

        rows.append((
            game_info["game_key"], date_key, r["snapshot_date"],
            r["price_usd"], r["initial_price_usd"], r["discount_pct"],
            r["owners_low"], r["owners_high"], r["concurrent_users"],
            r["steam_review_pct"], r["steam_review_count"],
            r["rawg_rating"], r["metacritic_score"], r["average_rating"], r["value_score"],
        ))

    if not rows:
        logger.warning("Tidak ada record valid untuk di-insert ke fact_game_snapshot.")
        return

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO fact_game_snapshot
                (game_key, date_key, snapshot_date, price_usd, initial_price_usd,
                 discount_pct, owners_low, owners_high, concurrent_users,
                 steam_review_pct, steam_review_count, rawg_rating,
                 metacritic_score, average_rating, value_score)
            VALUES %s
            ON CONFLICT (game_key, snapshot_date) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                discount_pct = EXCLUDED.discount_pct,
                owners_low = EXCLUDED.owners_low,
                owners_high = EXCLUDED.owners_high,
                concurrent_users = EXCLUDED.concurrent_users,
                steam_review_pct = EXCLUDED.steam_review_pct,
                steam_review_count = EXCLUDED.steam_review_count,
                average_rating = EXCLUDED.average_rating,
                value_score = EXCLUDED.value_score
            """,
            rows,
        )
    conn.commit()
    logger.info(
        "Berhasil insert/update %d baris ke fact_game_snapshot (%d dilewati karena tidak ada game_key)",
        len(rows), skipped,
    )


# ==============================================================================
# 7. GOLD LAYER -- DATA MART 1: mart_game_ranking
# ==============================================================================

def build_mart_game_ranking(conn, snapshot_date: date) -> None:
    """
    Gold Mart 1: Ranking game berdasarkan Value Score.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_game_ranking (
                game_key            INTEGER,
                steam_appid         BIGINT,
                game_name           VARCHAR(500),
                developer           VARCHAR(255),
                publisher           VARCHAR(255),
                snapshot_date       DATE,

                price_usd           NUMERIC(10,2),
                discount_pct        NUMERIC(5,2),
                steam_review_pct    NUMERIC(5,2),
                steam_review_count  INT,

                rawg_rating         NUMERIC(3,2),
                metacritic_score    INT,
                average_rating      NUMERIC(5,2),
                value_score         NUMERIC(6,2),

                owners_low          BIGINT,
                owners_high         BIGINT,
                concurrent_users    INT,

                value_rank          INT,

                PRIMARY KEY (game_key, snapshot_date)
            );
        """)

        cur.execute("""
            INSERT INTO mart_game_ranking (
                game_key,
                steam_appid,
                game_name,
                developer,
                publisher,
                snapshot_date,
                price_usd,
                discount_pct,
                steam_review_pct,
                steam_review_count,
                rawg_rating,
                metacritic_score,
                average_rating,
                value_score,
                owners_low,
                owners_high,
                concurrent_users,
                value_rank
            )
            SELECT
                dg.game_key,
                dg.steam_appid,
                dg.game_name,
                dg.developer,
                dg.publisher,
                f.snapshot_date,

                f.price_usd,
                f.discount_pct,
                f.steam_review_pct,
                f.steam_review_count,

                f.rawg_rating,
                f.metacritic_score,
                f.average_rating,
                f.value_score,

                f.owners_low,
                f.owners_high,
                f.concurrent_users,

                RANK() OVER (
                    PARTITION BY f.snapshot_date
                    ORDER BY f.value_score DESC NULLS LAST
                ) AS value_rank

            FROM fact_game_snapshot f

            JOIN dim_game dg
                ON f.game_key = dg.game_key

            WHERE f.snapshot_date = %s

            ON CONFLICT (game_key, snapshot_date)
            DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                discount_pct = EXCLUDED.discount_pct,
                steam_review_pct = EXCLUDED.steam_review_pct,
                steam_review_count = EXCLUDED.steam_review_count,
                rawg_rating = EXCLUDED.rawg_rating,
                metacritic_score = EXCLUDED.metacritic_score,
                average_rating = EXCLUDED.average_rating,
                value_score = EXCLUDED.value_score,
                owners_low = EXCLUDED.owners_low,
                owners_high = EXCLUDED.owners_high,
                concurrent_users = EXCLUDED.concurrent_users,
                value_rank = EXCLUDED.value_rank;
        """, (snapshot_date,))
    conn.commit()
    logger.info("mart_game_ranking berhasil dibangun untuk %s", snapshot_date)


# ==============================================================================
# 8. GOLD -- DATA MART 2: mart_genre_analysis
# ==============================================================================

def build_mart_genre_analysis(conn, snapshot_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_genre_analysis (
                genre_key               INTEGER,
                genre_name              VARCHAR(100),
                snapshot_date           DATE,
                total_games             INT,
                avg_price_usd           NUMERIC(10,2),
                avg_steam_review_pct    NUMERIC(5,2),
                avg_rawg_rating         NUMERIC(3,2),
                avg_average_rating      NUMERIC(5,2),
                avg_value_score         NUMERIC(6,2),
                total_estimated_owners  BIGINT,
                PRIMARY KEY (genre_key, snapshot_date)
            );
        """)

        cur.execute("""
            INSERT INTO mart_genre_analysis (
                genre_key, genre_name, snapshot_date, total_games,
                avg_price_usd, avg_steam_review_pct, avg_rawg_rating,
                avg_average_rating, avg_value_score, total_estimated_owners
            )
            SELECT
                dgen.genre_key, dgen.genre_name, f.snapshot_date,
                COUNT(DISTINCT dg.game_key),
                ROUND(AVG(f.price_usd), 2),
                ROUND(AVG(f.steam_review_pct), 2),
                ROUND(AVG(f.rawg_rating), 2),
                ROUND(AVG(f.average_rating), 2),
                ROUND(AVG(f.value_score), 2),
                SUM(COALESCE(f.owners_low, 0) + COALESCE(f.owners_high, 0)) / 2
            FROM fact_game_snapshot f
            JOIN dim_game dg ON f.game_key = dg.game_key
            JOIN bridge_game_genre bgg ON dg.game_key = bgg.game_key
            JOIN dim_genre dgen ON bgg.genre_key = dgen.genre_key
            WHERE f.snapshot_date = %s
            GROUP BY dgen.genre_key, dgen.genre_name, f.snapshot_date
            ON CONFLICT (genre_key, snapshot_date) DO UPDATE SET
                total_games = EXCLUDED.total_games,
                avg_price_usd = EXCLUDED.avg_price_usd,
                avg_steam_review_pct = EXCLUDED.avg_steam_review_pct,
                avg_rawg_rating = EXCLUDED.avg_rawg_rating,
                avg_average_rating = EXCLUDED.avg_average_rating,
                avg_value_score = EXCLUDED.avg_value_score,
                total_estimated_owners = EXCLUDED.total_estimated_owners;
        """, (snapshot_date,))
    conn.commit()
    logger.info("mart_genre_analysis berhasil dibangun untuk %s", snapshot_date)


# ==============================================================================
# 9. GOLD -- DATA MART 3: mart_platform_analysis (Cleaned)
# ==============================================================================

def build_mart_platform_analysis(conn, snapshot_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_platform_analysis (
                platform_key            INTEGER,
                platform_name           VARCHAR(100),
                snapshot_date           DATE,
                total_games             INT,
                avg_price_usd           NUMERIC(10,2),
                PRIMARY KEY (platform_key, snapshot_date)
            );
        """)

        cur.execute("""
            INSERT INTO mart_platform_analysis (
                platform_key, platform_name, snapshot_date, total_games, avg_price_usd
            )
            SELECT
                dp.platform_key, dp.platform_name, f.snapshot_date,
                COUNT(DISTINCT dg.game_key),
                ROUND(AVG(f.price_usd), 2)
            FROM fact_game_snapshot f
            JOIN dim_game dg ON f.game_key = dg.game_key
            JOIN bridge_game_platform bgp ON dg.game_key = bgp.game_key
            JOIN dim_platform dp ON bgp.platform_key = dp.platform_key
            WHERE f.snapshot_date = %s
            GROUP BY dp.platform_key, dp.platform_name, f.snapshot_date
            ON CONFLICT (platform_key, snapshot_date) DO UPDATE SET
                total_games = EXCLUDED.total_games,
                avg_price_usd = EXCLUDED.avg_price_usd;
        """, (snapshot_date,))
    conn.commit()
    logger.info("mart_platform_analysis berhasil dibangun untuk %s", snapshot_date)


# ==============================================================================
# 10. GOLD -- DATA MART 4: mart_daily_summary (Renamed from mart_game_trend)
# ==============================================================================

def build_mart_daily_summary(conn, snapshot_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_daily_summary (
                snapshot_date           DATE PRIMARY KEY,
                total_games             INT,
                avg_price_usd           NUMERIC(10,2),
                avg_steam_review_pct    NUMERIC(5,2),
                avg_rawg_rating         NUMERIC(3,2),
                avg_average_rating      NUMERIC(5,2),
                avg_value_score         NUMERIC(6,2),
                total_estimated_owners  BIGINT,
                avg_concurrent_users    NUMERIC(12,2)
            );
        """)

        cur.execute("""
            INSERT INTO mart_daily_summary (
                snapshot_date, total_games, avg_price_usd, avg_steam_review_pct,
                avg_rawg_rating, avg_average_rating, avg_value_score,
                total_estimated_owners, avg_concurrent_users
            )
            SELECT
                f.snapshot_date,
                COUNT(DISTINCT f.game_key),
                ROUND(AVG(f.price_usd), 2),
                ROUND(AVG(f.steam_review_pct), 2),
                ROUND(AVG(f.rawg_rating), 2),
                ROUND(AVG(f.average_rating), 2),
                ROUND(AVG(f.value_score), 2),
                SUM(COALESCE(f.owners_low, 0) + COALESCE(f.owners_high, 0)) / 2,
                ROUND(AVG(f.concurrent_users), 2)
            FROM fact_game_snapshot f
            WHERE f.snapshot_date = %s
            GROUP BY f.snapshot_date
            ON CONFLICT (snapshot_date) DO UPDATE SET
                total_games = EXCLUDED.total_games,
                avg_price_usd = EXCLUDED.avg_price_usd,
                avg_steam_review_pct = EXCLUDED.avg_steam_review_pct,
                avg_rawg_rating = EXCLUDED.avg_rawg_rating,
                avg_average_rating = EXCLUDED.avg_average_rating,
                avg_value_score = EXCLUDED.avg_value_score,
                total_estimated_owners = EXCLUDED.total_estimated_owners,
                avg_concurrent_users = EXCLUDED.avg_concurrent_users;
        """, (snapshot_date,))
    conn.commit()
    logger.info("mart_daily_summary berhasil dibangun untuk %s", snapshot_date)


# ==============================================================================
# 11. GOLD -- DATA MART 5: mart_price_value_segment
# ==============================================================================

def build_mart_price_value_segment(conn, snapshot_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_price_value_segment (
                snapshot_date           DATE,
                price_segment           VARCHAR(20),
                value_segment           VARCHAR(20),
                total_games             INT,
                avg_price_usd           NUMERIC(10,2),
                avg_steam_review_pct    NUMERIC(5,2),
                avg_rawg_rating         NUMERIC(3,2),
                avg_value_score         NUMERIC(6,2),
                total_estimated_owners  BIGINT,
                PRIMARY KEY (snapshot_date, price_segment, value_segment)
            );
        """)

        cur.execute("""
            INSERT INTO mart_price_value_segment (
                snapshot_date, price_segment, value_segment, total_games,
                avg_price_usd, avg_steam_review_pct, avg_rawg_rating,
                avg_value_score, total_estimated_owners
            )
            SELECT
                f.snapshot_date,
                CASE 
                    WHEN f.price_usd IS NULL THEN 'Unknown'
                    WHEN f.price_usd = 0 THEN 'Free'
                    WHEN f.price_usd <= 10 THEN 'Budget'
                    WHEN f.price_usd <= 30 THEN 'Mid-range'
                    WHEN f.price_usd <= 60 THEN 'Premium'
                    ELSE 'High-end'
                END AS price_segment,
                CASE 
                    WHEN f.value_score IS NULL THEN 'Unknown'
                    WHEN f.value_score < 60 THEN 'Low'
                    WHEN f.value_score < 80 THEN 'Medium'
                    ELSE 'High'
                END AS value_segment,
                COUNT(DISTINCT f.game_key),
                ROUND(AVG(f.price_usd), 2),
                ROUND(AVG(f.steam_review_pct), 2),
                ROUND(AVG(f.rawg_rating), 2),
                ROUND(AVG(f.value_score), 2),
                SUM(COALESCE(f.owners_low, 0) + COALESCE(f.owners_high, 0)) / 2
            FROM fact_game_snapshot f
            WHERE f.snapshot_date = %s
            GROUP BY
                f.snapshot_date,
                CASE 
                    WHEN f.price_usd IS NULL THEN 'Unknown'
                    WHEN f.price_usd = 0 THEN 'Free'
                    WHEN f.price_usd <= 10 THEN 'Budget'
                    WHEN f.price_usd <= 30 THEN 'Mid-range'
                    WHEN f.price_usd <= 60 THEN 'Premium'
                    ELSE 'High-end'
                END,
                CASE 
                    WHEN f.value_score IS NULL THEN 'Unknown'
                    WHEN f.value_score < 60 THEN 'Low'
                    WHEN f.value_score < 80 THEN 'Medium'
                    ELSE 'High'
                END
            ON CONFLICT (snapshot_date, price_segment, value_segment) DO UPDATE SET
                total_games = EXCLUDED.total_games,
                avg_price_usd = EXCLUDED.avg_price_usd,
                avg_steam_review_pct = EXCLUDED.avg_steam_review_pct,
                avg_rawg_rating = EXCLUDED.avg_rawg_rating,
                avg_value_score = EXCLUDED.avg_value_score,
                total_estimated_owners = EXCLUDED.total_estimated_owners;
        """, (snapshot_date,))
    conn.commit()
    logger.info("mart_price_value_segment berhasil dibangun untuk %s", snapshot_date)


# ==============================================================================
# 12. BUILD ALL GOLD MARTS
# ==============================================================================

def build_all_gold_marts(conn, snapshot_date: date) -> None:
    """
    Menjalankan seluruh proses pembuatan Gold Data Mart.
    """
    logger.info("Memulai pembangunan seluruh Gold Data Mart untuk %s", snapshot_date)
    
    build_mart_game_ranking(conn, snapshot_date)
    build_mart_genre_analysis(conn, snapshot_date)
    build_mart_platform_analysis(conn, snapshot_date)
    build_mart_daily_summary(conn, snapshot_date)
    build_mart_price_value_segment(conn, snapshot_date)
    
    logger.info("Seluruh Gold Data Mart berhasil dibangun untuk %s", snapshot_date)