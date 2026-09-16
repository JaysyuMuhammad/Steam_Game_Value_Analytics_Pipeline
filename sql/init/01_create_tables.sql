-- ==============================================================================
-- Steam Game Value Analytics - Data Warehouse DDL
-- ==============================================================================
-- File ini otomatis dijalankan oleh Postgres saat container postgres-warehouse
-- pertama kali dibuat (volume kosong). Kalau volume sudah ada isinya, file ini
-- TIDAK akan otomatis jalan lagi -- jalankan manual lewat psql kalau perlu.
-- ==============================================================================


-- ------------------------------------------------------------------------------
-- game_source_mapping
-- Audit trail hasil entity resolution SteamSpy <-> RAWG.
-- Berfungsi sebagai "cache" -- begitu appid punya mapping, tidak perlu
-- fuzzy matching ulang di batch berikutnya. Nilai rawg_rating & metacritic
-- juga di-cache di sini (bukan di dim_game) karena keduanya konseptual
-- adalah hasil enrichment RAWG, bukan identitas game -- dipakai ulang
-- setiap hari untuk mengisi fact_game_snapshot TANPA perlu memanggil
-- RAWG API lagi.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS game_source_mapping (
    mapping_id          SERIAL PRIMARY KEY,
    steam_appid         BIGINT NOT NULL UNIQUE,
    rawg_id             BIGINT,
    steam_name          VARCHAR(500) NOT NULL,
    rawg_name           VARCHAR(500),
    match_status        VARCHAR(20) NOT NULL CHECK (match_status IN ('exact', 'high_confidence', 'review', 'unmatched')),
    similarity_score    NUMERIC(5,2),
    rawg_rating         NUMERIC(3,2),
    metacritic          INT,
    matched_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mapping_appid ON game_source_mapping (steam_appid);
CREATE INDEX IF NOT EXISTS idx_mapping_status ON game_source_mapping (match_status);


-- ------------------------------------------------------------------------------
-- dim_game
-- Informasi identitas game -- relatif jarang berubah. MURNI identitas,
-- tidak menyimpan nilai enrichment RAWG (lihat game_source_mapping).
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_game (
    game_key            SERIAL PRIMARY KEY,
    steam_appid         BIGINT NOT NULL UNIQUE,
    rawg_id             BIGINT,
    game_name           VARCHAR(500) NOT NULL,
    developer           VARCHAR(255),
    publisher           VARCHAR(255),
    release_date        DATE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_game_appid ON dim_game (steam_appid);


-- ------------------------------------------------------------------------------
-- dim_genre + bridge_game_genre
-- Genre bersifat many-to-many terhadap game.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key            SERIAL PRIMARY KEY,
    genre_name           VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS bridge_game_genre (
    game_key             INT NOT NULL REFERENCES dim_game (game_key),
    genre_key            INT NOT NULL REFERENCES dim_genre (genre_key),
    PRIMARY KEY (game_key, genre_key)
);


-- ------------------------------------------------------------------------------
-- dim_platform + bridge_game_platform
-- Platform juga many-to-many terhadap game.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_key         SERIAL PRIMARY KEY,
    platform_name        VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS bridge_game_platform (
    game_key             INT NOT NULL REFERENCES dim_game (game_key),
    platform_key         INT NOT NULL REFERENCES dim_platform (platform_key),
    PRIMARY KEY (game_key, platform_key)
);


-- ------------------------------------------------------------------------------
-- dim_date
-- Dimension tanggal standar, memudahkan agregasi per minggu/bulan/tahun
-- di layer BI/dashboard nanti.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key             INT PRIMARY KEY,        -- format YYYYMMDD
    full_date            DATE NOT NULL UNIQUE,
    day_of_week          VARCHAR(10),
    week_of_year         INT,
    month                INT,
    month_name           VARCHAR(15),
    quarter              INT,
    year                 INT
);


-- ------------------------------------------------------------------------------
-- fact_game_snapshot
-- Grain: 1 row = 1 game per snapshot_date.
-- Berisi data yang BERUBAH setiap waktu (harga, ccu, owners, review, rating).
--
-- Tabel ini di-PARTITION BY RANGE (snapshot_date), partisi bulanan.
-- Alasan: sesuai guideline bootcamp yang meminta "tabel yang efektif dan
-- efisien dalam penyimpanan dan pengolahan data (Partition & Clustering)".
-- Partisi bulanan memudahkan query per periode & pruning otomatis oleh Postgres.
--
-- PENTING: Postgres MEWAJIBKAN kolom partisi (snapshot_date) ikut menjadi
-- bagian dari PRIMARY KEY dan setiap UNIQUE constraint di tabel ini.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_game_snapshot (
    snapshot_id          BIGSERIAL,
    game_key             INT NOT NULL REFERENCES dim_game (game_key),
    date_key             INT NOT NULL REFERENCES dim_date (date_key),
    snapshot_date         DATE NOT NULL,

    -- dari SteamSpy (berubah tiap hari)
    price_usd             NUMERIC(10,2),
    initial_price_usd      NUMERIC(10,2),
    discount_pct           NUMERIC(5,2),
    owners_low             BIGINT,
    owners_high            BIGINT,
    concurrent_users        INT,
    steam_review_pct        NUMERIC(5,2),
    steam_review_count      INT,

    -- dari RAWG (jarang berubah, tapi ikut tersimpan per snapshot untuk historical tracking)
    rawg_rating             NUMERIC(3,2),
    metacritic_score         INT,

    -- metrik turunan
    -- average_rating: gabungan steam_review_pct & rawg_rating (skala 0-100),
    -- pelengkap untuk dashboard yang butuh 1 angka ringkas -- steam_review_pct
    -- dan rawg_rating TETAP disimpan terpisah supaya insight "disukai
    -- pemain Steam" vs "dinilai bagus multi-platform (RAWG)" tidak hilang.
    average_rating            NUMERIC(5,2),
    value_score              NUMERIC(6,2),

    created_at               TIMESTAMP NOT NULL DEFAULT NOW(),

    -- kolom partisi (snapshot_date) WAJIB ikut di primary key & unique constraint
    PRIMARY KEY (snapshot_id, snapshot_date),
    UNIQUE (game_key, snapshot_date)
) PARTITION BY RANGE (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_fact_snapshot_date ON fact_game_snapshot (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_fact_game_key ON fact_game_snapshot (game_key);


-- ------------------------------------------------------------------------------
-- Partisi bulanan untuk fact_game_snapshot
--
-- Partisi TIDAK dibuat manual di sini. Sebagai gantinya, Airflow akan
-- menjalankan task "ensure_partition_exists" sebelum proses load, yang
-- otomatis membuat partisi bulan berjalan kalau belum ada
-- (CREATE TABLE IF NOT EXISTS ... PARTITION OF fact_game_snapshot ...).
--
-- Ini menghindari kebutuhan mengelola partisi secara manual setiap bulan.
-- ------------------------------------------------------------------------------


-- ==============================================================================
-- Selesai. Struktur ini merepresentasikan star schema:
--   dim_game, dim_genre, dim_platform, dim_date  -> dimension tables
--   fact_game_snapshot (PARTITIONED by month)     -> fact table (grain: game per hari)
--   game_source_mapping                           -> audit trail entity resolution
--
-- Partisi bulan berjalan dibuat OTOMATIS oleh Airflow task
-- "ensure_partition_exists" sebelum proses load -- tidak perlu dikelola manual.
-- ==============================================================================