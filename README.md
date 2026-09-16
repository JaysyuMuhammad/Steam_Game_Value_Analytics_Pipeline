# Steam Game Value Analytics

Pipeline data engineering untuk mengambil data game dari SteamSpy, memperkaya data dengan RAWG, menghitung metrik nilai game, dan menyajikan hasil analisis melalui data warehouse PostgreSQL serta Metabase.

Proyek ini menggunakan Apache Airflow untuk orkestrasi, MinIO sebagai penyimpanan Bronze, dan PostgreSQL sebagai data warehouse dengan pola medallion architecture.

## Tujuan

Pipeline ini menghasilkan data yang dapat digunakan untuk menganalisis:

- harga dan diskon game;
- estimasi jumlah pemilik game;
- concurrent users;
- persentase dan jumlah review Steam;
- rating RAWG dan skor Metacritic;
- average rating;
- value score;
- ranking game berdasarkan value score;
- analisis berdasarkan genre dan platform;
- ringkasan harian serta segmentasi harga dan nilai.

## Arsitektur


### Layer data

| Layer | Teknologi | Isi |
| --- | --- | --- |
| Bronze | MinIO | Respons JSON mentah dari SteamSpy dan RAWG berdasarkan tanggal |
| Silver | PostgreSQL | Hasil matching, dimension tables, dan snapshot game yang sudah dibersihkan |
| Gold | PostgreSQL | Data mart yang siap digunakan oleh Metabase |

## Alur Pipeline

DAG `steam_analytics_daily` dijadwalkan setiap hari pukul 02:00 dengan cron `0 2 * * *`.

1. **Extract SteamSpy ke Bronze**
   - Mengambil maksimal tiga halaman data SteamSpy.
   - Setiap halaman berisi sekitar 1.000 game.
   - Data disimpan ke MinIO dengan pola `bronze/steamspy/YYYY-MM-DD/data.json`.
   - Jika data untuk tanggal tersebut sudah ada, API tidak dipanggil ulang.

2. **Extract RAWG untuk game yang belum memiliki mapping**
   - Membaca data SteamSpy Bronze.
   - Membaca AppID yang sudah tersimpan di `game_source_mapping`.
   - Hanya game yang belum memiliki mapping yang dikirim ke RAWG.
   - RAWG mengembalikan beberapa kandidat untuk setiap game.
   - Jika tidak ada game baru, pipeline tetap membuat file RAWG kosong agar proses berikutnya tetap dapat berjalan.

3. **Matching dan pengisian dimension tables**
   - Kandidat RAWG dicocokkan dengan nama game SteamSpy menggunakan fuzzy matching.
   - Hasil pencocokan disimpan di `game_source_mapping` sebagai audit trail dan cache enrichment.
   - Mapping dengan status `exact` atau `high_confidence` dimasukkan ke `dim_game`.
   - Genre dan platform game reliable disimpan ke dimension serta bridge table masing-masing.
   - Mapping berstatus `review` atau `unmatched` tetap dicatat, tetapi tidak otomatis dimasukkan ke `dim_game`.

4. **Cleaning dan pengisian fact table**
   - Harga SteamSpy dikonversi dari sen ke USD.
   - Rentang owners dipisahkan menjadi nilai minimum dan maksimum.
   - Review positif dan negatif dihitung menjadi persentase review positif serta total review.
   - Data SteamSpy digabungkan dengan rating RAWG dan skor Metacritic dari mapping yang sudah tersimpan.
   - Hasilnya disimpan ke `fact_game_snapshot` dengan grain satu game untuk satu tanggal.
   - Partisi bulanan dibuat otomatis sebelum proses insert.
   - Jika snapshot tanggal yang sama diproses ulang, data game tersebut di-update melalui conflict handling.

5. **Build Gold Data Marts**
   - Seluruh data mart dibangun setelah fact table selesai diproses.
   - Data mart kemudian dapat dihubungkan ke Metabase untuk membuat dashboard dan analisis.

Mapping game baru dan snapshot harian merupakan dua proses yang berbeda. Tidak adanya game baru hanya membuat proses matching dan pengisian mapping dilewati; snapshot harga, review, owners, dan metrik harian tetap diproses.

## Rumus Metrik

### Average rating

Rating RAWG dikonversi dari skala 0-5 menjadi 0-100, kemudian dirata-ratakan dengan persentase review positif Steam yang tersedia.

```text
rawg_rating_scaled = rawg_rating * 20
average_rating = rata-rata nilai yang tersedia
```

Jika hanya satu sumber yang tersedia, nilai tersebut digunakan. Jika keduanya tidak tersedia, hasilnya `NULL`.

### Value score

Value score memakai tiga komponen berikut:

```text
review_component = steam_review_pct / 100
rating_component = rawg_rating / 5
price_component = max(0, 1 - price_usd / 70)

value_score = (
    review_component * 0.50 +
    rating_component * 0.30 +
    price_component * 0.20
) * 100
```

Bobot review adalah 50%, rating RAWG 30%, dan harga 20%. Game gratis mendapat nilai komponen harga 1, sedangkan game dengan harga 70 USD atau lebih mendapat nilai 0. Data komponen yang tidak tersedia dianggap 0.

## Struktur Proyek

```text
final project/
├── airflow/
│   ├── dags/steam_analytics.py       # DAG dan orkestrasi pipeline
│   ├── logs/                          # Log Airflow
│   └── plugins/                       # Plugin Airflow
├── sql/init/01_create_tables.sql     # DDL data warehouse
├── src/
│   ├── extract/
│   │   ├── rawg.py                    # Extract data enrichment RAWG
│   │   └── steamspy.py                # Extract data utama SteamSpy
│   ├── load/
│   │   ├── minio_client.py            # Baca/tulis object MinIO
│   │   └── postgres.py                # Operasi warehouse dan data mart
│   └── transform/
│       ├── cleaning.py                # Cleaning dan perhitungan metrik
│       └── matching.py                # Fuzzy matching SteamSpy-RAWG
├── docker-compose.yaml
├── requirement.txt
└── .env                               # Konfigurasi lokal, jangan di-commit
```

## Prasyarat

- Docker Desktop dengan Docker Compose.
- RAWG API key.
- Akses internet untuk SteamSpy API dan RAWG API.
- Port berikut tersedia: `3000`, `5433`, `5434`, `8080`, `9000`, dan `9001`.

## Menjalankan Proyek

Dari folder `
Steam_Game_Value_Analytics_Pipeline` ini, jalankan:

```bash
docker compose up -d
```

Periksa status container:

```bash
docker compose ps
```

Buka layanan berikut:

| Layanan | URL | Keterangan |
| --- | --- | --- |
| Airflow | http://localhost:8080 | Orkestrasi dan monitoring DAG |
| Metabase | http://localhost:3000 | Dashboard dan visualisasi |
| MinIO Console | http://localhost:9001 | Pemeriksaan object storage |
| PostgreSQL warehouse | `localhost:5434` | Host database untuk BI dan analitik |
| PostgreSQL Airflow | `localhost:5433` | Metadata database Airflow |

User awal Airflow yang dibuat oleh Compose:

```text
username: admin
password: admin
```

Setelah Airflow aktif, buka DAG `steam_analytics_daily` dan aktifkan DAG tersebut jika ingin menjalankannya sesuai jadwal. DAG juga dapat dijalankan secara manual dari Airflow UI.

Untuk melihat log:

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

Untuk menghentikan service:

```bash
docker compose down
```

Perintah tersebut tidak menghapus named volume. Data dapat dihapus dengan `docker compose down -v`, tetapi tindakan ini akan menghapus data PostgreSQL, MinIO, dan Metabase.

## Skema Warehouse

Warehouse menggunakan star schema dengan tabel utama berikut:

- `game_source_mapping`: hasil entity resolution SteamSpy dan RAWG serta cache rating RAWG dan Metacritic.
- `dim_game`: identitas game yang memiliki mapping reliable.
- `dim_genre` dan `bridge_game_genre`: genre game.
- `dim_platform` dan `bridge_game_platform`: platform game.
- `dim_date`: atribut kalender untuk kebutuhan agregasi waktu.
- `fact_game_snapshot`: metrik game yang berubah dari waktu ke waktu, dipartisi berdasarkan bulan.

Gold data marts yang dibuat oleh pipeline:

- `mart_game_ranking`
- `mart_genre_analysis`
- `mart_platform_analysis`
- `mart_daily_summary`
- `mart_price_value_segment`

## Catatan Keamanan dan Operasional

- Jangan commit file `.env` ke GitHub.
- API key RAWG dan password yang pernah terekspos perlu diganti sebelum repository dipublikasikan.
- Jangan menggunakan kredensial default `admin/admin` pada lingkungan produksi.
- File SQL inisialisasi PostgreSQL hanya otomatis dijalankan saat volume database masih kosong.
- SteamSpy memiliki jeda antar-request untuk mengurangi risiko rate limit. Pengambilan tiga halaman dapat membutuhkan waktu beberapa menit.

## Teknologi

- Python
- Apache Airflow 2.9.3
- PostgreSQL 15
- MinIO
- Metabase
- Docker Compose
- `requests`
- `pandas`
- `thefuzz`
- `boto3`
- `psycopg2-binary`
