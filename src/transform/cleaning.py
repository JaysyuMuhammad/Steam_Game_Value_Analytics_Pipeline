"""
src/transform/cleaning.py

Berisi fungsi untuk mengolah data SteamSpy menjadi data snapshot
yang siap disimpan ke fact_game_snapshot.

Data SteamSpy digunakan untuk mengambil informasi yang berubah,
seperti harga, diskon, jumlah owners, concurrent users, dan review.

Informasi rating RAWG dan Metacritic diambil dari game_lookup.
Data tersebut sebelumnya sudah disimpan di database melalui proses
matching antara SteamSpy dan RAWG.

Modul ini hanya melakukan transformasi data.
Modul tidak memanggil API dan tidak membuat koneksi ke database.
"""

import re
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


def parse_owners(owners_str: str) -> tuple[Optional[int], Optional[int]]:
    """
    Parsing string range owners SteamSpy, contoh:
        "20,000,000 .. 50,000,000" -> (20000000, 50000000)
    """
    if not owners_str:
        return None, None
    match = re.match(r"([\d,]+)\s*\.\.\s*([\d,]+)", owners_str)
    if not match:
        return None, None
    low = int(match.group(1).replace(",", ""))
    high = int(match.group(2).replace(",", ""))
    return low, high


def parse_price_cents(cents_value) -> Optional[float]:
    """SteamSpy menyimpan harga dalam SEN (misal '999' = $9.99)."""
    if cents_value is None or cents_value == "":
        return None
    try:
        return round(int(cents_value) / 100, 2)
    except (ValueError, TypeError):
        return None


def compute_review_stats(positive, negative) -> tuple[Optional[float], int]:
    """
    Hitung persentase review positif & total review count.
    Menangani kasus positive=negative=0 (division by zero) dengan
    mengembalikan None untuk persentase.
    """
    positive = int(positive or 0)
    negative = int(negative or 0)
    total = positive + negative

    if total == 0:
        return None, 0

    pct = round(positive / total * 100, 2)
    return pct, total


def compute_average_rating(steam_review_pct: Optional[float], rawg_rating: Optional[float]) -> Optional[float]:
    """
    Menghitung nilai rata-rata rating dari review Steam dan rating RAWG.

    Persentase review Steam menggunakan skala 0 sampai 100.
    Rating RAWG dikonversi dari skala 0 sampai 5 menjadi skala 0 sampai 100.

    Jika salah satu nilai tersedia, fungsi menggunakan nilai tersebut.
    Jika keduanya tidak tersedia, fungsi mengembalikan None.
    """
    values = []
    if steam_review_pct is not None:
        values.append(float(steam_review_pct))
    if rawg_rating is not None:
        values.append(float(rawg_rating) * 20)  # skala 0-5 -> 0-100

    if not values:
        return None
    return round(sum(values) / len(values), 2)


# Harga acuan untuk normalisasi price_component.
MAX_PRICE_REFERENCE = 70.0


def compute_value_score(steam_review_pct: Optional[float], rawg_rating: Optional[float],
                         price_usd: Optional[float]) -> Optional[float]:
    """
    Menghitung value score berdasarkan tiga komponen:

    - Persentase review Steam memiliki bobot 50%.
    - Rating RAWG memiliki bobot 30%.
    - Harga game memiliki bobot 20%.

    Persentase review dibagi 100 agar berada pada skala 0 sampai 1.
    Rating RAWG dibagi 5 karena menggunakan skala 0 sampai 5.

    Komponen harga menggunakan batas acuan sebesar 70 USD:

        price_component = max(
            0,
            1 - price_usd / MAX_PRICE_REFERENCE
        )

    Game gratis memiliki nilai komponen harga sebesar 1.
    Game dengan harga 70 USD atau lebih memiliki nilai 0.

    Jika salah satu data tidak tersedia, nilainya dianggap 0.
    Perhitungan tetap dilakukan agar game tersebut masih memiliki
    value score berdasarkan data yang tersedia.
"""
    review_component = (float(steam_review_pct) / 100) if steam_review_pct is not None else 0
    rating_component = (float(rawg_rating) / 5) if rawg_rating is not None else 0

    if price_usd is None:
        price_component = 0
    else:
        price_component = max(0, 1 - (float(price_usd) / MAX_PRICE_REFERENCE))

    score = (review_component * 0.5) + (rating_component * 0.3) + (price_component * 0.2)
    return round(score * 100, 2)


def extract_genre_names(genres_field) -> list[str]:
    """RAWG genres field: list of dict [{"id":.., "name": "Action"}, ...]."""
    if not isinstance(genres_field, list):
        return []
    return [g["name"] for g in genres_field if "name" in g]


def extract_platform_names(platforms_field) -> list[str]:
    """RAWG platforms field: list of dict [{"platform": {"name": "PC"}}, ...]."""
    if not isinstance(platforms_field, list):
        return []
    names = []
    for p in platforms_field:
        platform_info = p.get("platform", {})
        if "name" in platform_info:
            names.append(platform_info["name"])
    return names


def build_snapshot_record(steamspy_game: dict, game_info: dict, snapshot_date: date) -> dict:
    """
    Membuat satu record snapshot untuk sebuah game.

    Data diambil dari SteamSpy dan digabungkan dengan informasi
    rating RAWG serta Metacritic yang sudah tersimpan di database.
    Hasilnya berisi harga, diskon, owners, concurrent users,
    data review, average rating, dan value score.

    steamspy_game berisi data SteamSpy untuk satu game.
    game_info berisi game_key, rating RAWG, dan skor Metacritic.
    snapshot_date menunjukkan tanggal pengambilan data.
    """
    appid = int(steamspy_game["appid"])
    owners_low, owners_high = parse_owners(steamspy_game.get("owners"))
    price_usd = parse_price_cents(steamspy_game.get("price"))
    initial_price_usd = parse_price_cents(steamspy_game.get("initialprice"))
    review_pct, review_count = compute_review_stats(
        steamspy_game.get("positive"), steamspy_game.get("negative")
    )

    rawg_rating = game_info.get("rawg_rating")
    metacritic = game_info.get("metacritic")

    average_rating = compute_average_rating(review_pct, rawg_rating)
    value_score = compute_value_score(review_pct, rawg_rating, price_usd)

    return {
        "steam_appid": appid,
        "snapshot_date": snapshot_date.isoformat(),
        "price_usd": price_usd,
        "initial_price_usd": initial_price_usd,
        "discount_pct": steamspy_game.get("discount"),
        "owners_low": owners_low,
        "owners_high": owners_high,
        "concurrent_users": steamspy_game.get("ccu"),
        "steam_review_pct": review_pct,
        "steam_review_count": review_count,
        "rawg_rating": rawg_rating,
        "metacritic_score": metacritic,
        "average_rating": average_rating,
        "value_score": value_score,
    }


def build_silver_dataset(steamspy_games: list[dict], game_lookup: dict[int, dict],
                         snapshot_date: date) -> list[dict]:
    """
    Mengolah data game SteamSpy untuk tanggal tertentu.

    Data yang dihasilkan berisi informasi harga, jumlah pemilik,
    jumlah pengguna, review, rating, dan value score. Hasilnya
    digunakan untuk mengisi tabel fact_game_snapshot.

    game_lookup berisi informasi game yang sudah tersimpan di database,
    termasuk game_key, rating RAWG, dan skor Metacritic.
    """
    records = []
    skipped = 0

    for game in steamspy_games:
        appid = int(game["appid"])
        game_info = game_lookup.get(appid)

        if game_info is None:
            logger.warning("appid %s tidak ditemukan di game_lookup, dilewati", appid)
            skipped += 1
            continue

        record = build_snapshot_record(game, game_info, snapshot_date)
        records.append(record)

    logger.info(
        "build_silver_dataset selesai: %d record berhasil diproses, %d dilewati",
        len(records), skipped,
    )
    return records