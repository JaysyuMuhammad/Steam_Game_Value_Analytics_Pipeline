"""
src/extract/steamspy.py

Modul untuk menarik data dari SteamSpy API.
SteamSpy berperan sebagai PRIMARY SOURCE -- data yang berubah tiap hari
(harga, concurrent users, estimasi owners, review count).
"""

import time
import logging

import requests

logger = logging.getLogger(__name__)

STEAMSPY_BASE_URL = "https://steamspy.com/api.php"


def fetch_all_paginated(max_pages: int = 1, delay_seconds: float = 61.0) -> list[dict]:
    """
    Tarik semua data game dari SteamSpy API, 
    max_pages: jumlah page yang mau ditarik (1 page = 1000 game)
    delay_seconds: jeda antar request page, untuk menghindari rate limit
    Return: list of dict, setiap dict = 1 game (raw dari SteamSpy)
    """

    all_games: list[dict] = []
    for page in range(max_pages):
        params = {"request": "all", "page": page}
        response = _get_with_retry(STEAMSPY_BASE_URL, params, retries=3)
        data = response.json()  # bentuk: {"appid1": {...}, "appid2": {...}, ...}

        if not data:
            logger.info("Page %d kosong, berhenti pagination.", page)
            break

        page_items = list(data.values())  # jadi list of dict, tanpa transformasi Pandas
        all_games.extend(page_items)
        logger.info("Page %d: %d game ditarik (top %d-%d by owners)",
                     page, len(page_items), page * 1000, page * 1000 + len(page_items) - 1)

        if page < max_pages - 1:
            logger.info("Menunggu %.0f detik sebelum request page berikutnya (rate limit 'all')...", delay_seconds)
            time.sleep(delay_seconds)

    logger.info("Total game ditarik via pagination: %d", len(all_games))
    return all_games


def _get_with_retry(url: str, params: dict, retries: int) -> requests.Response:
    """
    Wrapper request GET dengan retry sederhana + exponential backoff.
    Menangani kasus rate limit (429) atau error server sementara (5xx).
    """
    last_exception = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            last_exception = e
            wait = 2 ** attempt
            logger.warning(
                "Request gagal (percobaan %d/%d): %s. Menunggu %ds sebelum retry.",
                attempt, retries, e, wait,
            )
            time.sleep(wait)

    raise RuntimeError(f"Request ke {url} gagal setelah {retries} percobaan") from last_exception