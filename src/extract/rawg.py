"""
src/extract/rawg.py

Modul untuk menarik data dari RAWG API.

RAWG berperan sebagai ENRICHMENT SOURCE:
- rating
- metacritic
- genre
- platform
- release date

RAWG hanya dipanggil untuk game yang belum memiliki mapping SteamSpy.
"""

import os
import time
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

RAWG_BASE_URL = "https://api.rawg.io/api"


def _get_api_key() -> str:
    """
    Mengambil RAWG API key dari environment variable.
    """
    api_key = os.environ.get("RAWG_API_KEY")

    if not api_key:
        raise RuntimeError(
            "RAWG_API_KEY tidak ditemukan di environment variable. "
            "Pastikan sudah diset lewat .env / Airflow Variable."
        )

    return api_key


def search_top_n(
    game_name: str,
    n: int = 5,
    retries: int = 3
) -> list[dict]:
    """
    Cari game berdasarkan nama dan mengambil beberapa kandidat.

    Kandidat-kandidat ini nantinya divalidasi oleh matching.py
    menggunakan similarity score.
    """
    api_key = _get_api_key()

    params = {
        "key": api_key,
        "search": game_name,
        "page_size": n
    }

    response = _get_with_retry(
        f"{RAWG_BASE_URL}/games",
        params,
        retries=retries
    )

    result = response.json()

    return result.get("results", [])


def fetch_rawg_for_unmapped(
    unmapped_games: list[dict],
    n_candidates: int = 5,
    delay_seconds: float = 0.3
) -> dict[str, list[dict]]:

    raw_responses: dict[str, list[dict]] = {}

    total = len(unmapped_games)

    for i, game in enumerate(unmapped_games, start=1):

        appid = int(game["appid"])
        game_name = game.get("name", "")

        try:
            candidates = search_top_n(
                game_name,
                n=n_candidates
            )

            raw_responses[str(appid)] = candidates
            candidate_names = [c.get("name", "Unknown") for c in candidates]
            logger.info(
                "[%d/%d] RAWG: %s (AppID %d) -> %d kandidat ditemukan: %s",
                i,
                total,
                game_name,
                appid,
                len(candidates),
                candidate_names
            )

        except Exception as e:

            logger.error(
                "Gagal mengambil RAWG untuk %s (AppID %d): %s",
                game_name,
                appid,
                e
            )
            raw_responses[str(appid)] = []

        time.sleep(delay_seconds)

    logger.info(
        "RAWG fetch selesai: %d game diproses.",
        total
    )

    return raw_responses


def _get_with_retry(
    url: str,
    params: dict,
    retries: int = 3
) -> requests.Response:
    """
    Request GET dengan retry sederhana + exponential backoff.
    """

    last_exception = None

    for attempt in range(1, retries + 1):

        try:

            response = requests.get(
                url,
                params=params,
                timeout=15
            )

            response.raise_for_status()

            return response

        except requests.exceptions.RequestException as e:

            last_exception = e

            wait = 2 ** attempt

            logger.warning(
                "Request RAWG gagal "
                "(percobaan %d/%d): %s. "
                "Menunggu %ds sebelum retry.",
                attempt,
                retries,
                e,
                wait
            )

            time.sleep(wait)

    raise RuntimeError(
        f"Request ke {url} gagal setelah {retries} percobaan"
    ) from last_exception