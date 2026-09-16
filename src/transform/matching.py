"""
src/transform/matching.py

Modul untuk:
1. Mendeteksi game yang belum punya mapping RAWG.
2. Melakukan entity resolution untuk game baru.
3. Menentukan match_status berdasarkan similarity score.

Matching menggunakan RAWG RAW response yang sudah disimpan
di MinIO Bronze.
"""

import re
import logging
from typing import Optional

from thefuzz import fuzz

logger = logging.getLogger(__name__)


THRESHOLD_HIGH_CONFIDENCE = 85
THRESHOLD_REVIEW = 70


def normalize_name(name: str) -> str:
    """
    Normalisasi nama game:
    lowercase, hapus tanda baca, dan rapikan spasi.
    """

    if not name:
        return ""

    name = name.lower()

    name = re.sub(
        r"[^a-z0-9\s]",
        "",
        name
    )

    name = re.sub(
        r"\s+",
        " ",
        name
    ).strip()

    return name


def get_unmapped_games(
    steamspy_games: list[dict],
    existing_appids: set[int]
) -> list[dict]:
    """
    Mengambil game SteamSpy yang belum memiliki mapping.

    Game yang sudah pernah ditemukan sebelumnya tidak perlu
    dicari kembali ke RAWG.
    """

    unmapped = [
        game
        for game in steamspy_games
        if int(game["appid"]) not in existing_appids
    ]

    logger.info(
        "Deteksi game baru: %d dari %d game hari ini belum punya mapping",
        len(unmapped),
        len(steamspy_games)
    )

    return unmapped


def classify_match_status(
    similarity_score: int,
    steam_name_clean: str,
    rawg_name_clean: str
) -> str:

    if similarity_score == 100:
        return "exact"

    if similarity_score >= THRESHOLD_HIGH_CONFIDENCE:
        return "high_confidence"

    if rawg_name_clean and steam_name_clean:

        if (
            rawg_name_clean in steam_name_clean
            or steam_name_clean in rawg_name_clean
        ):
            return "high_confidence"

    if similarity_score >= THRESHOLD_REVIEW:
        return "review"

    return "unmatched"


def find_best_candidate(
    steam_name: str,
    candidates: list[dict]
) -> tuple[Optional[dict], int]:
    """
    Memilih kandidat RAWG dengan similarity score tertinggi.
    """

    if not candidates:
        return None, 0

    steam_clean = normalize_name(steam_name)

    best_candidate = None
    best_score = 0

    for candidate in candidates:

        candidate_clean = normalize_name(
            candidate.get("name", "")
        )

        score = fuzz.token_sort_ratio(
            steam_clean,
            candidate_clean
        )

        if score > best_score:

            best_score = score
            best_candidate = candidate

    return best_candidate, best_score


def match_new_games_from_bronze(
    unmapped_games: list[dict],
    rawg_raw_responses: dict[str, list[dict]]
) -> list[dict]:
    """
    Melakukan entity resolution menggunakan RAWG response
    yang sudah disimpan di MinIO Bronze.

    TIDAK memanggil API.
    """

    results = []

    for game in unmapped_games:

        appid = int(game["appid"])
        steam_name = game.get("name", "")

        rawg_data = rawg_raw_responses.get(
            str(appid),
            []
        )

        if not rawg_data:

            results.append({
                "steam_appid": appid,
                "steam_name": steam_name,
                "rawg_id": None,
                "rawg_name": None,
                "match_status": "unmatched",
                "similarity_score": 0,
                "rawg_rating": None,
                "rawg_metacritic": None,
                "rawg_released": None,
                "rawg_genres": [],
                "rawg_platforms": [],
            })

            continue

        candidates = rawg_data

        best_candidate, score = find_best_candidate(
            steam_name,
            candidates
        )

        if not best_candidate:

            results.append({
                "steam_appid": appid,
                "steam_name": steam_name,
                "rawg_id": None,
                "rawg_name": None,
                "match_status": "unmatched",
                "similarity_score": 0,
                "rawg_rating": None,
                "rawg_metacritic": None,
                "rawg_released": None,
                "rawg_genres": [],
                "rawg_platforms": [],
            })

            continue

        steam_clean = normalize_name(
            steam_name
        )

        rawg_clean = normalize_name(
            best_candidate.get("name", "")
        )

        status = classify_match_status(
            score,
            steam_clean,
            rawg_clean
        )

        match_result = {
            "steam_appid": appid,
            "steam_name": steam_name,
            "rawg_id": best_candidate.get("id"),
            "rawg_name": best_candidate.get("name"),
            "match_status": status,
            "similarity_score": score,
            "rawg_rating": best_candidate.get("rating"),
            "rawg_metacritic": best_candidate.get("metacritic"),
            "rawg_released": best_candidate.get("released"),
            "rawg_genres": best_candidate.get("genres", []),
            "rawg_platforms": best_candidate.get("platforms", []),
        }

        results.append(match_result)

        logger.info(
            "[%d] %s -> %s "
            "(status=%s, score=%d)",
            appid,
            steam_name,
            match_result["rawg_name"],
            status,
            score
        )

    return results