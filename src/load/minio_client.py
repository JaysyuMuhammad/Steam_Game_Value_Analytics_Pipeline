"""
src/load/minio_client.py

Modul untuk baca/tulis data ke MinIO (S3-compatible object storage).
Dipakai untuk 2 keperluan:
  1. BRONZE  -- menyimpan raw JSON persis seperti dari API (idempotency:
     kalau proses transform gagal, tidak perlu re-fetch API).
"""

import os
import json
import logging
from datetime import date
from typing import Any

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

BUCKET_NAME = "steam-analytics"


def _get_client():
    """
    Bikin koneksi ke MinIO.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY / MINIO_SECRET_KEY tidak ditemukan di environment. "
            "Pastikan sudah diset lewat .env / Airflow Variable."
        )

    return boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


def _build_key(layer: str, dataset: str, snapshot_date: date) -> str:
    """
    Bangun path/key konsisten untuk objek di MinIO.
    Contoh: bronze/steamspy/2026-08-27/data.json
            silver/game_snapshot/2026-08-27/data.json
    """
    return f"{layer}/{dataset}/{snapshot_date.isoformat()}/data.json"


def write_json(layer: str, dataset: str, snapshot_date: date, data: Any) -> str:
    """
    Tulis data (list/dict) sebagai JSON ke MinIO.
    Return: key/path objek yang berhasil ditulis.
    """
    client = _get_client()
    key = _build_key(layer, dataset, snapshot_date)
    body = json.dumps(data, default=str).encode("utf-8")

    client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body, ContentType="application/json")
    logger.info("Berhasil menulis %d record ke s3://%s/%s", len(data) if hasattr(data, "__len__") else 1, BUCKET_NAME, key)
    return key


def read_json(layer: str, dataset: str, snapshot_date: date) -> Any:
    """
    Baca kembali data JSON dari MinIO berdasarkan layer, dataset, dan tanggal.
    Dipakai untuk melanjutkan proses (misal transform baca dari bronze,
    load ke Postgres baca dari silver) tanpa perlu re-fetch API.
    """
    client = _get_client()
    key = _build_key(layer, dataset, snapshot_date)

    response = client.get_object(Bucket=BUCKET_NAME, Key=key)
    body = response["Body"].read().decode("utf-8")
    data = json.loads(body)

    logger.info("Berhasil membaca %d record dari s3://%s/%s", len(data) if hasattr(data, "__len__") else 1, BUCKET_NAME, key)
    return data


def object_exists(layer: str, dataset: str, snapshot_date: date) -> bool:
    """
    Cek apakah objek untuk kombinasi layer/dataset/tanggal tertentu sudah
    ada. Berguna sebagai idempotency check -- kalau Bronze untuk hari ini
    sudah ada, task extract tidak perlu memanggil API lagi.
    """
    client = _get_client()
    key = _build_key(layer, dataset, snapshot_date)
    try:
        client.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except client.exceptions.ClientError:
        return False