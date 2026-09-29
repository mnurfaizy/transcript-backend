"""
Backend transkripsi video Drive -> AssemblyAI -> tulis hasil ke Google Sheet.
Versi webhook: submit job ke AssemblyAI lalu selesai - hasil transkrip datang
lewat callback ke /webhook, bukan lewat polling yang bisa makan waktu lama.

Alur:
  POST /transcribe (dipanggil Apps Script, wajib header Authorization: Bearer <APP_SECRET_TOKEN>)
    Tahap 1: Autentikasi service account
    Tahap 2: Download video dari Google Drive (streaming ke file sementara)
    Tahap 3: Upload video itu ke AssemblyAI
    Tahap 4: Submit job transkripsi dengan webhook_url yang nunjuk balik ke /webhook
    -> selesai, proses ini nggak nunggu transkripsi kelar

  POST /webhook (dipanggil AssemblyAI otomatis begitu transkrip selesai)
    Ambil transkrip lengkap, tulis ke sel B<row> di Sheet.

Kalau ada tahap yang gagal, pesan errornya ditulis ke sel yang sama supaya
langsung kelihatan dari sheet-nya - detail lengkapnya selalu ada di log
(tab "Logs" di dashboard Render).
"""

import os
import io
import json
import logging
import tempfile

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from pydantic import BaseModel
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("transcribe")

app = FastAPI()

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

ASSEMBLYAI_API_KEY = os.environ.get("ASSEMBLYAI_API_KEY")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
APP_SECRET_TOKEN = os.environ.get("APP_SECRET_TOKEN")   # otentikasi dari Apps Script -> backend
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")       # verifikasi callback AssemblyAI -> backend
BASE_URL = os.environ.get("BASE_URL")                   # URL publik service ini sendiri (https://xxx.onrender.com)

# Ganti sesuai bahasa dominan di video kandidat. "id" = Indonesia.
TRANSCRIPT_LANGUAGE_CODE = "id"


class TranscribeRequest(BaseModel):
    file_id: str
    sheet_id: str
    tab_name: str = "Sheet1"
    row: int


def get_credentials():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def write_result(creds, sheet_id, tab_name, row, text):
    sheets = build("sheets", "v4", credentials=creds)
    range_ = f"{tab_name}!B{row}"
    sheets.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=range_,
        valueInputOption="RAW",
        body={"values": [[text]]},
    ).execute()


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/transcribe")
def transcribe(req: TranscribeRequest, background_tasks: BackgroundTasks, request: Request):
    auth_header = request.headers.get("authorization", "")
    if not APP_SECRET_TOKEN or auth_header != f"Bearer {APP_SECRET_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    log.info("Request diterima: file_id=%s, row=%s", req.file_id, req.row)
    background_tasks.add_task(submit_job, req.file_id, req.sheet_id, req.tab_name, req.row)
    return {"status": "diterima, sedang download & submit ke AssemblyAI"}


def submit_job(file_id, sheet_id, tab_name, row):
    """Download video + upload ke AssemblyAI + submit job.
    Setelah job ke-submit, tugas ini SELESAI - transkrip datang lewat webhook."""

    # Tahap 1: autentikasi
    try:
        log.info("[Tahap 1/4] Autentikasi service account...")
        creds = get_credentials()
        log.info("[Tahap 1/4] OK")
    except Exception as e:
        log.error("[GAGAL - Tahap 1] Autentikasi gagal: %s", e)
        return

    tmp_path = None

    # Tahap 2: download video dari Drive
    try:
        log.info("[Tahap 2/4] Download video dari Drive (file_id=%s)...", file_id)
        drive = build("drive", "v3", credentials=creds)
        request_media = drive.files().get_media(fileId=file_id)

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        tmp_path = tmp.name
        tmp.close()

        fh = io.FileIO(tmp_path, "wb")
        downloader = MediaIoBaseDownload(fh, request_media, chunksize=10 * 1024 * 1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                log.info("  progress download: %d%%", int(status.progress() * 100))
        fh.close()

        size_mb = os.path.getsize(tmp_path) / 1024 / 1024
        log.info("[Tahap 2/4] OK - tersimpan sementara, ukuran %.1f MB", size_mb)
    except Exception as e:
        msg = f"ERROR (download Drive): {e}"
        log.error("[GAGAL - Tahap 2] %s", e)
        write_result(creds, sheet_id, tab_name, row, msg)
        return

    # Tahap 3: upload ke AssemblyAI
    try:
        log.info("[Tahap 3/4] Upload ke AssemblyAI...")
        with open(tmp_path, "rb") as f:
            upload_resp = requests.post(
                "https://api.assemblyai.com/v2/upload",
                headers={"authorization": ASSEMBLYAI_API_KEY},
                data=f,
            )
        upload_resp.raise_for_status()
        upload_url = upload_resp.json()["upload_url"]
        log.info("[Tahap 3/4] OK - upload_url diterima")
    except Exception as e:
        msg = f"ERROR (upload AssemblyAI): {e}"
        log.error("[GAGAL - Tahap 3] %s", e)
        write_result(creds, sheet_id, tab_name, row, msg)
        return
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    # Tahap 4: submit job transkripsi dengan webhook - TIDAK POLLING DI SINI
    try:
        log.info("[Tahap 4/4] Submit job transkripsi (via webhook)...")
        webhook_url = f"{BASE_URL}/webhook?sheet_id={sheet_id}&tab_name={tab_name}&row={row}"
        submit_resp = requests.post(
            "https://api.assemblyai.com/v2/transcript",
            headers={"authorization": ASSEMBLYAI_API_KEY},
            json={
                "audio_url": upload_url,
                "language_code": TRANSCRIPT_LANGUAGE_CODE,
                "speech_models": ["universal-2"],
                "webhook_url": webhook_url,
                "webhook_auth_header_name": "x-webhook-secret",
                "webhook_auth_header_value": WEBHOOK_SECRET,
            },
        )
        submit_resp.raise_for_status()
        job_id = submit_resp.json().get("id")
        log.info("[Tahap 4/4] OK - job disubmit (id=%s), menunggu webhook callback", job_id)
    except Exception as e:
        msg = f"ERROR (submit AssemblyAI): {e}"
        log.error("[GAGAL - Tahap 4] %s", e)
        write_result(creds, sheet_id, tab_name, row, msg)
        return


@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        log.warning("[WEBHOOK] Ditolak - secret nggak cocok")
        raise HTTPException(status_code=401, detail="Unauthorized")

    sheet_id = request.query_params.get("sheet_id")
    tab_name = request.query_params.get("tab_name", "Sheet1")
    row = int(request.query_params.get("row", 0))

    body = await request.json()
    transcript_id = body.get("transcript_id")
    status = body.get("status")
    log.info("[WEBHOOK] Diterima untuk row=%s: transcript_id=%s status=%s", row, transcript_id, status)

    creds = get_credentials()

    if status != "completed":
        write_result(creds, sheet_id, tab_name, row, f"ERROR (status AssemblyAI: {status})")
        return {"ok": True}

    try:
        poll_resp = requests.get(
            f"https://api.assemblyai.com/v2/transcript/{transcript_id}",
            headers={"authorization": ASSEMBLYAI_API_KEY},
        )
        poll_resp.raise_for_status()
        transcript_text = poll_resp.json().get("text") or "(transkrip kosong)"
        write_result(creds, sheet_id, tab_name, row, transcript_text)
        log.info("[WEBHOOK] SELESAI - hasil ditulis ke %s!B%d", tab_name, row)
    except Exception as e:
        log.error("[WEBHOOK] Gagal ambil/tulis hasil: %s", e)
        write_result(creds, sheet_id, tab_name, row, f"ERROR (ambil hasil transkrip): {e}")

    return {"ok": True}
