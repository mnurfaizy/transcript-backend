"""
Backend transkripsi video Drive/YouTube -> AssemblyAI/Apify -> file .md di Drive + link di Google Sheet.

Versi untuk dashboard "Initial Assessment V.3":
  kolom A = Nama kandidat, D = Project Assignment, G = Supporting Link (video/portfolio),
  kolom H = Transcript Video (diisi link ke file .md).

Input `file_ids` boleh berupa ID FILE video ATAU ID FOLDER Drive:
  Case 1: link langsung ke satu video
  Case 2: link folder berisi beberapa video (file non-video dilewati, bukan error)
  Case 3: link folder yang videonya ada di subfolder (dicari rekursif,
          sampai kedalaman MAX_FOLDER_DEPTH)

Hasil: SATU BARIS = SATU FILE .md, apa pun jumlah videonya:
  Talent Assessment / <Project Assignment> / transcript - <Nama kandidat>.md
Tiap video menjadi satu bagian di file itu (judul + link video + transkrip ber-timestamp).
Link file .md ditulis ke kolom `output_col` (H). Menjalankan ulang baris yang sama
memperbarui file yang sama (tidak membuat duplikat).

File .md dibuat oleh web app Apps Script milik pemilik folder (bukan service account:
service account tidak punya kuota di My Drive Gmail pribadi).

Alur:
  POST /transcribe (dari Apps Script, header Authorization: Bearer <APP_SECRET_TOKEN>)
    -> langsung balas "diterima", lalu di background:
       Resolve : cek file/folder, kumpulkan daftar video
       Per video (satu per satu): ambil video/audio -> upload AssemblyAI -> submit job + webhook
       (video kecil di-download utuh; video besar cukup diekstrak audionya lewat ffmpeg)
  POST /webhook (dipanggil AssemblyAI per video yang selesai)
    -> ambil transkrip ber-timestamp, simpan sebagai bagian dari "job baris"
  Begitu SEMUA video sebuah baris punya hasil (berhasil atau gagal):
    -> susun satu file .md -> kirim ke web app Apps Script -> tulis link ke kolom H

Kalau ada video yang gagal tapi yang lain berhasil, file .md tetap dibuat dan bagian
yang gagal ditandai "GAGAL" beserta alasannya. Kalau SEMUA gagal, pesan error ditulis
ke sel H. Detail lengkap ada di tab "Logs" Render.
"""

import os
import io
import re
import html
import json
import logging
import queue
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlparse, parse_qs

from fastapi import FastAPI, Request, HTTPException
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest
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
APP_SECRET_TOKEN = os.environ.get("APP_SECRET_TOKEN")   # Apps Script -> backend
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")       # AssemblyAI -> backend
BASE_URL = os.environ.get("BASE_URL")                   # URL publik service ini (tanpa trailing slash)

# File .md ditulis lewat web app Apps Script (jalan sebagai pemilik folder Talent Assessment).
DRIVE_WEBAPP_URL = os.environ.get("DRIVE_WEBAPP_URL")        # URL web app, berakhiran /exec
DRIVE_WEBAPP_SECRET = os.environ.get("DRIVE_WEBAPP_SECRET")  # sama dengan Script Property TX_DRIVE_SECRET
WITA = timezone(timedelta(hours=8))
JOB_TTL_S = 12 * 3600       # job baris yang tak kunjung lengkap dibuang dari memori setelah 12 jam

# Opsional (Environment Variable Render): seberapa dalam subfolder ditelusuri.
MAX_FOLDER_DEPTH = int(os.environ.get("MAX_FOLDER_DEPTH", "5"))

# Penyimpanan sementara (/tmp) instance Render free dibatasi 2 GB. Kalau penuh,
# Render mematikan instance dan SEMUA job yang sedang jalan hilang. Jadi:
#   ukuran <= DISK_MAX_MB : download video ke disk lalu upload (jalur teruji)
#   ukuran  > DISK_MAX_MB : videonya TIDAK disimpan. ffmpeg membaca langsung dari
#                           Drive lewat HTTP, membuang gambarnya, dan hanya menyimpan
#                           audio (mono 16 kHz mp3, sekitar 28 MB per jam). Audio kecil
#                           itulah yang di-upload ke AssemblyAI. Tidak ada batas ukuran video.
DISK_MAX_MB = int(os.environ.get("DISK_MAX_MB", "1500"))
DISK_MAX_BYTES = DISK_MAX_MB * 1024 * 1024
AUDIO_BITRATE = "64k"
FFMPEG_TIMEOUT_S = int(os.environ.get("FFMPEG_TIMEOUT_S", "3600"))
PROGRESS_LOG_EVERY_S = 30   # seberapa sering progres ekstraksi audio ditulis ke log

DRIVE_MEDIA_URL = "https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"

# --- YouTube: transkrip diambil dari caption lewat Apify (satu video per panggilan);
#     playlist diperluas dulu jadi daftar video lewat YouTube Data API v3 (gratis, butuh API key).
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
APIFY_ACTOR = os.environ.get("APIFY_ACTOR", "pintostudio~youtube-transcript-scraper")
YT_TRANSCRIPT_LANG = os.environ.get("YT_TRANSCRIPT_LANG", "id")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
YT_PARAGRAPH_SECONDS = 30   # caption digabung jadi paragraf tiap ~30 detik, diawali [MM:SS]
APIFY_RUN_URL = "https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
YOUTUBE_PLAYLIST_URL = "https://www.googleapis.com/youtube/v3/playlistItems"
ASSEMBLYAI_UPLOAD_URL = "https://api.assemblyai.com/v2/upload"

# Batas keras jumlah video per permintaan (pengaman kredit AssemblyAI).
HARD_CAP_VIDEOS = 20

# "id" = Indonesia. Ganti kalau bahasa dominan video berbeda.
TRANSCRIPT_LANGUAGE_CODE = "id"

FOLDER_MIME = "application/vnd.google-apps.folder"
VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi", ".wmv", ".flv",
    ".mpeg", ".mpg", ".mts", ".m2ts", ".mxf", ".3gp",
}

# --- Transkrip yang SUDAH ADA di Drive (file di folder yang sama dengan videonya, mis. transkrip
#     Google Meet berupa Google Doc, atau .vtt/.srt/.txt dari Zoom). Kalau ada dan cocok dengan yakin,
#     dipakai langsung (tanpa AssemblyAI). Matikan dengan Environment Variable USE_DRIVE_TRANSCRIPTS=0.
USE_DRIVE_TRANSCRIPTS = os.environ.get("USE_DRIVE_TRANSCRIPTS", "1") != "0"
GDOC_MIME = "application/vnd.google-apps.document"
SIDECAR_RANK = {"vtt": 0, "srt": 1, "gdoc": 2, "txt": 3}   # makin kecil makin diutamakan
TRANSCRIPT_HINT = re.compile(r"transcript|transkrip|caption|subtitle", re.I)
MIN_TRANSCRIPT_CHARS = 30

# Batas Google Sheets: 50.000 karakter per sel.
MAX_CELL_CHARS = 49000


class TranscribeRequest(BaseModel):
    file_ids: list[str] = []   # satu atau lebih ID file video / folder (dari satu sel)
    file_id: str = ""          # kompatibilitas versi lama (satu ID)
    youtube_urls: list[str] = []   # link video / playlist YouTube (dari sel yang sama)
    sheet_id: str
    tab_name: str = "Initial Assessment V.3"
    row: int
    output_col: str = "H"   # kolom untuk link hasil (Transcript Video)
    max_videos: int = HARD_CAP_VIDEOS   # batas video per baris (pengaman kredit)
    candidate: str = ""     # Nama kandidat (kolom A) -> nama file
    project: str = ""       # Project Assignment (kolom D) -> nama folder


# ---------------------------------------------------------------- helper umum

def get_credentials():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def col_to_num(letters):
    n = 0
    for ch in letters.upper():
        n = n * 26 + (ord(ch) - 64)
    return n


def num_to_col(n):
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1(tab_name, col, row):
    # Nama tab berspasi (mis. "Initial Assessment") wajib dikutip di notasi A1.
    safe_tab = tab_name.replace("'", "''")
    return f"'{safe_tab}'!{col}{row}"


def fit_cell(text):
    if len(text) <= MAX_CELL_CHARS:
        return text
    cut = text[:MAX_CELL_CHARS]
    idx = cut.rfind("\n\n")
    if idx > MAX_CELL_CHARS * 0.8:
        cut = cut[:idx]
    hilang = len(text) - len(cut)
    return (cut + "\n\n[TERPOTONG: transkrip melebihi batas 50.000 karakter per sel "
            f"Google Sheets. {hilang} karakter terakhir tidak ditampilkan.]")


def write_cell(creds, sheet_id, tab_name, row, col, text):
    sheets = build("sheets", "v4", credentials=creds)
    sheets.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=a1(tab_name, col, row),
        valueInputOption="RAW",
        body={"values": [[fit_cell(text)]]},
    ).execute()


def write_link(creds, sheet_id, tab_name, row, col, url, label="Transcript"):
    """Tulis teks `label` yang berlink ke `url` (rich-text link, tidak tergantung locale rumus)."""
    sheets = build("sheets", "v4", credentials=creds)
    meta = sheets.spreadsheets().get(
        spreadsheetId=sheet_id, fields="sheets.properties(sheetId,title)"
    ).execute()
    gid = next((s["properties"]["sheetId"] for s in meta.get("sheets", [])
                if s["properties"]["title"] == tab_name), None)
    if gid is None:
        raise RuntimeError(f"tab '{tab_name}' tidak ditemukan di spreadsheet")
    c = col_to_num(col) - 1
    sheets.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": [{
        "updateCells": {
            "range": {"sheetId": gid, "startRowIndex": row - 1, "endRowIndex": row,
                      "startColumnIndex": c, "endColumnIndex": c + 1},
            "rows": [{"values": [{
                "userEnteredValue": {"stringValue": label},
                "textFormatRuns": [{"startIndex": 0, "format": {"link": {"uri": url}}}],
            }]}],
            "fields": "userEnteredValue,textFormatRuns",
        }
    }]}).execute()


def format_timestamp_ms(ms):
    """Milidetik -> MM:SS, atau H:MM:SS kalau lebih dari 1 jam."""
    total_seconds = int(ms // 1000)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def fetch_timestamped_transcript(transcript_id):
    """Transkrip per paragraf, tiap paragraf diawali timestamp [MM:SS]."""
    resp = requests.get(
        f"https://api.assemblyai.com/v2/transcript/{transcript_id}/paragraphs",
        headers={"authorization": ASSEMBLYAI_API_KEY},
        timeout=60,
    )
    resp.raise_for_status()
    paragraphs = resp.json().get("paragraphs", [])
    if not paragraphs:
        return "(transkrip kosong)"
    lines = [f"[{format_timestamp_ms(p['start'])}] {p['text']}" for p in paragraphs]
    return "\n\n".join(lines)


# ----------------------------------------- satu baris = satu file .md

class RowJob:
    """Menampung hasil semua video dari SATU baris sheet sampai lengkap, lalu jadi satu file .md.
    Disimpan di memori (sama seperti antrean): kalau Render restart, job yang berjalan hilang."""

    def __init__(self, job_id, videos, notes, candidate, project, sheet_id, tab_name, row, col):
        self.id = job_id
        self.videos = videos
        self.total = len(videos)
        self.notes = notes
        self.candidate = candidate
        self.project = project
        self.sheet_id = sheet_id
        self.tab_name = tab_name
        self.row = row
        self.col = col
        self.parts = {}          # idx (1-based) -> {"text", "error", "source"}
        self.finalized = False
        self.result = None       # {"ok": bool, "url"/"error": ...} setelah selesai
        self.created = time.time()
        self.lock = threading.Lock()


ROW_JOBS = {}
ROW_JOBS_LOCK = threading.Lock()


def register_job(job):
    now = time.time()
    with ROW_JOBS_LOCK:
        for jid in [k for k, j in ROW_JOBS.items() if now - j.created > JOB_TTL_S]:
            log.warning("[Job] %s dibuang (lebih dari %d jam tidak lengkap)", jid, JOB_TTL_S // 3600)
            ROW_JOBS.pop(jid, None)
        ROW_JOBS[job.id] = job


def get_job(job_id):
    with ROW_JOBS_LOCK:
        return ROW_JOBS.get(job_id)


def submit_part(job, idx, text=None, error=None, source=""):
    """Catat hasil satu video. Kalau ini video terakhir yang ditunggu, langsung susun & simpan file .md.
    Fungsi ini tidak melempar error (semua kegagalan ditulis ke log / sel H)."""
    with job.lock:
        if job.finalized or idx in job.parts or not (1 <= idx <= job.total):
            return
        job.parts[idx] = {"text": text, "error": error, "source": source}
        done = len(job.parts)
        ready = done >= job.total
        if ready:
            job.finalized = True
        else:
            # progres ditulis di dalam lock supaya tidak menimpa hasil akhir dari thread lain
            try:
                write_cell(get_credentials(), job.sheet_id, job.tab_name, job.row, job.col,
                           f"Diproses: {done}/{job.total} video selesai - menunggu sisanya")
            except Exception as e:
                log.warning("[Job %s] gagal menulis progres: %s", job.id, e)
    if ready:
        finalize_job(job)


def build_markdown(job):
    now = datetime.now(WITA).strftime("%Y-%m-%d %H:%M WITA")
    ok = sum(1 for p in job.parts.values() if p.get("text"))
    lines = [f"# Transcript - {job.candidate}", "",
             f"- **Project:** {job.project}",
             f"- **Diproses:** {now}",
             f"- **Jumlah video:** {job.total} ({ok} berhasil)"]
    if job.notes:
        lines.append(f"- **Catatan:** {job.notes}")
    lines += ["", "---", ""]
    for i, v in enumerate(job.videos, start=1):
        p = job.parts.get(i) or {}
        lines.append(f"## {i}. {v['path']}" if job.total > 1 else f"## {v['path']}")
        if v.get("link"):
            lines.append(f"Link: {v['link']}")
        if p.get("source"):
            lines.append(f"Sumber transkrip: {p['source']}")
        lines.append("")
        lines.append(p["text"] if p.get("text") else f"> **GAGAL** - {p.get('error') or 'tidak ada hasil'}")
        lines += ["", "---", ""]
    return "\n".join(lines).rstrip() + "\n"


def save_markdown_to_drive(project, candidate, content):
    """Kirim isi .md ke web app Apps Script -> {'url', 'action', ...}. Aman diulang (file diperbarui by nama)."""
    if not DRIVE_WEBAPP_URL or not DRIVE_WEBAPP_SECRET:
        raise RuntimeError("DRIVE_WEBAPP_URL / DRIVE_WEBAPP_SECRET belum di-set di Environment Variable Render")
    payload = {"secret": DRIVE_WEBAPP_SECRET, "project": project, "candidate": candidate, "content": content}
    last = "tidak diketahui"
    for attempt in (1, 2, 3):
        try:
            # Apps Script membalas lewat redirect 302; requests mengikutinya otomatis.
            r = requests.post(DRIVE_WEBAPP_URL, json=payload, timeout=120)
            try:
                data = r.json()
            except ValueError:
                raise RuntimeError(
                    f"web app Apps Script tidak membalas JSON (HTTP {r.status_code}). Cek deployment: "
                    "'Execute as: Me' dan 'Who has access: Anyone', serta URL berakhiran /exec")
            if not data.get("ok"):
                raise RuntimeError("web app menolak: " + str(data.get("error", data))[:200])
            if not data.get("url"):
                raise RuntimeError(
                    "web app membalas ok tapi tanpa 'url' file. Balasan aslinya: "
                    + json.dumps(data, ensure_ascii=False)[:300]
                    + ". Kemungkinan DRIVE_WEBAPP_URL menunjuk ke deployment/script lain, atau ada "
                    "doPost/doGet ganda di project Apps Script.")
            return data
        except Exception as e:
            last = str(e).replace(DRIVE_WEBAPP_SECRET, "***")
            log.warning("[Drive] percobaan %d/3 gagal: %s", attempt, last)
            if ("menolak" in last and "Unauthorized" in last) or "tanpa 'url'" in last:
                break          # secret salah / balasan salah: mengulang tidak membantu
            time.sleep(4 * attempt)
    raise RuntimeError(last)


def finalize_job(job):
    """Semua video baris ini sudah punya hasil: simpan .md, tulis link (atau error) ke kolom hasil."""
    with ROW_JOBS_LOCK:
        ROW_JOBS.pop(job.id, None)
    try:
        creds = get_credentials()
        good = [i for i, p in job.parts.items() if p.get("text")]
        if not good:
            errs = [job.parts[i].get("error") or "tidak ada hasil" for i in sorted(job.parts)]
            msg = errs[0] if job.total == 1 else \
                f"ERROR: semua {job.total} video gagal. " + " | ".join(f"[{i + 1}] {e}" for i, e in enumerate(errs))
            log.error("[Job %s] %s", job.id, msg)
            write_cell(creds, job.sheet_id, job.tab_name, job.row, job.col, msg)
            job.result = {"ok": False, "error": msg}
            return
        res = save_markdown_to_drive(job.project, job.candidate, build_markdown(job))
        write_link(creds, job.sheet_id, job.tab_name, job.row, job.col, res["url"])
        failed = job.total - len(good)
        log.info("[Job %s] SELESAI - %s (%s), %d/%d video berhasil -> %s",
                 job.id, f"transcript - {job.candidate}.md", res.get("action", "?"), len(good), job.total, res["url"])
        if failed:
            log.warning("[Job %s] %d video gagal; ditandai GAGAL di dalam file .md", job.id, failed)
        job.result = {"ok": True, "url": res["url"], "action": res.get("action"), "failed": failed}
    except Exception as e:
        msg = f"ERROR (simpan .md ke Drive): {e}"
        log.error("[Job %s] %s", job.id, msg)
        job.result = {"ok": False, "error": msg}
        try:
            write_cell(get_credentials(), job.sheet_id, job.tab_name, job.row, job.col, msg)
        except Exception as e2:
            log.error("[Job %s] gagal menulis pesan error ke sheet: %s", job.id, e2)


def describe_failure(transcript_id, status):
    try:
        r = requests.get(
            f"https://api.assemblyai.com/v2/transcript/{transcript_id}",
            headers={"authorization": ASSEMBLYAI_API_KEY},
            timeout=60,
        )
        return r.json().get("error") or status
    except Exception:
        return status


# ------------------------------------------------ pencarian video di Drive

def is_video(name, mime):
    if mime and mime.startswith("video/"):
        return True
    # Beberapa video (mis. .mkv) kadang terdaftar sebagai file umum di Drive.
    if not mime or mime == "application/octet-stream":
        return os.path.splitext(name.lower())[1] in VIDEO_EXTENSIONS
    return False


def to_int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def natural_key(s):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", s.lower())]


def list_children(drive, folder_id):
    items, page_token = [], None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, mimeType, size, capabilities(canDownload))",
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items


def make_video(item, path):
    return {"id": item["id"], "name": item["name"], "path": path, "size": to_int(item.get("size")),
            "link": f"https://drive.google.com/file/d/{item['id']}/view",
            "can_download": (item.get("capabilities") or {}).get("canDownload")}


def transcript_kind(name, mime):
    """Jenis file transkrip: 'vtt' | 'srt' | 'txt' | 'gdoc' | None. Google Doc dianggap transkrip
    hanya kalau namanya menyebut transcript/transkrip/caption/subtitle."""
    if mime == GDOC_MIME:
        return "gdoc" if TRANSCRIPT_HINT.search(name) else None
    ext = os.path.splitext(name.lower())[1]
    if ext == ".vtt" or mime == "text/vtt":
        return "vtt"
    if ext == ".srt" or mime == "application/x-subrip":
        return "srt"
    if ext == ".txt":
        return "txt"
    return None


_NAME_NOISE = re.compile(r"\b(closed caption|recording|transcript|transkrip|captions?|subtitles?|audio|video|cc)\b")
_LANG_TAIL = re.compile(r"\b(id|en|ind|indonesia|indonesian|english)$")


def norm_name(name):
    """Nama disederhanakan supaya 'X - Recording.mp4' dan 'X - Transcript' (atau
    'GMT..._Recording_1920x1080.mp4' dan 'GMT..._Recording.transcript.vtt') dianggap sama."""
    base = re.sub(r"\.(mp4|mov|mkv|webm|m4v|avi|wmv|flv|mpeg|mpg|mts|m2ts|mxf|3gp|vtt|srt|txt|sbv)$", "", name, flags=re.I)
    base = re.sub(r"\.(transcript|captions?|subtitles?)$", "", base, flags=re.I)
    base = re.sub(r"\d{3,4}x\d{3,4}", " ", base)
    base = re.sub(r"[\W_]+", " ", base.lower()).strip()
    base = _NAME_NOISE.sub(" ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return _LANG_TAIL.sub("", base).strip()


def classify_items(items, prefix=""):
    """Isi satu folder -> (subfolder, video, kandidat file transkrip, item lain yang dilewati)."""
    folders, vids, cands, others = [], [], [], []
    for item in items:
        name, mime = item["name"], item.get("mimeType", "")
        path = prefix + name
        if mime == FOLDER_MIME:
            folders.append((item, path))
        elif is_video(name, mime):
            vids.append(make_video(item, path))
        else:
            kind = transcript_kind(name, mime) if USE_DRIVE_TRANSCRIPTS else None
            if kind:
                cands.append({"id": item["id"], "name": name, "kind": kind, "path": path,
                              "strong": kind in ("vtt", "srt") or bool(TRANSCRIPT_HINT.search(name))})
            else:
                others.append(f"{path} ({mime})")
    return folders, vids, cands, others


def attach_sidecars(videos, cands):
    """Pasangkan video dengan file transkrip dari SATU folder yang sama. Hanya pasangan yang yakin:
    (1) nama sama setelah dinormalisasi, atau (2) satu-satunya video di folder itu dan ada tepat satu
    kandidat 'kuat' (.vtt/.srt atau nama menyebut transcript) dengan prioritas tertinggi.
    Ragu-ragu = tidak dipasangkan, dan video jatuh ke AssemblyAI."""
    if not videos or not cands:
        return
    by_key = {}
    for c in cands:
        key = norm_name(c["name"])
        if key:
            by_key.setdefault(key, []).append(c)
    for v in videos:
        pool = by_key.get(norm_name(v["name"]) or "\0")
        if pool:
            best = min(SIDECAR_RANK[c["kind"]] for c in pool)
            top = [c for c in pool if SIDECAR_RANK[c["kind"]] == best]
            if len(top) == 1:
                v["sidecar"] = top[0]
    if len(videos) == 1 and "sidecar" not in videos[0]:
        strong = [c for c in cands if c["strong"]]
        if strong:
            best = min(SIDECAR_RANK[c["kind"]] for c in strong)
            top = [c for c in strong if SIDECAR_RANK[c["kind"]] == best]
            if len(top) == 1:
                videos[0]["sidecar"] = top[0]


def attach_sidecar_from_parent(drive, video, parent_id):
    """Link langsung ke satu video: cari file transkrip di folder induknya (kalau folder itu bisa dibaca)."""
    try:
        _, vids, cands, _ = classify_items(list_children(drive, parent_id))
    except Exception as e:
        log.info("[Resolve] folder induk tidak bisa dibaca, pencarian transkrip Drive dilewati: %s", e)
        return
    attach_sidecars(vids, cands)
    for v in vids:
        if v["id"] == video["id"] and v.get("sidecar"):
            video["sidecar"] = v["sidecar"]


def collect_videos(drive, root_id, prefix=""):
    """Telusuri folder (dan subfolder) -> (daftar video, daftar item yang dilewati)."""
    videos, skipped, visited = [], [], set()
    stack = [(root_id, prefix, 0)]
    while stack:
        folder_id, prefix, depth = stack.pop()
        if folder_id in visited:
            continue
        visited.add(folder_id)
        folders, vids, cands, others = classify_items(list_children(drive, folder_id), prefix)
        skipped.extend(others)
        attach_sidecars(vids, cands)
        used = {v["sidecar"]["id"] for v in vids if v.get("sidecar")}
        skipped.extend(f"{c['path']} (file transkrip tidak terpasang ke video mana pun)"
                       for c in cands if c["id"] not in used)
        videos.extend(vids)
        for item, path in folders:
            if depth + 1 > MAX_FOLDER_DEPTH:
                skipped.append(f"{path}/ (subfolder terlalu dalam)")
            else:
                stack.append((item["id"], path + "/", depth + 1))
    videos.sort(key=lambda v: natural_key(v["path"]))
    return videos, skipped


def summarize_skipped(skipped, limit=5):
    if not skipped:
        return ""
    shown = "; ".join(skipped[:limit])
    more = f" dan {len(skipped) - limit} lainnya" if len(skipped) > limit else ""
    return f" {len(skipped)} item dilewati: {shown}{more}."


def gather_videos(drive, root_ids, sa_email):
    """Kumpulkan video dari satu atau lebih link (file atau folder), urut sesuai link.
    Link yang tidak bisa diakses / bukan video dicatat, tidak menggagalkan link lain."""
    videos, skipped, notes, seen = [], [], [], set()
    multi = len(root_ids) > 1
    for rid in root_ids:
        try:
            meta = drive.files().get(
                fileId=rid, fields="id,name,mimeType,size,parents,capabilities(canDownload)", supportsAllDrives=True
            ).execute()
        except Exception as e:
            log.warning("[Resolve] id=%s tidak bisa diakses: %s", rid, e)
            notes.append(f"link {rid[:6]}... tidak bisa diakses (share ke {sa_email})")
            continue
        name, mime = meta["name"], meta.get("mimeType", "")
        if mime == FOLDER_MIME:
            log.info("[Resolve] Folder '%s' - mencari video (maks kedalaman %d)...", name, MAX_FOLDER_DEPTH)
            found, skip = collect_videos(drive, rid, prefix=(name + "/") if multi else "")
            skipped.extend(skip)
        elif is_video(name, mime):
            found = [make_video(meta, name)]
            if USE_DRIVE_TRANSCRIPTS and meta.get("parents"):
                attach_sidecar_from_parent(drive, found[0], meta["parents"][0])
        else:
            skipped.append(f"{name} ({mime})")
            continue
        for v in found:
            if v["id"] not in seen:
                seen.add(v["id"])
                videos.append(v)
    return videos, skipped, notes


# ------------------------------------------- antrean: satu permintaan per waktu

# Instance Render free hanya 512 MB RAM / 0.1 CPU. Kalau beberapa baris dipicu
# sekaligus lalu diproses paralel (download ratusan MB masing-masing), instance
# bisa kelebihan beban dan restart, dan semua job yang sedang jalan hilang.
# Karena itu semua permintaan masuk antrean FIFO dan dikerjakan satu per satu.
JOBS = queue.Queue()


def _worker():
    while True:
        args = JOBS.get()
        row = args[4]
        try:
            log.info("[Antrean] mulai memproses baris %s (menunggu di belakangnya: %d)", row, JOBS.qsize())
            process_request(*args)
            log.info("[Antrean] baris %s selesai diproses", row)
        except Exception as e:
            log.error("[Antrean] baris %s error tak terduga: %s", row, e)
        finally:
            JOBS.task_done()


threading.Thread(target=_worker, daemon=True, name="job-worker").start()


# ------------------------------------------------------------------ endpoint

@app.get("/")
def health():
    # "ffmpeg": true = ffmpeg terpasang (dibutuhkan untuk video besar)
    # "drive_webapp": true = DRIVE_WEBAPP_URL & DRIVE_WEBAPP_SECRET sudah di-set (untuk menyimpan file .md)
    return {"status": "ok", "ffmpeg": shutil.which("ffmpeg") is not None,
            "drive_webapp": bool(DRIVE_WEBAPP_URL and DRIVE_WEBAPP_SECRET)}


@app.post("/transcribe")
def transcribe(req: TranscribeRequest, request: Request):
    auth_header = request.headers.get("authorization", "")
    if not APP_SECRET_TOKEN or auth_header != f"Bearer {APP_SECRET_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    output_col = re.sub(r"[^A-Za-z]", "", req.output_col).upper() or "H"
    max_videos = max(1, min(req.max_videos, HARD_CAP_VIDEOS))
    root_ids = list(dict.fromkeys(i for i in (req.file_ids or [req.file_id]) if i))
    youtube_urls = list(dict.fromkeys(u.strip() for u in req.youtube_urls if u and u.strip()))
    candidate, project = req.candidate.strip(), req.project.strip()
    if not root_ids and not youtube_urls:
        raise HTTPException(status_code=400, detail="tidak ada link Drive/YouTube")
    if not candidate or not project:
        raise HTTPException(status_code=400, detail="nama kandidat / project assignment kosong")
    log.info("Request diterima: %d link Drive, %d link YouTube, tab=%s, row=%s, kolom=%s, maks_video=%s, "
             "kandidat=%s, project=%s", len(root_ids), len(youtube_urls), req.tab_name, req.row,
             output_col, max_videos, candidate, project)
    JOBS.put((root_ids, youtube_urls, req.sheet_id, req.tab_name, req.row, output_col, max_videos,
              candidate, project))
    waiting = JOBS.qsize()
    log.info("[Antrean] baris %s masuk antrean (menunggu giliran: %d)", req.row, waiting)
    return {"status": "diterima, masuk antrean", "menunggu_di_depan": waiting}


def process_request(root_ids, youtube_urls, sheet_id, tab_name, row, output_col, max_videos,
                    candidate, project):
    job = None
    try:
        creds = get_credentials()
        sa_email = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON).get("client_email", "service account")
    except Exception as e:
        log.error("[GAGAL - Autentikasi] %s", e)  # tanpa kredensial, sheet tidak bisa ditulis
        return

    def fail(msg):
        log.error(msg)
        try:
            write_cell(creds, sheet_id, tab_name, row, output_col, msg)
        except Exception as e2:
            log.error("Gagal menulis pesan error ke sheet: %s", e2)

    try:
        drive = build("drive", "v3", credentials=creds)

        # --- Resolve: kumpulkan video dari semua link di sel
        videos, skipped, notes = [], [], []
        if root_ids:
            log.info("[Resolve] %d link Drive: %s", len(root_ids), ", ".join(root_ids))
            videos, skipped, notes = gather_videos(drive, root_ids, sa_email)
        if youtube_urls:
            log.info("[Resolve] %d link YouTube", len(youtube_urls))
            yt_videos, yt_notes = gather_youtube(youtube_urls)
            videos = videos + yt_videos
            notes = notes + yt_notes
        total = len(videos)
        log.info("[Resolve] Ditemukan %d video.%s", total, summarize_skipped(skipped))

        if total == 0:
            detail = (" " + "; ".join(notes).rstrip(".") + ".") if notes else ""
            hint = f" Untuk link Drive, pastikan sudah di-share ke {sa_email} (minimal Viewer)." if root_ids else ""
            fail("ERROR: tidak ada video ditemukan." + detail + summarize_skipped(skipped) + hint)
            return

        if total > max_videos:
            fail(f"ERROR: ditemukan {total} video, melebihi batas {max_videos} per baris. "
                 "Pisahkan folder/playlist-nya ke beberapa baris.")
            return

        # --- Satu job per baris: menampung hasil semua video, lalu jadi satu file .md
        note_parts = list(notes)
        if skipped:
            note_parts.append(summarize_skipped(skipped).strip())
        job = RowJob(uuid.uuid4().hex[:12], videos, "; ".join(n for n in note_parts if n),
                     candidate, project, sheet_id, tab_name, row, output_col)
        register_job(job)
        write_cell(creds, sheet_id, tab_name, row, output_col, f"Diproses: {total} video - dalam antrean")

        # --- Proses satu per satu (hemat RAM & disk di Render free)
        for i, video in enumerate(videos):
            if video.get("kind") == "youtube":
                process_youtube_video(creds, video, i + 1, total, job)
            else:
                process_one_video(creds, drive, video, i + 1, total, job)

        log.info("[Selesai] Semua %d video diproses; transkrip video Drive menyusul lewat webhook.", total)
    except Exception as e:
        if job is not None:
            job.finalized = True   # hentikan job ini supaya webhook susulan tidak menimpa pesan error
            with ROW_JOBS_LOCK:
                ROW_JOBS.pop(job.id, None)
        fail(f"ERROR (tak terduga): {e}")


# ------------------------------------------------------------------- YouTube

class NoCaptions(Exception):
    pass


def parse_youtube_url(url):
    """-> ("video", id) | ("playlist", id) | (None, None).
    Link /watch?v=ID&list=... dianggap SATU video (bukan seluruh playlist)."""
    u = url.strip()
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    try:
        parsed = urlparse(u)
    except ValueError:
        return None, None
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    q = parse_qs(parsed.query)
    vid_re = r"[A-Za-z0-9_-]{11}"

    if host == "youtu.be" or host.endswith(".youtu.be"):
        m = re.match(rf"^/({vid_re})", path)
        return ("video", m.group(1)) if m else (None, None)
    if host == "youtube.com" or host.endswith(".youtube.com"):
        v = (q.get("v") or [""])[0]
        if path.startswith("/watch") and re.match(vid_re, v):
            return "video", v[:11]
        m = re.match(rf"^/(?:shorts|live|embed|v)/({vid_re})", path)
        if m:
            return "video", m.group(1)
        lst = (q.get("list") or [""])[0]
        if lst and (path.startswith("/playlist") or path.startswith("/watch")):
            return "playlist", lst
    return None, None


def expand_playlist(playlist_id, limit=100):
    """Isi playlist -> [(video_id, judul)] lewat YouTube Data API v3."""
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY belum di-set di Render (dibutuhkan untuk membaca isi playlist)")
    items, page_token = [], None
    while len(items) < limit:
        params = {"part": "snippet", "playlistId": playlist_id, "maxResults": 50, "key": YOUTUBE_API_KEY}
        if page_token:
            params["pageToken"] = page_token
        try:
            r = requests.get(YOUTUBE_PLAYLIST_URL, params=params, timeout=30)
        except requests.RequestException as e:
            raise RuntimeError("koneksi ke YouTube API gagal: " + str(e).replace(YOUTUBE_API_KEY, "***"))
        if r.status_code != 200:
            try:
                msg = r.json()["error"]["message"]
            except Exception:
                msg = r.text[:150]
            raise RuntimeError(f"YouTube API HTTP {r.status_code}: {msg}")
        data = r.json()
        for it in data.get("items", []):
            sn = it.get("snippet") or {}
            vid = (sn.get("resourceId") or {}).get("videoId")
            if vid:
                items.append((vid, sn.get("title", "")))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return items


def gather_youtube(urls):
    """Link YouTube (video / playlist) -> daftar video + catatan masalah."""
    videos, notes, seen, private = [], [], set(), 0
    for u in urls:
        kind, yid = parse_youtube_url(u)
        if kind == "video":
            entries = [(yid, "")]
        elif kind == "playlist":
            try:
                entries = expand_playlist(yid)
            except Exception as e:
                notes.append(f"playlist {yid[:8]}... gagal dibaca: {e}")
                continue
        else:
            notes.append(f"link YouTube tidak dikenali: {u[:60]}")
            continue
        for vid, title in entries:
            if title in ("Private video", "Deleted video"):
                private += 1
                continue
            if vid in seen:
                continue
            seen.add(vid)
            label = f"{title} (youtu.be/{vid})" if title else f"youtu.be/{vid}"
            videos.append({"kind": "youtube", "id": vid, "name": title or vid, "path": label,
                           "url": f"https://www.youtube.com/watch?v={vid}",
                           "link": f"https://www.youtube.com/watch?v={vid}"})
    if private:
        notes.append(f"{private} video playlist private/terhapus dilewati")
    return videos, notes


def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _clean_caption(text):
    for _ in range(2):  # caption YouTube kadang ter-escape dua kali (&amp;#39;)
        new = html.unescape(text)
        if new == text:
            break
        text = new
    return re.sub(r"\s+", " ", text).strip()


def format_youtube_segments(segments, guess_units=True):
    """Segmen caption [{start, dur, text}] -> paragraf ~30 detik, diawali [MM:SS]."""
    groups = []  # [detik_mulai, [potongan teks]]
    # Pengaman satuan: waktu mulai di atas 10 jam hampir pasti berarti milidetik, bukan detik.
    starts = [_to_float(sg.get("start")) for sg in segments]
    scale = 0.001 if (guess_units and starts and max(starts) > 36000) else 1.0
    for seg in segments:
        text = _clean_caption(str(seg.get("text", "")))
        if not text:
            continue
        start = _to_float(seg.get("start")) * scale
        if not groups or start - groups[-1][0] >= YT_PARAGRAPH_SECONDS:
            groups.append([start, []])
        groups[-1][1].append(text)
    return "\n\n".join(f"[{format_timestamp_ms(int(g[0] * 1000))}] {' '.join(g[1])}" for g in groups)


def fetch_youtube_transcript(video_url):
    """Panggil actor Apify (satu video per run) dan kembalikan transkrip ber-timestamp."""
    if not APIFY_TOKEN:
        raise RuntimeError("APIFY_TOKEN belum di-set di Environment Variable Render")
    url = APIFY_RUN_URL.format(actor=APIFY_ACTOR)
    last_err = "tidak diketahui"
    for attempt in (1, 2):
        try:
            r = requests.post(
                url,
                headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                json={"videoUrl": video_url, "targetLanguage": YT_TRANSCRIPT_LANG},
                timeout=330,
            )
        except requests.RequestException as e:
            last_err = "koneksi ke Apify gagal: " + str(e).replace(APIFY_TOKEN, "***")
        else:
            if r.status_code in (200, 201):
                break
            if r.status_code == 401:
                raise RuntimeError("token Apify ditolak (HTTP 401) - token salah atau sudah dicabut")
            if r.status_code == 402:
                raise RuntimeError("kredit/batas biaya Apify habis (HTTP 402)")
            last_err = f"Apify HTTP {r.status_code}: {r.text[:200]}"
            if r.status_code < 500:
                last_err += ". Kemungkinan video private/dihapus atau tidak punya caption."
            if r.status_code < 500 and r.status_code != 408:
                raise RuntimeError(last_err)
        if attempt == 1:
            time.sleep(5)
    else:
        raise RuntimeError(last_err)

    try:
        parsed = r.json()
    except ValueError:
        raise RuntimeError("respons Apify bukan JSON yang valid")
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict) and "data" in parsed[0]:
        segments = parsed[0]["data"]
    elif isinstance(parsed, list):
        segments = parsed
    elif isinstance(parsed, dict):
        segments = parsed.get("data")
    else:
        segments = None
    if not isinstance(segments, list):
        raise NoCaptions()
    segments = [sg for sg in segments if isinstance(sg, dict)]
    text = format_youtube_segments(segments) if segments else ""
    if not text:
        raise NoCaptions()
    return text


def process_youtube_video(creds, video, idx, total, job):
    tag = f"[Video {idx}/{total}]"
    name = video["path"]

    def fail_part(msg):
        log.error("%s %s", tag, msg)
        submit_part(job, idx, error=msg)

    try:
        log.info("%s Ambil transkrip YouTube via Apify: %s", tag, name)
        t0 = time.time()
        text = fetch_youtube_transcript(video["url"])
        log.info("%s OK - %d karakter (%.0f detik)", tag, len(text), time.time() - t0)
    except NoCaptions:
        fail_part(f"ERROR (tidak ada caption) - {name}: video ini tidak punya caption/transkrip yang bisa "
                  "diambil (dinonaktifkan pemilik, private, atau dihapus).")
    except Exception as e:
        fail_part(f"ERROR (transkrip YouTube) - {name}: {e}")
    else:
        submit_part(job, idx, text=text, source="caption YouTube (via Apify)")


# ------------------------------------------------- transkrip yang sudah ada di Drive

_CUE_TS = re.compile(r"(?:(\d+):)?(\d{1,2}):(\d{2})[.,](\d{1,3})\s*-->")


def parse_subtitles(text):
    """Isi .vtt / .srt -> [{'start': detik, 'text': ...}]."""
    segs, start, lines = [], None, []
    for raw in text.split("\n") + [""]:
        line = raw.strip()
        m = _CUE_TS.search(line)
        if m:
            if start is not None and lines:
                segs.append({"start": start, "text": " ".join(lines)})
            h, mi, sec, ms = m.groups()
            start = int(h or 0) * 3600 + int(mi) * 60 + int(sec) + int(ms.ljust(3, "0")[:3]) / 1000
            lines = []
        elif not line:
            if start is not None and lines:
                segs.append({"start": start, "text": " ".join(lines)})
            start, lines = None, []
        elif start is not None:
            lines.append(re.sub(r"<[^>]+>", "", line).strip())
    return [sg for sg in segs if sg["text"].strip()]


def read_drive_file_bytes(drive, file_id, max_bytes=8 * 1024 * 1024):
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id, supportsAllDrives=True),
                             chunksize=4 * 1024 * 1024)
    done = False
    while not done:
        _, done = dl.next_chunk(num_retries=3)
        if buf.tell() > max_bytes:
            raise RuntimeError("file transkrip terlalu besar")
    return buf.getvalue()


def read_sidecar_text(drive, sc):
    """Isi file transkrip di Drive -> teks siap ditulis ke sel. Melempar error kalau tidak layak dipakai."""
    kind = sc["kind"]
    if kind == "gdoc":
        data = drive.files().export(fileId=sc["id"], mimeType="text/plain").execute()
        raw = data if isinstance(data, (bytes, bytearray)) else str(data).encode("utf-8")
    else:
        raw = read_drive_file_bytes(drive, sc["id"])
    text = raw.decode("utf-8-sig", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    if kind in ("vtt", "srt"):
        segs = parse_subtitles(text)
        segs = [sg for i, sg in enumerate(segs) if i == 0 or sg["text"] != segs[i - 1]["text"]]
        body = format_youtube_segments(segs, guess_units=False)
    else:
        body = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(re.sub(r"\s+", "", body)) < MIN_TRANSCRIPT_CHARS:
        raise RuntimeError("isi file transkrip kosong/terlalu pendek")
    return body


def upload_via_disk(creds, drive, video, tag, state):
    """Jalur teruji: download ke /tmp, upload, lalu file langsung dihapus."""
    tmp_path = None
    try:
        state["stage"] = "download Drive"
        log.info("%s Tahap 1/3 Download: %s", tag, video["path"])
        request_media = drive.files().get_media(fileId=video["id"], supportsAllDrives=True)
        tmp = tempfile.NamedTemporaryFile(
            delete=False, suffix=os.path.splitext(video["name"])[1] or ".bin"
        )
        tmp_path = tmp.name
        tmp.close()

        fh = io.FileIO(tmp_path, "wb")
        downloader = MediaIoBaseDownload(fh, request_media, chunksize=10 * 1024 * 1024)
        done, last_pct = False, -1
        while not done:
            status, done = downloader.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                if pct // 20 != last_pct // 20:  # log tiap ~20%
                    log.info("%s   progress download: %d%%", tag, pct)
                    last_pct = pct
        fh.close()
        log.info("%s Tahap 1/3 OK - %.1f MB", tag, os.path.getsize(tmp_path) / 1024 / 1024)

        state["stage"] = "upload AssemblyAI"
        log.info("%s Tahap 2/3 Upload ke AssemblyAI...", tag)
        with open(tmp_path, "rb") as f:
            resp = requests.post(
                ASSEMBLYAI_UPLOAD_URL,
                headers={"authorization": ASSEMBLYAI_API_KEY},
                data=f,
                timeout=(15, 1800),
            )
        resp.raise_for_status()
        log.info("%s Tahap 2/3 OK", tag)
        return resp.json()["upload_url"]
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def format_hms(seconds):
    seconds = int(seconds)
    return f"{seconds // 3600}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def probe_duration(token, file_id):
    """Durasi video (detik) supaya progres bisa ditampilkan dalam persen. None kalau gagal."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-headers", f"Authorization: Bearer {token}\r\n",
             "-show_entries", "format=duration", "-of", "default=nw=1:nk=1",
             DRIVE_MEDIA_URL.format(file_id=file_id)],
            capture_output=True, text=True, timeout=120,
        )
        return float(r.stdout.strip()) if r.returncode == 0 and r.stdout.strip() else None
    except Exception:
        return None


def diagnose_drive_error(token, file_id):
    """ffmpeg hanya melaporkan '403 Forbidden'. Minta 1 byte lewat API biasa untuk membaca
    alasan asli dari Drive. Hasil: (status, reason, message) atau None kalau tidak bisa."""
    try:
        r = requests.get(
            DRIVE_MEDIA_URL.format(file_id=file_id),
            headers={"Authorization": f"Bearer {token}", "Range": "bytes=0-0"},
            timeout=30,
        )
        if r.status_code < 400:
            return None
        err = r.json().get("error", {})
        reason = ((err.get("errors") or [{}])[0]).get("reason", "")
        return r.status_code, reason, err.get("message", "")
    except Exception:
        return None


def explain_drive_error(status, reason, message, sa_email):
    base = f"Drive menolak akses (HTTP {status}, alasan: {reason or 'tidak disebut'} - {message})."
    if reason == "downloadQuotaExceeded":
        return (base + " Kuota download file ini habis (batas dari Google per file). "
                "Coba lagi beberapa jam atau 24 jam lagi, atau minta pemilik membuat salinan file.")
    if status == 403:
        return (base + " Kemungkinan pemilik/admin melarang download oleh viewer (opsi 'Batasi download'). "
                f"Minta pemilik mengizinkan download, atau beri akses Editor ke {sa_email}.")
    if status == 404:
        return base + f" File tidak ditemukan atau belum di-share ke {sa_email}."
    return base


def extract_audio_via_ffmpeg(creds, file_id, out_path, tag=""):
    """ffmpeg membaca video langsung dari Drive lewat HTTP (mendukung Range, jadi MP4
    yang metadata-nya di akhir file pun aman), membuang video, menyimpan audionya.
    Progres ditulis ke log tiap PROGRESS_LOG_EVERY_S detik."""
    creds.refresh(GoogleAuthRequest())
    token = creds.token
    duration = probe_duration(token, file_id)
    if duration:
        log.info("%s   durasi video: %s", tag, format_hms(duration))
    cmd = [
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-nostats",
        "-progress", "pipe:1",
        "-headers", f"Authorization: Bearer {token}\r\n",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "10",
        "-i", DRIVE_MEDIA_URL.format(file_id=file_id),
        "-vn", "-ac", "1", "-ar", "16000", "-c:a", "libmp3lame", "-b:a", AUDIO_BITRATE,
        "-y", out_path,
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    timed_out = {"v": False}

    def _kill():
        timed_out["v"] = True
        proc.kill()

    timer = threading.Timer(FFMPEG_TIMEOUT_S, _kill)
    timer.start()
    err_lines, cur, speed, last_log = [], None, "", time.time()
    try:
        for raw in proc.stdout:
            line = raw.strip()
            m = re.match(r"^([a-z_0-9]+)=(.*)$", line)
            if not m:
                if line:
                    err_lines.append(line)
                continue
            key, val = m.groups()
            if key in ("out_time_us", "out_time_ms") and val.lstrip("-").isdigit() and int(val) > 0:
                cur = int(val) / 1_000_000
            elif key == "speed":
                speed = val
            now = time.time()
            if cur is not None and now - last_log >= PROGRESS_LOG_EVERY_S:
                last_log = now
                pct = f" ({min(99, int(cur * 100 / duration))}%)" if duration else ""
                total = f" dari {format_hms(duration)}" if duration else ""
                log.info("%s   ekstrak audio: %s%s%s, kecepatan %s", tag, format_hms(cur), total, pct, speed or "?")
        rc = proc.wait()
    finally:
        timer.cancel()
        if proc.poll() is None:
            proc.kill()
    if timed_out["v"]:
        raise RuntimeError(f"ffmpeg melebihi batas waktu {FFMPEG_TIMEOUT_S} detik")
    if rc != 0:
        err = "\n".join(err_lines[-6:]).replace(token, "***").strip()
        if "does not contain any stream" in err:
            raise RuntimeError("video ini tidak punya track audio")
        if re.search(r"Server returned 4\d\d", err):
            diag = diagnose_drive_error(token, file_id)
            if diag:
                sa = getattr(creds, "service_account_email", "service account")
                raise RuntimeError(explain_drive_error(*diag, sa))
        raise RuntimeError("ffmpeg gagal: " + err[-300:])


def upload_via_audio(creds, drive, video, tag, state):
    """Untuk video besar: ekstrak audionya saja (tanpa menyimpan videonya), lalu upload audio."""
    tmp_path = None
    try:
        state["stage"] = "ekstrak audio (ffmpeg)"
        log.info("%s Tahap 1/3 Ekstrak audio dari video %.0f MB (videonya tidak disimpan): %s",
                 tag, (video.get("size") or 0) / 1024 / 1024, video["path"])
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3")
        tmp_path = tmp.name
        tmp.close()

        t0 = time.time()
        extract_audio_via_ffmpeg(creds, video["id"], tmp_path, tag)
        out_mb = os.path.getsize(tmp_path) / 1024 / 1024
        if out_mb <= 0:
            raise RuntimeError("hasil ekstraksi audio kosong")
        log.info("%s Tahap 1/3 OK - audio %.1f MB (%.0f detik)", tag, out_mb, time.time() - t0)

        state["stage"] = "upload AssemblyAI"
        log.info("%s Tahap 2/3 Upload audio ke AssemblyAI...", tag)
        with open(tmp_path, "rb") as f:
            resp = requests.post(
                ASSEMBLYAI_UPLOAD_URL,
                headers={"authorization": ASSEMBLYAI_API_KEY},
                data=f,
                timeout=(15, 1800),
            )
        resp.raise_for_status()
        log.info("%s Tahap 2/3 OK", tag)
        return resp.json()["upload_url"]
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def process_one_video(creds, drive, video, idx, total, job):
    tag = f"[Video {idx}/{total}]"
    name = video["path"]

    def fail_part(msg):
        log.error("%s %s", tag, msg)
        submit_part(job, idx, error=msg)

    sc = video.get("sidecar")
    if sc:
        body = None
        try:
            body = read_sidecar_text(drive, sc)
        except Exception as e:
            log.warning("%s Transkrip Drive '%s' tidak bisa dipakai (%s) - lanjut ke AssemblyAI", tag, sc["name"], e)
        if body is not None:
            log.info("%s Transkrip sudah ada di Drive (%s: %s) - dipakai, AssemblyAI dilewati", tag, sc["kind"], sc["name"])
            submit_part(job, idx, text=body, source=f"file transkrip di Drive - {sc['name']}")
            return

    if video.get("can_download") is False:
        sa = getattr(creds, "service_account_email", "service account")
        fail_part(f"ERROR (tidak boleh di-download) - {name}: pemilik/admin membatasi download untuk akun ini "
                  f"(kemampuan 'canDownload' = false). Minta pemilik mengizinkan download, atau beri akses "
                  f"Editor ke {sa}. Video ini dilewati; video lain tetap diproses.")
        return

    size = video.get("size")
    use_audio = bool(size and size > DISK_MAX_BYTES)
    state = {"stage": "download Drive"}
    try:
        upload_url = (upload_via_audio if use_audio else upload_via_disk)(creds, drive, video, tag, state)

        state["stage"] = "submit AssemblyAI"
        log.info("%s Tahap 3/3 Submit job (via webhook)...", tag)
        # job/idx/total: untuk menggabungkan semua video satu baris ke satu file .md.
        # candidate/project/label ikut dikirim sebagai cadangan kalau job di memori hilang (restart).
        params = {"job": job.id, "idx": idx, "total": total, "sheet_id": job.sheet_id,
                  "tab_name": job.tab_name, "row": job.row, "col": job.col,
                  "candidate": job.candidate, "project": job.project, "label": name}
        webhook_url = f"{BASE_URL}/webhook?{urlencode(params)}"
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
            timeout=60,
        )
        submit_resp.raise_for_status()
        log.info("%s Tahap 3/3 OK - job id=%s, menunggu webhook", tag, submit_resp.json().get("id"))
    except Exception as e:
        fail_part(f"ERROR ({state['stage']}) - {name}: {e}")


@app.get("/manual-recover")
def manual_recover(transcript_id: str, sheet_id: str, row: int, candidate: str, project: str,
                   tab_name: str = "Initial Assessment V.3", col: str = "H", label: str = "",
                   token: str = ""):
    """Endpoint darurat: tarik ulang transkrip yang sudah 'completed' di AssemblyAI
    tapi webhook-nya gagal nyampe. Dipanggil manual lewat browser.
    Membuat/memperbarui file .md kandidat HANYA dengan satu transkrip ini (jadi kalau barisnya
    punya beberapa video, jalankan ulang barisnya lewat menu, bukan lewat endpoint ini)."""
    if not APP_SECRET_TOKEN or token != APP_SECRET_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    creds = get_credentials()
    try:
        r = requests.get(
            f"https://api.assemblyai.com/v2/transcript/{transcript_id}",
            headers={"authorization": ASSEMBLYAI_API_KEY},
            timeout=60,
        )
        r.raise_for_status()
        status = r.json().get("status")
        if status != "completed":
            write_cell(creds, sheet_id, tab_name, row, col, f"ERROR (status AssemblyAI: {status})")
            return {"ok": False, "status": status}
        text = fetch_timestamped_transcript(transcript_id)
        job = RowJob(uuid.uuid4().hex[:12],
                     [{"path": label or f"AssemblyAI {transcript_id}", "link": ""}], "",
                     candidate.strip(), project.strip(), sheet_id, tab_name, row, col.upper())
        submit_part(job, 1, text=text, source="AssemblyAI (pemulihan manual)")
        return job.result or {"ok": False, "error": "job tidak selesai"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_webhook(q, body):
    """Dijalankan di threadpool (banyak panggilan jaringan) supaya event loop tidak tersumbat."""
    sheet_id = q.get("sheet_id")
    tab_name = q.get("tab_name", "Initial Assessment V.3")
    row = int(q.get("row", 0))
    col = q.get("col", "H")
    label = q.get("label", "")
    job_id = q.get("job", "")
    idx = int(q.get("idx", 1))
    total = int(q.get("total", 1))
    candidate, project = q.get("candidate", "").strip(), q.get("project", "").strip()

    transcript_id = body.get("transcript_id")
    status = body.get("status")
    log.info("[WEBHOOK] row=%s kolom=%s video=%s/%s transcript_id=%s status=%s",
             row, col, idx, total, transcript_id, status)

    creds = get_credentials()
    job = get_job(job_id) if job_id else None
    if job is None:
        if total == 1 and candidate and project:
            # Job di memori hilang (restart) tapi barisnya cuma satu video: bangun ulang dari parameter URL.
            log.warning("[WEBHOOK] job %s tidak ada di memori - dibangun ulang dari parameter webhook", job_id)
            job = RowJob(job_id or uuid.uuid4().hex[:12], [{"path": label or "video", "link": ""}], "",
                         candidate, project, sheet_id, tab_name, row, col)
        else:
            msg = ("ERROR: backend sempat restart di tengah proses sehingga hasil baris ini tidak bisa "
                   "digabung. Kosongkan sel ini lalu jalankan ulang barisnya.")
            log.error("[WEBHOOK] %s (job=%s, video %s/%s)", msg, job_id, idx, total)
            write_cell(creds, sheet_id, tab_name, row, col, msg)
            return

    if status != "completed":
        detail = describe_failure(transcript_id, status)
        label_part = f" - {label}" if label else ""
        submit_part(job, idx, error=f"ERROR (AssemblyAI {status}){label_part}: {detail}")
        return

    try:
        text = fetch_timestamped_transcript(transcript_id)
    except Exception as e:
        log.error("[WEBHOOK] Gagal ambil hasil: %s", e)
        submit_part(job, idx, error=f"ERROR (ambil hasil transkrip) - {label}: {e}")
        return
    submit_part(job, idx, text=text, source="AssemblyAI (Universal-2)")


@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        log.warning("[WEBHOOK] Ditolak - secret nggak cocok")
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await request.json()
    await run_in_threadpool(handle_webhook, dict(request.query_params), body)
    return {"ok": True}
