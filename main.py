"""
Backend transkripsi video Drive -> AssemblyAI -> tulis hasil ke Google Sheet.

Input `file_id` boleh berupa ID FILE video ATAU ID FOLDER Drive:
  Case 1: link langsung ke satu video
  Case 2: link folder berisi beberapa video (file non-video dilewati, bukan error)
  Case 3: link folder yang videonya ada di subfolder (dicari rekursif,
          sampai kedalaman MAX_FOLDER_DEPTH)

Hasil: satu video = satu file Markdown (.md), disimpan lewat Web App Apps Script milik
satu akun Google (folder Talent Assessment/<project>/). Di sheet, HANYA SATU SEL
(`output_col`, mis. kolom G) yang ditulis: satu baris per video, berformat
    [k/N] Judul video: https://drive.google.com/file/d/.../view     (selesai)
    [k/N] Diproses: ... / [k/N] ERROR (...) ...                       (status)
`max_videos` kini hanya batas jumlah video per baris (pengaman kredit), BUKAN jumlah kolom.

Alur:
  POST /transcribe (dari Apps Script, header Authorization: Bearer <APP_SECRET_TOKEN>)
    -> langsung balas "diterima", lalu di background:
       Resolve : cek file/folder, kumpulkan daftar video
       Per video (satu per satu): ambil video/audio -> upload AssemblyAI -> submit job + webhook
       (video kecil di-download utuh; video besar cukup diekstrak audionya lewat ffmpeg)
  POST /webhook (dipanggil AssemblyAI per video yang selesai)
    -> ambil transkrip ber-timestamp, simpan sebagai .md (Web App), tulis link-nya ke sel

Kalau ada tahap yang gagal, pesan error ditulis di baris video terkait (video lain
tetap jalan). Detail lengkap ada di tab "Logs" Render.
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
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlparse, parse_qs

from fastapi import FastAPI, Request, HTTPException
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

# --- Penyimpanan hasil: file .md dibuat oleh Web App Apps Script (berjalan atas nama pemilik
#     folder Talent Assessment), karena service account tidak punya kuota Drive sendiri.
WEBAPP_URL = os.environ.get("WEBAPP_URL")       # URL "/exec" Web App penerima
WEBAPP_TOKEN = os.environ.get("WEBAPP_TOKEN")   # sama persis dengan Script Property WEBAPP_TOKEN di Web App


class TranscribeRequest(BaseModel):
    file_ids: list[str] = []   # satu atau lebih ID file video / folder (dari satu sel)
    file_id: str = ""          # kompatibilitas versi lama (satu ID)
    youtube_urls: list[str] = []   # link video / playlist YouTube (dari sel yang sama)
    sheet_id: str
    tab_name: str = "Sheet1"
    row: int
    output_col: str = "B"   # kolom pertama untuk hasil
    max_videos: int = 1     # batas jumlah video per baris (pengaman kredit)
    candidate_name: str = ""   # nama kandidat (kolom A) -> dipakai untuk nama file .md
    project: str = ""          # nama project (kolom D) -> menentukan subfolder tujuan


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


def write_block(creds, sheet_id, tab_name, row, start_col, texts):
    end_col = num_to_col(col_to_num(start_col) + len(texts) - 1)
    rng = a1(tab_name, start_col, row) + f":{end_col}{row}"
    sheets = build("sheets", "v4", credentials=creds)
    sheets.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=rng,
        valueInputOption="RAW",
        body={"values": [[fit_cell(t) for t in texts]]},
    ).execute()


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


def build_cell_text(transcript_id, label):
    body = fetch_timestamped_transcript(transcript_id)
    return f"== {label} ==\n\n{body}" if label else body


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


# ------------------------- hasil -> file .md (Web App) + status per video di SATU sel

SLOT_RE = re.compile(r"^\[(\d+)/(\d+)\]\s")
_SLOT_LOCK = threading.Lock()   # webhook beberapa video bisa datang bersamaan -> baca-ubah-tulis sel harus bergantian
WIB = timezone(timedelta(hours=7))


def one_line(text, limit=600):
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s if len(s) <= limit else s[:limit - 1] + "…"


def read_cell(creds, sheet_id, tab_name, col, row):
    sheets = build("sheets", "v4", credentials=creds)
    res = sheets.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=a1(tab_name, col, row)).execute()
    vals = res.get("values") or []
    return vals[0][0] if vals and vals[0] else ""


def parse_slots(text):
    slots, warns = {}, []
    for line in str(text).split("\n"):
        m = SLOT_RE.match(line)
        if m:
            slots[int(m.group(1))] = line
        elif line.startswith("PERINGATAN:"):
            warns.append(line)
    return slots, warns


def render_slots(slots, total, warns):
    lines = list(warns)
    for i in range(1, total + 1):
        lines.append(slots.get(i, f"[{i}/{total}] Diproses: menunggu"))
    return "\n".join(lines)


def init_slots(creds, sheet_id, tab_name, row, col, total, texts, warn=""):
    """Tulis status awal semua video sekaligus (menimpa isi sel)."""
    lines = []
    if warn:
        lines.append("PERINGATAN: " + one_line(warn, 800))
    lines += [f"[{i + 1}/{total}] {one_line(t)}" for i, t in enumerate(texts)]
    with _SLOT_LOCK:
        write_cell(creds, sheet_id, tab_name, row, col, "\n".join(lines))


def update_slot(creds, sheet_id, tab_name, row, col, idx, total, text):
    """Ubah baris video ke-idx saja; baris video lain dipertahankan."""
    line = f"[{idx}/{total}] {one_line(text)}"
    with _SLOT_LOCK:
        try:
            current = read_cell(creds, sheet_id, tab_name, col, row)
        except Exception as e:
            log.warning("Gagal membaca sel sebelum update (%s) - memakai sel kosong", e)
            current = ""
        slots, warns = parse_slots(current)
        slots[idx] = line
        write_cell(creds, sheet_id, tab_name, row, col, render_slots(slots, total, warns))


def compose_md(title, body, cand, proj, src_video="", src_note=""):
    meta = []
    if cand:
        meta.append(f"- Kandidat: {cand}")
    if proj:
        meta.append(f"- Project: {proj}")
    if src_video:
        meta.append(f"- Video: {src_video}")
    if src_note:
        meta.append(f"- Sumber transkrip: {src_note}")
    meta.append(f"- Dibuat: {datetime.now(WIB).strftime('%Y-%m-%d %H:%M')} WIB")
    return f"# Transkrip: {one_line(title, 300)}\n\n" + "\n".join(meta) + "\n\n---\n\n" + body.strip() + "\n"


def save_markdown(title, content, cand, proj):
    """Kirim ke Web App Apps Script -> file .md di Drive -> kembalikan URL file.
    Aman diulang: nama file yang sama ditimpa, bukan diduplikasi."""
    if not WEBAPP_URL or not WEBAPP_TOKEN:
        raise RuntimeError("WEBAPP_URL / WEBAPP_TOKEN belum di-set di Environment Variable Render")
    payload = {"token": WEBAPP_TOKEN, "candidate": cand, "project": proj,
               "title": title, "content": content}
    last = None
    for attempt in range(1, 4):
        try:
            r = requests.post(WEBAPP_URL, json=payload, timeout=180)
            try:
                data = r.json()
            except ValueError:
                raise RuntimeError(f"Web App membalas bukan JSON (status {r.status_code}). Cek deployment: "
                                   f"'Execute as: Me' + akses 'Anyone'. Awal balasan: {r.text[:120]!r}")
            if not data.get("ok"):
                raise RuntimeError(f"Web App menolak: {data.get('error')}")
            return data["url"]
        except Exception as e:
            last = e
            log.warning("[save_markdown] percobaan %d/3 gagal: %s", attempt, e)
            if attempt < 3:
                time.sleep(3 * attempt)
    raise last


def deliver_transcript(creds, sheet_id, tab_name, row, col, idx, total, title, body, cand, proj,
                       src_video="", src_note=""):
    md = compose_md(title, body, cand, proj, src_video, src_note)
    url = save_markdown(title, md, cand, proj)
    update_slot(creds, sheet_id, tab_name, row, col, idx, total, f"{title}: {url}")
    return url, len(md)


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
    return {"status": "ok", "ffmpeg": shutil.which("ffmpeg") is not None}


@app.post("/transcribe")
def transcribe(req: TranscribeRequest, request: Request):
    auth_header = request.headers.get("authorization", "")
    if not APP_SECRET_TOKEN or auth_header != f"Bearer {APP_SECRET_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    output_col = re.sub(r"[^A-Za-z]", "", req.output_col).upper() or "B"
    max_videos = max(1, min(req.max_videos, HARD_CAP_VIDEOS))
    root_ids = list(dict.fromkeys(i for i in (req.file_ids or [req.file_id]) if i))
    youtube_urls = list(dict.fromkeys(u.strip() for u in req.youtube_urls if u and u.strip()))
    if not root_ids and not youtube_urls:
        raise HTTPException(status_code=400, detail="tidak ada link Drive/YouTube")
    log.info("Request diterima: %d link Drive, %d link YouTube, tab=%s, row=%s, kolom=%s, maks_video=%s",
             len(root_ids), len(youtube_urls), req.tab_name, req.row, output_col, max_videos)
    JOBS.put((root_ids, youtube_urls, req.sheet_id, req.tab_name, req.row, output_col, max_videos,
              req.candidate_name.strip(), req.project.strip()))
    waiting = JOBS.qsize()
    log.info("[Antrean] baris %s masuk antrean (menunggu giliran: %d)", req.row, waiting)
    return {"status": "diterima, masuk antrean", "menunggu_di_depan": waiting}


def process_request(root_ids, youtube_urls, sheet_id, tab_name, row, output_col, max_videos,
                    candidate="", project=""):
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
            fail(f"ERROR: ditemukan {total} video, melebihi batas {max_videos} video per baris. "
                 f"Pecah folder/playlist-nya, atau naikkan MAX_VIDEOS_PER_ROW di Apps Script (maks {HARD_CAP_VIDEOS}).")
            return

        # --- Status awal per video: SATU sel (kolom output_col), satu baris per video.
        init_slots(creds, sheet_id, tab_name, row, output_col, total,
                   [f"Diproses: {v['path']} - dalam antrean" for v in videos],
                   "; ".join(notes))

        # --- Proses satu per satu (hemat RAM & disk di Render free)
        for i, video in enumerate(videos):
            if video.get("kind") == "youtube":
                process_youtube_video(creds, video, i + 1, total, sheet_id, tab_name, row, output_col,
                                      candidate, project)
            else:
                process_one_video(creds, drive, video, i + 1, total, sheet_id, tab_name, row, output_col,
                                  candidate, project)

        log.info("[Selesai] Semua %d video diproses; transkrip video Drive menyusul lewat webhook.", total)
    except Exception as e:
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
                           "url": f"https://www.youtube.com/watch?v={vid}"})
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


def process_youtube_video(creds, video, idx, total, sheet_id, tab_name, row, col, cand="", proj=""):
    tag = f"[Video {idx}/{total}]"
    name = video["path"]

    def write_err(msg):
        log.error("%s %s", tag, msg)
        try:
            update_slot(creds, sheet_id, tab_name, row, col, idx, total, msg)
        except Exception as e2:
            log.error("%s Gagal menulis pesan error ke sheet: %s", tag, e2)

    try:
        log.info("%s Ambil transkrip YouTube via Apify: %s", tag, name)
        t0 = time.time()
        text = fetch_youtube_transcript(video["url"])
        url, n = deliver_transcript(creds, sheet_id, tab_name, row, col, idx, total, name, text, cand, proj,
                                    src_video=video["url"], src_note="caption YouTube (Apify)")
        log.info("%s OK - %d karakter transkrip, file .md: %s (%.0f detik)", tag, len(text), url, time.time() - t0)
    except NoCaptions:
        write_err(f"ERROR (tidak ada caption) - {name}: video ini tidak punya caption/transkrip yang bisa "
                  "diambil (dinonaktifkan pemilik, private, atau dihapus).")
    except Exception as e:
        write_err(f"ERROR (transkrip YouTube) - {name}: {e}")


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


def process_one_video(creds, drive, video, idx, total, sheet_id, tab_name, row, col, cand="", proj=""):
    tag = f"[Video {idx}/{total}]"
    name = video["path"]
    src_video = f"https://drive.google.com/file/d/{video['id']}/view"

    def write_err(msg):
        log.error("%s %s", tag, msg)
        try:
            update_slot(creds, sheet_id, tab_name, row, col, idx, total, msg)
        except Exception as e2:
            log.error("%s Gagal menulis pesan error ke sheet: %s", tag, e2)

    sc = video.get("sidecar")
    if sc:
        body = None
        try:
            body = read_sidecar_text(drive, sc)
        except Exception as e:
            log.warning("%s Transkrip Drive '%s' tidak bisa dipakai (%s) - lanjut ke AssemblyAI", tag, sc["name"], e)
        if body is not None:
            try:
                url, n = deliver_transcript(creds, sheet_id, tab_name, row, col, idx, total, name, body, cand, proj,
                                            src_video=src_video,
                                            src_note=f"file transkrip di Drive - {sc['name']}")
                log.info("%s Transkrip sudah ada di Drive (%s: %s) - dipakai, AssemblyAI dilewati. File .md: %s",
                         tag, sc["kind"], sc["name"], url)
            except Exception as e:
                write_err(f"ERROR (simpan file .md) - {name}: {e}")
            return

    if video.get("can_download") is False:
        sa = getattr(creds, "service_account_email", "service account")
        write_err(f"ERROR (tidak boleh di-download) - {name}: pemilik/admin membatasi download untuk akun ini "
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
        params = {"sheet_id": sheet_id, "tab_name": tab_name, "row": row, "col": col,
                  "idx": idx, "total": total, "label": name,
                  "cand": cand, "proj": proj, "src": src_video}
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
        write_err(f"ERROR ({state['stage']}) - {name}: {e}")


def finish_assemblyai(creds, transcript_id, sheet_id, tab_name, row, col, idx, total, title, cand, proj, src):
    body = fetch_timestamped_transcript(transcript_id)
    return deliver_transcript(creds, sheet_id, tab_name, row, col, idx, total, title, body, cand, proj,
                              src_video=src,
                              src_note=f"AssemblyAI (Universal-2, bahasa {TRANSCRIPT_LANGUAGE_CODE})")


@app.get("/manual-recover")
def manual_recover(transcript_id: str, sheet_id: str, row: int, tab_name: str = "Sheet1",
                   col: str = "G", label: str = "", token: str = "",
                   idx: int = 1, total: int = 1, cand: str = "", proj: str = "", src: str = ""):
    """Endpoint darurat: tarik ulang transkrip yang sudah 'completed' di AssemblyAI
    tapi webhook-nya gagal nyampe. Dipanggil manual lewat browser."""
    if not APP_SECRET_TOKEN or token != APP_SECRET_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    creds = get_credentials()
    title = label or f"video {idx}"
    try:
        r = requests.get(
            f"https://api.assemblyai.com/v2/transcript/{transcript_id}",
            headers={"authorization": ASSEMBLYAI_API_KEY},
            timeout=60,
        )
        r.raise_for_status()
        status = r.json().get("status")
        if status != "completed":
            update_slot(creds, sheet_id, tab_name, row, col, idx, total,
                        f"ERROR (status AssemblyAI: {status}) - {title}")
            return {"ok": False, "status": status}
        url, n = finish_assemblyai(creds, transcript_id, sheet_id, tab_name, row, col, idx, total,
                                   title, cand, proj, src)
        return {"ok": True, "chars": n, "url": url}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        log.warning("[WEBHOOK] Ditolak - secret nggak cocok")
        raise HTTPException(status_code=401, detail="Unauthorized")

    q = request.query_params
    sheet_id = q.get("sheet_id")
    tab_name = q.get("tab_name", "Sheet1")
    row = int(q.get("row", 0))
    col = q.get("col", "G")
    idx = int(q.get("idx", 1) or 1)
    total = int(q.get("total", 1) or 1)
    title = q.get("label", "") or f"video {idx}"
    cand = q.get("cand", "")
    proj = q.get("proj", "")
    src = q.get("src", "")

    body = await request.json()
    transcript_id = body.get("transcript_id")
    status = body.get("status")
    log.info("[WEBHOOK] row=%s kolom=%s video=%s/%s transcript_id=%s status=%s",
             row, col, idx, total, transcript_id, status)

    def work():
        try:
            creds = get_credentials()
            if status != "completed":
                detail = describe_failure(transcript_id, status)
                update_slot(creds, sheet_id, tab_name, row, col, idx, total,
                            f"ERROR (AssemblyAI {status}) - {title}: {detail}")
                return
            try:
                url, n = finish_assemblyai(creds, transcript_id, sheet_id, tab_name, row, col, idx, total,
                                           title, cand, proj, src)
                log.info("[WEBHOOK] SELESAI - file .md: %s (baris %s, video %s/%s)", url, row, idx, total)
            except Exception as e:
                log.error("[WEBHOOK] Gagal ambil/simpan hasil: %s", e)
                update_slot(creds, sheet_id, tab_name, row, col, idx, total,
                            f"ERROR (ambil/simpan transkrip) - {title}: {e}")
        except Exception as e:
            log.error("[WEBHOOK] error tak terduga: %s", e)

    # Dijalankan di thread terpisah supaya AssemblyAI langsung dapat balasan 200
    # (menyimpan .md lewat Web App bisa makan beberapa detik; webhook yang lambat memicu pengiriman ulang).
    threading.Thread(target=work, daemon=True, name=f"webhook-{row}-{idx}").start()
    return {"ok": True}
