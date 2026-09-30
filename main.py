"""
Backend transkripsi video (Drive + YouTube) -> tulis hasil ke Google Sheet.

Input boleh berupa link file video / folder Drive (subfolder dicari rekursif) dan
link video / playlist YouTube. Satu permintaan = satu baris sheet.

HASIL: SEMUA video dari satu baris ditulis di SATU SEL (`output_col`), dengan format:

    -- Judul video 1, link video 1 --
    [00:00] transkrip ...

    -- Judul video 2, link video 2 --
    [00:00] transkrip ...

Video Drive selesai lewat webhook AssemblyAI (urutan selesai bisa acak), video YouTube
selesai langsung. Karena itu setiap video hanya mengganti BLOK-nya sendiri di dalam sel
(baca sel -> ganti blok yang linknya cocok -> tulis balik, dilindungi lock per sel).
Sel sheet sendiri menjadi penyimpan status, jadi aman kalau instance Render restart.

Batas 50.000 karakter per sel Google Sheets:
  Tiap video mendapat jatah karakter (batas sel dibagi jumlah video). Kalau transkripnya
  lebih panjang, bagian yang muat tetap tampil di sel dan SISANYA disimpan utuh di tab
  "Transkrip Lengkap" (dibuat otomatis) dengan penanda [TERPOTONG ... kunci: XXXX] di sel.
  Apps Script menyambungkan kembali teks lengkapnya saat Sync ke Coda.

Status per blok:
  "Diproses: ..."  -> masih berjalan
  "ERROR (...)"    -> video itu gagal (video lain tetap jalan)
  selain itu       -> transkrip selesai
Status seluruh sel (tanpa blok): "ERROR: ..." atau "Tidak ada video: ...".

Detail lengkap tiap tahap ada di tab "Logs" Render.
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
#     API key yang sama juga dipakai untuk mengambil JUDUL video yang dikirim lewat link langsung.
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
APIFY_ACTOR = os.environ.get("APIFY_ACTOR", "pintostudio~youtube-transcript-scraper")
YT_TRANSCRIPT_LANG = os.environ.get("YT_TRANSCRIPT_LANG", "id")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
YT_PARAGRAPH_SECONDS = 30   # caption digabung jadi paragraf tiap ~30 detik, diawali [MM:SS]
APIFY_RUN_URL = "https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
YOUTUBE_PLAYLIST_URL = "https://www.googleapis.com/youtube/v3/playlistItems"
YOUTUBE_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
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

# Batas Google Sheets: 50.000 karakter per sel.
MAX_CELL_CHARS = 49000
MIN_VIDEO_CAP = 1500          # jatah minimum per video (aman untuk maks 20 video)
CAP_MARGIN = 250              # ruang untuk penanda [TERPOTONG ...] / [PERINGATAN ...]

# Tab penampung sisa transkrip yang tidak muat di sel. Harus sama dengan
# CONFIG.TRANSCRIPT.OVERFLOW_TAB di Apps Script.
OVERFLOW_TAB = "Transkrip Lengkap"
OVERFLOW_CHUNK = 45000


class TranscribeRequest(BaseModel):
    file_ids: list[str] = []   # satu atau lebih ID file video / folder (dari satu sel)
    file_id: str = ""          # kompatibilitas versi lama (satu ID)
    youtube_urls: list[str] = []   # link video / playlist YouTube (dari sel yang sama)
    sheet_id: str
    tab_name: str = "Sheet1"
    row: int
    output_col: str = "B"   # kolom TUNGGAL untuk hasil (semua video digabung di sini)
    max_videos: int = 1     # batas jumlah video per baris (maks HARD_CAP_VIDEOS)


# ---------------------------------------------------------------- helper umum

def get_credentials():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def a1(tab_name, col, row):
    # Nama tab berspasi (mis. "Initial Assessment") wajib dikutip di notasi A1.
    safe_tab = tab_name.replace("'", "''")
    return f"'{safe_tab}'!{col}{row}"


def fit_cell(text):
    """Pengaman terakhir. Kondisi normal tidak pernah sampai sini karena tiap video sudah
    dibatasi jatahnya (lihat per_video_cap / fit_body)."""
    if len(text) <= MAX_CELL_CHARS:
        return text
    cut = text[:MAX_CELL_CHARS]
    idx = cut.rfind("\n\n")
    if idx > MAX_CELL_CHARS * 0.8:
        cut = cut[:idx]
    hilang = len(text) - len(cut)
    return (cut + "\n\n[TERPOTONG: isi sel melebihi batas 50.000 karakter per sel "
            f"Google Sheets. {hilang} karakter terakhir tidak ditampilkan.]")


def write_cell(creds, sheet_id, tab_name, row, col, text):
    sheets = build("sheets", "v4", credentials=creds)
    sheets.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=a1(tab_name, col, row),
        valueInputOption="RAW",
        body={"values": [[fit_cell(text)]]},
    ).execute()


def read_cell(creds, sheet_id, tab_name, row, col):
    sheets = build("sheets", "v4", credentials=creds)
    resp = sheets.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=a1(tab_name, col, row)
    ).execute()
    vals = resp.get("values") or []
    return str(vals[0][0]) if vals and vals[0] else ""


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


# ------------------------------------------- blok video di dalam SATU sel

HEADER_RE = re.compile(r"^-- .+ --$")


def clean_title(t):
    return re.sub(r"\s+", " ", str(t or "")).strip()


def make_header(title, link):
    return f"-- {clean_title(title) or link}, {link} --"


def parse_blocks(text):
    """Isi sel -> [(header, body), ...]. Baris pembuka blok = baris berbentuk '-- ... --'.
    Teks tanpa header (mis. pesan 'ERROR: ...' seluruh sel) menghasilkan daftar kosong."""
    blocks, cur = [], None
    for line in str(text or "").split("\n"):
        if HEADER_RE.match(line):
            cur = [line, []]
            blocks.append(cur)
        elif cur is not None:
            cur[1].append(line)
    return [(h, "\n".join(body).strip("\n")) for h, body in blocks]


def render_blocks(blocks):
    return "\n\n".join(h + ("\n" + b if b else "") for h, b in blocks)


def replace_block(blocks, header, link, body):
    """Ganti blok yang linknya cocok; kalau tidak ada, tambahkan di akhir."""
    suffix = f", {link} --"
    out = list(blocks)
    for i, (h, _) in enumerate(out):
        if h.endswith(suffix):
            out[i] = (header, body)
            return out
    out.append((header, body))
    return out


_CELL_LOCKS = {}
_CELL_LOCKS_GUARD = threading.Lock()


def cell_lock(sheet_id, tab_name, row, col):
    key = (sheet_id, tab_name, row, col)
    with _CELL_LOCKS_GUARD:
        return _CELL_LOCKS.setdefault(key, threading.Lock())


def set_block(creds, sheet_id, tab_name, row, col, header, link, body):
    """Ganti isi satu blok video tanpa menyentuh blok lain (baca-ubah-tulis, dengan lock)."""
    with cell_lock(sheet_id, tab_name, row, col):
        current = read_cell(creds, sheet_id, tab_name, row, col)
        blocks = replace_block(parse_blocks(current), header, link, body)
        write_cell(creds, sheet_id, tab_name, row, col, render_blocks(blocks))


def set_all_blocks(creds, sheet_id, tab_name, row, col, blocks):
    with cell_lock(sheet_id, tab_name, row, col):
        write_cell(creds, sheet_id, tab_name, row, col, render_blocks(blocks))


def per_video_cap(headers):
    """Jatah karakter transkrip per video supaya seluruh blok muat di satu sel."""
    n = max(1, len(headers))
    overhead = sum(len(h) for h in headers) + n * 4
    return max(MIN_VIDEO_CAP, (MAX_CELL_CHARS - overhead) // n - CAP_MARGIN)


# ------------------------------------------- overflow: sisa transkrip panjang

_OVERFLOW_READY = set()
_OVERFLOW_LOCK = threading.Lock()


def ensure_overflow_tab(sheets, sheet_id):
    with _OVERFLOW_LOCK:
        if sheet_id in _OVERFLOW_READY:
            return
        meta = sheets.spreadsheets().get(
            spreadsheetId=sheet_id, fields="sheets.properties.title"
        ).execute()
        titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
        if OVERFLOW_TAB not in titles:
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": OVERFLOW_TAB}}}]},
            ).execute()
            sheets.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=a1(OVERFLOW_TAB, "A", 1) + ":C1",
                valueInputOption="RAW",
                body={"values": [["kunci", "bagian", "teks (JANGAN diubah/dihapus sebelum Sync ke Coda)"]]},
            ).execute()
            log.info("[Overflow] tab '%s' dibuat", OVERFLOW_TAB)
        _OVERFLOW_READY.add(sheet_id)


def save_overflow(creds, sheet_id, key, rest):
    sheets = build("sheets", "v4", credentials=creds)
    ensure_overflow_tab(sheets, sheet_id)
    rows = [[key, i, rest[p:p + OVERFLOW_CHUNK]]
            for i, p in enumerate(range(0, len(rest), OVERFLOW_CHUNK))]
    sheets.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"'{OVERFLOW_TAB}'!A:C",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def fit_body(creds, sheet_id, key, text, cap):
    """Transkrip <= jatah: tampil utuh. Lebih panjang: potong di batas paragraf, sisanya
    disimpan ke tab overflow, dan sel diberi penanda [TERPOTONG ... kunci: KEY]."""
    if len(text) <= cap:
        return text
    cut = text.rfind("\n\n", 0, cap)
    if cut < cap * 0.5:
        cut = cap
    inline, rest = text[:cut], text[cut:]
    try:
        save_overflow(creds, sheet_id, key, rest)
    except Exception as e:
        log.error("[Overflow] gagal menyimpan sisa transkrip (%s): %s", key, e)
        why = str(e).replace("\n", " ").replace("]", ")")[:150]
        return (inline + f"\n\n[TERPOTONG: {len(rest)} karakter terakhir TIDAK tersimpan "
                f"(gagal menulis ke tab '{OVERFLOW_TAB}': {why})]")
    log.info("[Overflow] %s: %d karakter tampil di sel, %d karakter disimpan di tab '%s'",
             key, len(inline), len(rest), OVERFLOW_TAB)
    return inline + f'\n\n[TERPOTONG - lanjutan teks ada di tab "{OVERFLOW_TAB}", kunci: {key}]'


def put_transcript(creds, sheet_id, tab_name, row, col, link, title, job, idx, cap, text, warn=""):
    if not link:   # job lama tanpa link: perilaku lama (tulis seluruh sel)
        write_cell(creds, sheet_id, tab_name, row, col, text)
        return
    body = fit_body(creds, sheet_id, f"{job or uuid.uuid4().hex[:10]}-{idx}", text, cap)
    if warn:
        body = f"[PERINGATAN: {warn}]\n\n{body}"
    set_block(creds, sheet_id, tab_name, row, col, make_header(title, link), link, body)


def put_error(creds, sheet_id, tab_name, row, col, link, title, msg):
    if not link:
        write_cell(creds, sheet_id, tab_name, row, col, msg)
        return
    set_block(creds, sheet_id, tab_name, row, col, make_header(title, link), link, msg)


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


def collect_videos(drive, root_id, prefix=""):
    """Telusuri folder (dan subfolder) -> (daftar video, daftar item yang dilewati)."""
    videos, skipped, visited = [], [], set()
    stack = [(root_id, prefix, 0)]
    while stack:
        folder_id, prefix, depth = stack.pop()
        if folder_id in visited:
            continue
        visited.add(folder_id)
        for item in list_children(drive, folder_id):
            name, mime = item["name"], item.get("mimeType", "")
            path = prefix + name
            if mime == FOLDER_MIME:
                if depth + 1 > MAX_FOLDER_DEPTH:
                    skipped.append(f"{path}/ (subfolder terlalu dalam)")
                else:
                    stack.append((item["id"], path + "/", depth + 1))
            elif is_video(name, mime):
                videos.append({"id": item["id"], "name": name, "path": path, "size": to_int(item.get("size")),
                           "can_download": (item.get("capabilities") or {}).get("canDownload")})
            else:
                skipped.append(f"{path} ({mime})")
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
                fileId=rid, fields="id,name,mimeType,size,capabilities(canDownload)", supportsAllDrives=True
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
            found = [{"id": meta["id"], "name": name, "path": name, "size": to_int(meta.get("size")),
                      "can_download": (meta.get("capabilities") or {}).get("canDownload")}]
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
    JOBS.put((root_ids, youtube_urls, req.sheet_id, req.tab_name, req.row, output_col, max_videos))
    waiting = JOBS.qsize()
    log.info("[Antrean] baris %s masuk antrean (menunggu giliran: %d)", req.row, waiting)
    return {"status": "diterima, masuk antrean", "menunggu_di_depan": waiting}


def process_request(root_ids, youtube_urls, sheet_id, tab_name, row, output_col, max_videos):
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
            if notes:   # ada link yang gagal diakses / dibaca -> ini masalah nyata
                hint = (f" Untuk link Drive, pastikan sudah di-share ke {sa_email} (minimal Viewer)."
                        if root_ids else "")
                fail("ERROR: tidak ada video ditemukan. " + "; ".join(notes).rstrip(".") + "."
                     + summarize_skipped(skipped) + hint)
            else:       # link bisa diakses tapi memang bukan video (portfolio PDF, dsb.)
                fail("Tidak ada video: link ini tidak berisi file video (file non-video dilewati)."
                     + summarize_skipped(skipped))
            return
        if total > max_videos:
            fail(f"ERROR: ditemukan {total} video, melebihi batas {max_videos} video per baris. "
                 "Naikkan MAX_VIDEOS_PER_ROW di Apps Script (maks 20) atau pisahkan link-nya.")
            return

        # --- Judul + link + header tiap video, jatah karakter, dan id job
        for v in videos:
            if v.get("kind") == "youtube":
                v["link"] = v["url"]
            else:
                v["link"] = f"https://drive.google.com/file/d/{v['id']}/view"
            v["title"] = clean_title(v["name"] if v.get("kind") == "youtube" else v["path"])
            v["header"] = make_header(v["title"], v["link"])
        if notes:
            videos[0]["warn"] = "; ".join(notes)  # tampil sebagai baris peringatan di blok video pertama
        job = uuid.uuid4().hex[:10]
        cap = per_video_cap([v["header"] for v in videos])
        log.info("[Resolve] job=%s, jatah per video=%d karakter", job, cap)

        # --- Placeholder: semua blok langsung tampil, lalu diganti satu per satu
        set_all_blocks(creds, sheet_id, tab_name, row, output_col,
                       [(v["header"], f"Diproses: [{i + 1}/{total}] dalam antrean")
                        for i, v in enumerate(videos)])

        # --- Proses satu per satu (hemat RAM & disk di Render free)
        for i, video in enumerate(videos):
            if video.get("kind") == "youtube":
                process_youtube_video(creds, video, i + 1, total, sheet_id, tab_name, row, output_col, job, cap)
            else:
                process_one_video(creds, drive, video, i + 1, total, sheet_id, tab_name, row, output_col, job, cap)

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


def fetch_youtube_titles(video_ids):
    """Judul video untuk link langsung (bukan dari playlist). Gagal = judul kosong, bukan error."""
    titles = {}
    if not YOUTUBE_API_KEY or not video_ids:
        return titles
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        try:
            r = requests.get(
                YOUTUBE_VIDEOS_URL,
                params={"part": "snippet", "id": ",".join(chunk), "key": YOUTUBE_API_KEY},
                timeout=30,
            )
            if r.status_code == 200:
                for it in r.json().get("items", []):
                    titles[it["id"]] = (it.get("snippet") or {}).get("title", "")
            else:
                log.warning("[Resolve] YouTube videos API HTTP %s (judul dilewati)", r.status_code)
        except requests.RequestException as e:
            log.warning("[Resolve] YouTube videos API gagal: %s", str(e).replace(YOUTUBE_API_KEY, "***"))
    return titles


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
            videos.append({"kind": "youtube", "id": vid, "name": title or vid, "path": title or vid,
                           "no_title": not title, "url": f"https://www.youtube.com/watch?v={vid}"})
    # Judul untuk link video langsung (playlist sudah membawa judulnya sendiri)
    missing = [v["id"] for v in videos if v.get("no_title")]
    if missing:
        titles = fetch_youtube_titles(missing)
        for v in videos:
            if v.get("no_title") and titles.get(v["id"]):
                v["name"] = v["path"] = titles[v["id"]]
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


def format_youtube_segments(segments):
    """Segmen caption [{start, dur, text}] -> paragraf ~30 detik, diawali [MM:SS]."""
    groups = []  # [detik_mulai, [potongan teks]]
    # Pengaman satuan: waktu mulai di atas 10 jam hampir pasti berarti milidetik, bukan detik.
    starts = [_to_float(sg.get("start")) for sg in segments]
    scale = 0.001 if starts and max(starts) > 36000 else 1.0
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


def process_youtube_video(creds, video, idx, total, sheet_id, tab_name, row, col, job, cap):
    tag = f"[Video {idx}/{total}]"
    name = video["path"]
    link, title = video["link"], video["title"]

    def write_err(msg):
        log.error("%s %s", tag, msg)
        try:
            put_error(creds, sheet_id, tab_name, row, col, link, title, msg)
        except Exception as e2:
            log.error("%s Gagal menulis pesan error ke sheet: %s", tag, e2)

    try:
        log.info("%s Ambil transkrip YouTube via Apify: %s", tag, name)
        t0 = time.time()
        text = fetch_youtube_transcript(video["url"])
        put_transcript(creds, sheet_id, tab_name, row, col, link, title, job, idx, cap, text,
                       warn=video.get("warn", ""))
        log.info("%s OK - %d karakter (%.0f detik)", tag, len(text), time.time() - t0)
    except NoCaptions:
        write_err("ERROR (tidak ada caption): video ini tidak punya caption/transkrip yang bisa "
                  "diambil (dinonaktifkan pemilik, private, atau dihapus).")
    except Exception as e:
        write_err(f"ERROR (transkrip YouTube): {e}")


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


def process_one_video(creds, drive, video, idx, total, sheet_id, tab_name, row, col, job, cap):
    tag = f"[Video {idx}/{total}]"
    name = video["path"]
    link, title = video["link"], video["title"]

    def write_err(msg):
        log.error("%s %s", tag, msg)
        try:
            put_error(creds, sheet_id, tab_name, row, col, link, title, msg)
        except Exception as e2:
            log.error("%s Gagal menulis pesan error ke sheet: %s", tag, e2)

    if video.get("can_download") is False:
        sa = getattr(creds, "service_account_email", "service account")
        write_err("ERROR (tidak boleh di-download): pemilik/admin membatasi download untuk akun ini "
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
                  "link": link, "label": title, "job": job, "idx": idx, "cap": cap}
        if video.get("warn"):
            params["warn"] = video["warn"]
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
        write_err(f"ERROR ({state['stage']}): {e}")


@app.get("/manual-recover")
def manual_recover(transcript_id: str, sheet_id: str, row: int, tab_name: str = "Sheet1",
                   col: str = "B", label: str = "", token: str = "",
                   link: str = "", job: str = "", idx: int = 0, cap: int = MAX_CELL_CHARS):
    """Endpoint darurat: tarik ulang transkrip yang sudah 'completed' di AssemblyAI
    tapi webhook-nya gagal nyampe. Dipanggil manual lewat browser.
    Isi `link` (link video Drive, persis seperti di header blok) supaya hasilnya masuk ke
    BLOK video itu; `label` = judul video. Tanpa `link`, seluruh sel ditimpa (perilaku lama)."""
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
            put_error(creds, sheet_id, tab_name, row, col, link, label, f"ERROR (status AssemblyAI: {status})")
            return {"ok": False, "status": status}
        text = fetch_timestamped_transcript(transcript_id)
        put_transcript(creds, sheet_id, tab_name, row, col, link, label, job, idx, cap, text)
        return {"ok": True, "chars": len(text)}
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
    col = q.get("col", "B")       # default "B" supaya job lama (tanpa col) tetap jalan
    link = q.get("link", "")      # job lama (tanpa link) -> tulis seluruh sel seperti dulu
    label = q.get("label", "")    # judul video
    job = q.get("job", "")
    idx = int(q.get("idx") or 0)
    cap = int(q.get("cap") or MAX_CELL_CHARS)
    warn = q.get("warn", "")

    body = await request.json()
    transcript_id = body.get("transcript_id")
    status = body.get("status")
    log.info("[WEBHOOK] row=%s kolom=%s transcript_id=%s status=%s", row, col, transcript_id, status)

    creds = get_credentials()

    if status != "completed":
        detail = describe_failure(transcript_id, status)
        put_error(creds, sheet_id, tab_name, row, col, link, label, f"ERROR (AssemblyAI {status}): {detail}")
        return {"ok": True}

    try:
        text = fetch_timestamped_transcript(transcript_id)
        put_transcript(creds, sheet_id, tab_name, row, col, link, label, job, idx, cap, text, warn=warn)
        log.info("[WEBHOOK] SELESAI - hasil ditulis ke %s", a1(tab_name, col, row))
    except Exception as e:
        log.error("[WEBHOOK] Gagal ambil/tulis hasil: %s", e)
        try:
            put_error(creds, sheet_id, tab_name, row, col, link, label, f"ERROR (ambil hasil transkrip): {e}")
        except Exception as e2:
            log.error("[WEBHOOK] Gagal menulis pesan error ke sheet: %s", e2)

    return {"ok": True}
