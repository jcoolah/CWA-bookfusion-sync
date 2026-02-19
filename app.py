import os
import secrets
import sqlite3
import hashlib
import logging
import atexit
import threading
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, abort, redirect, render_template, request, send_file, url_for
from flask_wtf.csrf import CSRFProtect

APP_PORT = int(os.getenv("APP_PORT", "8090"))
LIB_DIR = os.getenv("CALIBRE_LIBRARY_DIR", "/calibre-library")
API_KEY = os.getenv("BOOKFUSION_API_KEY")
API_BASE = os.getenv("BOOKFUSION_API_BASE", "https://www.bookfusion.com/calibre-api/v1")
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "15"))
SYNC_STATE_DB_PATH = os.getenv("SYNC_STATE_DB_PATH", "/app/data/synced_books.db")
SYNC_LOG_PATH = os.getenv("SYNC_LOG_PATH", "/app/logs/bookfusion-sync.log")
DEFAULT_SYNC_MODE = os.getenv("DEFAULT_SYNC_MODE", "manual").strip().lower()
SYNC_TAG = (os.getenv("SYNC_TAG", "bf") or "bf").strip()

DB_PATH = os.path.join(LIB_DIR, "metadata.db")
VALID_MODES = {"manual", "automatic"}
if DEFAULT_SYNC_MODE not in VALID_MODES:
    DEFAULT_SYNC_MODE = "manual"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", secrets.token_hex(32))
csrf = CSRFProtect(app)
scheduler = BackgroundScheduler(timezone="UTC")
sync_lock = threading.Lock()


def setup_logging():
    os.makedirs(os.path.dirname(SYNC_LOG_PATH), exist_ok=True)

    logger = logging.getLogger("bookfusion_sync")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(
        SYNC_LOG_PATH,
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


logger = setup_logging()


# -------------------------
# Utility Functions
# -------------------------

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def state_conn():
    os.makedirs(os.path.dirname(SYNC_STATE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(SYNC_STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_state_db():
    conn = state_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS synced_books (
            book_id INTEGER PRIMARY KEY,
            file_digest TEXT NOT NULL,
            synced_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            mode TEXT NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,
            succeeded INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            skipped INTEGER NOT NULL DEFAULT 0,
            message TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def get_sync_mode():
    conn = state_conn()
    row = conn.execute(
        "SELECT value FROM sync_settings WHERE key = 'sync_mode'"
    ).fetchone()
    if row and row["value"] in VALID_MODES:
        mode = row["value"]
    else:
        mode = DEFAULT_SYNC_MODE
        conn.execute(
            """
            INSERT INTO sync_settings(key, value)
            VALUES ('sync_mode', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (mode,),
        )
        conn.commit()
    conn.close()
    return mode


def set_sync_mode(mode):
    if mode not in VALID_MODES:
        raise ValueError("Invalid sync mode")

    conn = state_conn()
    conn.execute(
        """
        INSERT INTO sync_settings(key, value)
        VALUES ('sync_mode', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (mode,),
    )
    conn.commit()
    conn.close()
    logger.info("Sync mode updated to %s", mode)


def get_last_sync_run():
    conn = state_conn()
    row = conn.execute(
        """
        SELECT started_at, completed_at, mode, processed, succeeded, failed, skipped, message
        FROM sync_runs
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def start_sync_run(mode):
    conn = state_conn()
    cur = conn.execute(
        "INSERT INTO sync_runs(started_at, mode) VALUES(?, ?)",
        (utc_now_iso(), mode),
    )
    conn.commit()
    run_id = cur.lastrowid
    conn.close()
    return run_id


def finish_sync_run(run_id, processed, succeeded, failed, skipped, message=None):
    conn = state_conn()
    conn.execute(
        """
        UPDATE sync_runs
        SET completed_at = ?, processed = ?, succeeded = ?, failed = ?, skipped = ?, message = ?
        WHERE id = ?
        """,
        (utc_now_iso(), processed, succeeded, failed, skipped, message, run_id),
    )
    conn.commit()
    conn.close()


def get_synced_digest(book_id):
    conn = state_conn()
    row = conn.execute(
        "SELECT file_digest FROM synced_books WHERE book_id = ?",
        (book_id,),
    ).fetchone()
    conn.close()
    return row["file_digest"] if row else None


def upsert_synced_book(book_id, digest):
    conn = state_conn()
    conn.execute(
        """
        INSERT INTO synced_books(book_id, file_digest, synced_at)
        VALUES(?, ?, ?)
        ON CONFLICT(book_id) DO UPDATE SET
            file_digest = excluded.file_digest,
            synced_at = excluded.synced_at
        """,
        (book_id, digest, utc_now_iso()),
    )
    conn.commit()
    conn.close()

def compute_digest(file_path):
    h = hashlib.sha256()
    size = os.path.getsize(file_path)
    h.update(size.to_bytes(8, byteorder="big"))
    h.update(b"\0")
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_tagged_books(tag_name=SYNC_TAG):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT books.id, books.title, books.path
        FROM books
        JOIN books_tags_link ON books.id = books_tags_link.book
        JOIN tags ON tags.id = books_tags_link.tag
        WHERE tags.name = ?
        """,
        (tag_name,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_primary_epub(book):
    book_dir = os.path.join(LIB_DIR, book["path"])
    if not os.path.isdir(book_dir):
        return None, None, f"Book folder missing: {book_dir}"

    epub_files = sorted(f for f in os.listdir(book_dir) if f.lower().endswith(".epub"))
    if not epub_files:
        return None, None, "No EPUB found"

    file_name = epub_files[0]
    return file_name, os.path.join(book_dir, file_name), None


def get_book_path(book_id):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT path FROM books WHERE id = ?",
        (book_id,),
    ).fetchone()
    conn.close()
    return row[0] if row else None


def get_cover_path(book_dir):
    for cover_name in ("cover.jpg", "cover.jpeg", "cover.png", "cover.webp"):
        cover_path = os.path.join(book_dir, cover_name)
        if os.path.isfile(cover_path):
            return cover_path
    return None


def remove_tag(book_id, tag_name=SYNC_TAG):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        DELETE FROM books_tags_link
        WHERE book = ?
        AND tag = (
            SELECT id FROM tags WHERE name = ?
        )
        """,
        (book_id, tag_name),
    )
    conn.commit()
    conn.close()


def get_full_metadata(book_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    title = conn.execute(
        "SELECT title FROM books WHERE id = ?",
        (book_id,),
    ).fetchone()["title"]

    authors = conn.execute(
        """
        SELECT authors.name
        FROM authors
        JOIN books_authors_link ON authors.id = books_authors_link.author
        WHERE books_authors_link.book = ?
        ORDER BY authors.name
        """,
        (book_id,),
    ).fetchall()

    tags = conn.execute(
        """
        SELECT tags.name
        FROM tags
        JOIN books_tags_link ON tags.id = books_tags_link.tag
        WHERE books_tags_link.book = ?
        """,
        (book_id,),
    ).fetchall()

    comments = conn.execute(
        "SELECT text FROM comments WHERE book = ?",
        (book_id,),
    ).fetchone()

    identifiers = conn.execute(
        "SELECT type, val FROM identifiers WHERE book = ?",
        (book_id,),
    ).fetchall()

    languages = conn.execute(
        """
        SELECT languages.lang_code
        FROM languages
        JOIN books_languages_link ON languages.id = books_languages_link.lang_code
        WHERE books_languages_link.book = ?
        """,
        (book_id,),
    ).fetchall()

    conn.close()

    return {
        "title": title,
        "authors": [a["name"] for a in authors],
        "tags": [t["name"] for t in tags if t["name"] != SYNC_TAG],
        "summary": comments["text"] if comments else None,
        "isbn": next((i["val"] for i in identifiers if i["type"] == "isbn"), None),
        "language": languages[0]["lang_code"] if languages else None,
    }


# -------------------------
# Upload Logic
# -------------------------

def upload_book(book, file_name, file_path, digest):
    headers = {
        "Authorization": f"Basic {requests.auth._basic_auth_str(API_KEY, '')[6:]}"
    }

    # 1️⃣ INIT
    r = requests.post(
        f"{API_BASE}/uploads/init",
        headers=headers,
        files=[
            ("filename", (None, file_name)),
            ("digest", (None, digest)),
        ],
    )

    if r.status_code not in (200, 201):
        logger.error("Init failed for %s (%s): %s", book["title"], book["id"], r.status_code)
        return False, f"Init failed: {r.status_code}"

    data = r.json()
    upload_url = data["url"]
    upload_params = data["params"]

    # 2️⃣ S3 UPLOAD
    with open(file_path, "rb") as f:
        multipart = [(k, (None, v)) for k, v in upload_params.items()]
        multipart.append(("file", (file_name, f)))

        r2 = requests.post(upload_url, files=multipart)

    if r2.status_code not in (200, 201, 204):
        logger.error("S3 upload failed for %s (%s): %s", book["title"], book["id"], r2.status_code)
        return False, f"S3 upload failed: {r2.status_code}"

    # 3️⃣ FINALIZE
    meta = get_full_metadata(book["id"])
    metadata_digest = digest  # can enhance later

    finalize_parts = [
        ("key", (None, upload_params["key"])),
        ("digest", (None, digest)),
        ("metadata[calibre_metadata_digest]", (None, metadata_digest)),
        ("metadata[title]", (None, meta["title"])),
    ]

    for author in meta["authors"]:
        finalize_parts.append(("metadata[author_list][]", (None, author)))

    for tag in meta["tags"]:
        finalize_parts.append(("metadata[tag_list][]", (None, tag)))

    if meta["summary"]:
        finalize_parts.append(("metadata[summary]", (None, meta["summary"])))

    if meta["isbn"]:
        finalize_parts.append(("metadata[isbn]", (None, meta["isbn"])))

    if meta["language"]:
        finalize_parts.append(("metadata[language]", (None, meta["language"])))

    r3 = requests.post(
        f"{API_BASE}/uploads/finalize",
        headers=headers,
        files=finalize_parts,
    )

    if r3.status_code not in (200, 201):
        logger.error("Finalize failed for %s (%s): %s", book["title"], book["id"], r3.status_code)
        return False, f"Finalize failed: {r3.status_code}"

    return True, "Uploaded"


def run_sync_cycle(mode):
    if mode not in VALID_MODES:
        raise ValueError("Invalid mode for sync cycle")

    if not API_KEY:
        msg = "BOOKFUSION_API_KEY is not configured"
        logger.error(msg)
        return {
            "results": [],
            "total": 0,
            "succeeded": 0,
            "failed": 1,
            "skipped": 0,
            "message": msg,
        }

    if not sync_lock.acquire(blocking=False):
        logger.info("Sync cycle skipped because another sync is in progress")
        return {
            "results": [],
            "total": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "message": "Another sync is currently running",
        }

    run_id = start_sync_run(mode)
    books = get_tagged_books()
    results = []
    succeeded = 0
    failed = 0
    skipped = 0
    logger.info("Starting %s sync cycle with %s tagged books", mode, len(books))

    try:
        for book in books:
            file_name, file_path, file_error = get_primary_epub(book)
            if file_error:
                failed += 1
                results.append({
                    "title": book["title"],
                    "success": False,
                    "message": file_error,
                })
                continue

            digest = compute_digest(file_path)
            previous_digest = get_synced_digest(book["id"])
            if mode == "automatic" and previous_digest == digest:
                skipped += 1
                results.append({
                    "title": book["title"],
                    "success": True,
                    "message": "Skipped (already synced)",
                    "skipped": True,
                })
                continue

            success, message = upload_book(book, file_name, file_path, digest)
            if success:
                remove_tag(book["id"])
                upsert_synced_book(book["id"], digest)
                succeeded += 1
            else:
                failed += 1

            results.append({
                "title": book["title"],
                "success": success,
                "message": message,
                "skipped": False,
            })

        processed = len(books)
        finish_sync_run(run_id, processed, succeeded, failed, skipped)
        logger.info(
            "Completed %s sync cycle: processed=%s succeeded=%s failed=%s skipped=%s",
            mode,
            processed,
            succeeded,
            failed,
            skipped,
        )
        return {
            "results": results,
            "total": processed,
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "message": None,
        }
    except Exception as exc:
        logger.exception("Sync cycle crashed")
        finish_sync_run(
            run_id,
            len(books),
            succeeded,
            failed + 1,
            skipped,
            message=str(exc),
        )
        return {
            "results": results,
            "total": len(books),
            "succeeded": succeeded,
            "failed": failed + 1,
            "skipped": skipped,
            "message": str(exc),
        }
    finally:
        sync_lock.release()


def scheduled_sync_job():
    mode = get_sync_mode()
    if mode != "automatic":
        logger.info("Scheduler tick skipped (mode=%s)", mode)
        return
    run_sync_cycle("automatic")


# -------------------------
# Web Routes
# -------------------------

@app.get("/")
def index():
    books = get_tagged_books()
    mode = get_sync_mode()
    return render_template(
        "index.html",
        books=books,
        count=len(books),
        mode=mode,
        sync_tag=SYNC_TAG,
        interval=SYNC_INTERVAL_MINUTES,
        last_run=get_last_sync_run(),
    )

@app.post("/sync")
def sync():
    summary = run_sync_cycle("manual")

    return render_template(
        "results.html",
        results=summary["results"],
        total=summary["total"],
        succeeded=summary["succeeded"],
        failed=summary["failed"],
        skipped=summary["skipped"],
        mode="manual",
        message=summary["message"],
    )


@app.post("/mode")
def set_mode():
    requested_mode = (request.form.get("mode") or "").strip().lower()
    if requested_mode not in VALID_MODES:
        abort(400)
    set_sync_mode(requested_mode)
    return redirect(url_for("index"))


@app.get("/covers/<int:book_id>")
def book_cover(book_id):
    relative_path = get_book_path(book_id)
    if not relative_path:
        abort(404)

    lib_root = os.path.realpath(LIB_DIR)
    book_dir = os.path.realpath(os.path.join(LIB_DIR, relative_path))
    if not book_dir.startswith(f"{lib_root}{os.sep}"):
        abort(404)

    cover_path = get_cover_path(book_dir)
    if not cover_path:
        abort(404)

    return send_file(cover_path, max_age=300)


if __name__ == "__main__":
    init_state_db()
    get_sync_mode()
    scheduler.add_job(
        scheduled_sync_job,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="bookfusion-listening-mode",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    logger.info(
        "bookfusion-sync started on port=%s mode=%s interval=%sm sync_tag=%s",
        APP_PORT,
        get_sync_mode(),
        SYNC_INTERVAL_MINUTES,
        SYNC_TAG,
    )
    app.run(host="0.0.0.0", port=APP_PORT)
