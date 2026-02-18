import os
import secrets
import sqlite3
import hashlib
import requests
from flask import Flask, abort, render_template, request, send_file
from flask_wtf.csrf import CSRFProtect

APP_PORT = int(os.getenv("APP_PORT", "8090"))
LIB_DIR = os.getenv("CALIBRE_LIBRARY_DIR", "/calibre-library")
API_KEY = os.getenv("BOOKFUSION_API_KEY")
API_BASE = os.getenv("BOOKFUSION_API_BASE", "https://www.bookfusion.com/calibre-api/v1")

DB_PATH = os.path.join(LIB_DIR, "metadata.db")

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", secrets.token_hex(32))
csrf = CSRFProtect(app)


# -------------------------
# Utility Functions
# -------------------------

def compute_digest(file_path):
    h = hashlib.sha256()
    size = os.path.getsize(file_path)
    h.update(size.to_bytes(8, byteorder="big"))
    h.update(b"\0")
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_tagged_books(tag_name="bf"):
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


def remove_tag(book_id, tag_name="bf"):
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
        "tags": [t["name"] for t in tags if t["name"] != "bf"],
        "summary": comments["text"] if comments else None,
        "isbn": next((i["val"] for i in identifiers if i["type"] == "isbn"), None),
        "language": languages[0]["lang_code"] if languages else None,
    }


# -------------------------
# Upload Logic
# -------------------------

def upload_book(book):
    headers = {
        "Authorization": f"Basic {requests.auth._basic_auth_str(API_KEY, '')[6:]}"
    }

    book_dir = os.path.join(LIB_DIR, book["path"])
    epub_files = [f for f in os.listdir(book_dir) if f.endswith(".epub")]

    if not epub_files:
        return False, "No EPUB found"

    file_name = epub_files[0]
    file_path = os.path.join(book_dir, file_name)

    digest = compute_digest(file_path)

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
        return False, f"Finalize failed: {r3.status_code}"

    return True, "Uploaded"


# -------------------------
# Web Routes
# -------------------------

@app.get("/")
def index():
    books = get_tagged_books()
    return render_template("index.html", books=books, count=len(books))

@app.post("/sync")
def sync():
    books = get_tagged_books()
    results = []

    for book in books:
        success, message = upload_book(book)
        if success:
            remove_tag(book["id"])

        results.append({
            "title": book["title"],
            "success": success,
            "message": message
        })

    return render_template(
        "results.html",
        results=results,
        total=len(results)
    )


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
    app.run(host="0.0.0.0", port=APP_PORT)
