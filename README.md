# bookfusion-sync

Sync tagged ebooks from Calibre-Web-Automated to BookFusion with a simple web UI, including an optional background "listening mode".

## Screenshot

![bookfusion-sync screenshot](docs/screenshot.png)

## What it does

- Reads books tagged `SYNC_TAG` (default: `bf`) from your Calibre library database (`metadata.db`)
- Uploads ebook files to BookFusion via API
- Removes the sync tag after successful upload
- Provides UI mode toggle: `manual` or `automatic`
- In automatic mode, runs a scheduler every 15 minutes and syncs unsynced tagged books
- Tracks synced file digests in a local SQLite state DB (`synced_books.db`)
- Writes sync activity logs to a file

## Requirements

- Docker / Docker Compose
- A Calibre library folder mounted into the container
- BookFusion API key

## Environment variables

- `APP_PORT` (default: `8090`)
- `CALIBRE_LIBRARY_DIR` (default: `/calibre-library`)
- `BOOKFUSION_API_KEY` (required)
- `BOOKFUSION_API_BASE` (default: `https://www.bookfusion.com/calibre-api/v1`)
- `SECRET_KEY` (recommended; use a stable random value for CSRF/session consistency)
- `SYNC_INTERVAL_MINUTES` (default: `15`)
- `SYNC_STATE_DB_PATH` (default: `/app/data/synced_books.db`)
- `SYNC_LOG_PATH` (default: `/app/logs/bookfusion-sync.log`)
- `DEFAULT_SYNC_MODE` (default: `manual`, valid: `manual`, `automatic`)
- `SYNC_TAG` (default: `bf`)

## Docker Compose example

```yaml
services:
  bookfusion-sync:
    build: ./bookfusion-sync
    container_name: bookfusion-sync
    environment:
      - APP_PORT=8090
      - CALIBRE_LIBRARY_DIR=/calibre-library
      - BOOKFUSION_API_KEY=${BOOKFUSION_API_KEY}
      - BOOKFUSION_API_BASE=https://www.bookfusion.com/calibre-api/v1
      - SECRET_KEY=${BOOKFUSION_SECRET_KEY}
      - SYNC_INTERVAL_MINUTES=15
      - SYNC_STATE_DB_PATH=/app/data/synced_books.db
      - SYNC_LOG_PATH=/app/logs/bookfusion-sync.log
      - DEFAULT_SYNC_MODE=manual
      - SYNC_TAG=bf
    volumes:
      - /path/to/your/calibre-library:/calibre-library
      - /path/to/bookfusion-sync-data:/app/data
      - /path/to/bookfusion-sync-logs:/app/logs
    ports:
      - "8090:8090"
    restart: unless-stopped
```

## Run

```bash
docker compose up -d --build bookfusion-sync
```

Then open:

- `http://<your-host>:8090`

## How to use

1. In Calibre/Calibre-Web, add your sync tag (default `bf`) to books you want synced.
2. Open the app UI.
3. In the app, choose mode:
   - `Manual`: sync only when you click **Sync ...**
   - `Automatic`: scheduler checks every 15 minutes and syncs automatically
4. (Optional) click **Sync Now (Manual Trigger)** in automatic mode for immediate run.
5. Check results/logs and confirm books appear in BookFusion.

## Security notes

- Do not commit real API keys or secret keys.
- Keep `SECRET_KEY` stable across restarts to avoid CSRF/session issues.
