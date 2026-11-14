"""SQLite storage layer.

One database file is shared by every API process (WAL mode, busy timeout).
All writes happen in short IMMEDIATE transactions, so background comparison
never blocks corpus submissions and vice versa.

Tables
------
meta                 single row: current_generation / published_generation
articles             committed corpus (id, side, text, updated_generation)
edges / article_groups  published verification results, keyed by generation
compute_jobs         pending/running background verification jobs (lease based)
sync_state           single row: database epoch + last committed change seq
change_batches       one row per committed modification batch (commit order)
change_upserts       new/changed articles inside a batch (full new text)
change_deletes       deleted article ids inside a batch

Change-log rows are written in the same transaction as the article mutations,
so a committed batch and its change record appear or disappear together.
"""

import os
import sqlite3
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_generation INTEGER NOT NULL,
    published_generation INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS articles (
    id TEXT PRIMARY KEY,
    side TEXT NOT NULL CHECK (side IN ('train', 'test')),
    text TEXT NOT NULL CHECK (length(text) BETWEEN 5 AND 2000),
    updated_generation INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS edges (
    generation INTEGER NOT NULL,
    a TEXT NOT NULL,
    b TEXT NOT NULL,
    similarity REAL NOT NULL,
    PRIMARY KEY (generation, a, b)
);

CREATE TABLE IF NOT EXISTS article_groups (
    generation INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    article_id TEXT NOT NULL,
    side TEXT NOT NULL,
    PRIMARY KEY (generation, group_id, article_id)
);

CREATE TABLE IF NOT EXISTS compute_jobs (
    generation INTEGER PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running')),
    claimed_by TEXT,
    claimed_at REAL
);

CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    epoch TEXT NOT NULL,
    last_seq INTEGER NOT NULL,
    min_seq INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS change_batches (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    generation INTEGER NOT NULL,
    committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS change_upserts (
    seq INTEGER NOT NULL REFERENCES change_batches(seq) ON DELETE CASCADE,
    id TEXT NOT NULL,
    side TEXT NOT NULL,
    text TEXT NOT NULL,
    PRIMARY KEY (seq, id)
);

CREATE TABLE IF NOT EXISTS change_deletes (
    seq INTEGER NOT NULL REFERENCES change_batches(seq) ON DELETE CASCADE,
    id TEXT NOT NULL,
    PRIMARY KEY (seq, id)
);

CREATE INDEX IF NOT EXISTS idx_change_upserts_seq ON change_upserts (seq);
CREATE INDEX IF NOT EXISTS idx_change_deletes_seq ON change_deletes (seq);
"""

CREATE_SYNC_STATE = """
INSERT OR IGNORE INTO sync_state (id, epoch, last_seq, min_seq)
VALUES (1, ?,
        COALESCE((SELECT MAX(seq) FROM change_batches), 0),
        COALESCE((SELECT MIN(seq) - 1 FROM change_batches), 0))
"""

# Hard corpus limits (also enforced at the API boundary).
MAX_ARTICLES = 300
MIN_TEXT_LEN = 5
MAX_TEXT_LEN = 2000


def connect(db_path: str) -> sqlite3.Connection:
    """Open a connection in autocommit mode; transactions are explicit."""
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(db_path: str) -> None:
    directory = os.path.dirname(db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        # Defensive: an older copy of this schema may already have created
        # sync_state without the retention floor column.
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(sync_state)").fetchall()
        }
        if "min_seq" not in columns:
            conn.execute(
                "ALTER TABLE sync_state ADD COLUMN min_seq INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "INSERT OR IGNORE INTO meta (id, current_generation, published_generation)"
            " VALUES (1, 0, 0)"
        )
        # The epoch identifies this database file. If a legacy database is
        # opened (articles already present, no change log yet), it gets a new
        # epoch and a sequence baseline of 0; existing articles stay in place
        # and the first snapshot serves them with the new cursor.
        conn.execute(CREATE_SYNC_STATE, (uuid.uuid4().hex,))
        conn.commit()
    finally:
        conn.close()


def get_meta(conn: sqlite3.Connection):
    """Return ``(current_generation, published_generation)``."""
    row = conn.execute(
        "SELECT current_generation, published_generation FROM meta WHERE id = 1"
    ).fetchone()
    return row["current_generation"], row["published_generation"]
