"""Cursor-based change feed over the committed corpus.

The sync protocol
-----------------
1. ``GET /sync/snapshot`` returns *all* currently committed articles together
   with a continuation cursor.  Articles and cursor are read in one SQLite
   read transaction, so the cursor is guaranteed to belong to the exact
   committed view the articles came from.
2. ``GET /sync/changes?cursor=...`` returns committed modification batches
   strictly after the cursor, in commit order, at most
   :data:`MAX_BATCHES_PER_PAGE` complete batches per page.

A ``/articles/batch`` write assigns the next change sequence number and
inserts the batch's change record inside the same ``BEGIN IMMEDIATE``
transaction that applies the article updates.  Writes are therefore totally
ordered by ``seq`` (SQLite serialises writers), and a committed batch either
exists together with its change record or not at all.

Cursor validity
---------------
The opaque cursor carries the database *epoch* (a random id created once per
database file) and the last sequence number the consumer has confirmed.  A
cursor is refused with ``409 snapshot_required`` when it

* belongs to another database identity (database replaced/reset),
* points past this database's change log (database restored/rolled back),
* points before the oldest retained change (change history was truncated).

In both cases the consumer must take a fresh snapshot instead of silently
missing changes.  A syntactically broken or tampered cursor is rejected
with ``400``.
"""

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass

from .db import get_meta

MAX_BATCHES_PER_PAGE = 10
CURSOR_VERSION = 1
_CHECKSUM_PREFIX = b"corpus-sync-cursor/v1\x00"


class InvalidCursor(Exception):
    """The token is not a cursor produced by this service."""


class SnapshotRequired(Exception):
    """The cursor is well formed but cannot be continued safely."""


@dataclass(frozen=True)
class Cursor:
    epoch: str
    seq: int


def _checksum(raw: bytes) -> str:
    return hashlib.sha256(_CHECKSUM_PREFIX + raw).hexdigest()[:16]


def encode_cursor(epoch: str, seq: int) -> str:
    """Encode ``(epoch, seq)`` into an opaque URL-safe token."""
    raw = json.dumps(
        {"v": CURSOR_VERSION, "e": epoch, "s": int(seq)},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    return f"{token}.{_checksum(raw)}"


def decode_cursor(value: str) -> Cursor:
    """Decode a cursor token, raising :class:`InvalidCursor` on bad input."""
    try:
        token, checksum = value.split(".", 1)
        padded = token + "=" * (-len(token) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        if not hmac.compare_digest(checksum, _checksum(raw)):
            raise ValueError("checksum mismatch")
        payload = json.loads(raw)
        if not isinstance(payload, dict) or payload.get("v") != CURSOR_VERSION:
            raise ValueError("unsupported cursor version")
        epoch = payload["e"]
        seq = payload["s"]
        if not isinstance(epoch, str) or not epoch:
            raise ValueError("bad epoch")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise ValueError("bad sequence number")
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidCursor("cursor is malformed or was not issued by this service") from exc
    return Cursor(epoch=epoch, seq=seq)


def read_sync_state(conn):
    """Return ``(epoch, last_seq)`` from within the caller's transaction."""
    row = conn.execute("SELECT epoch, last_seq FROM sync_state WHERE id = 1").fetchone()
    if row is None:  # pragma: no cover - init_db always creates the row
        raise RuntimeError("sync_state row missing; database not initialised")
    return row["epoch"], row["last_seq"]


def oldest_servable_seq(conn) -> int:
    """The oldest cursor sequence that can still be continued.

    Consumers holding a cursor at or after this point are guaranteed that
    every later batch is still present. With an untruncated log this is 0.
    """
    row = conn.execute("SELECT min_seq FROM sync_state WHERE id = 1").fetchone()
    return 0 if row is None else int(row["min_seq"])


def validate_cursor(conn, cursor: Cursor) -> tuple[str, int]:
    """Check that *cursor* can be continued; return current state."""
    epoch, last_seq = read_sync_state(conn)
    if cursor.epoch != epoch:
        raise SnapshotRequired(
            "cursor belongs to a different database identity; "
            "fetch a fresh snapshot"
        )
    if cursor.seq > last_seq:
        raise SnapshotRequired(
            "cursor is ahead of this database's committed change log "
            "(database was restored or replaced); fetch a fresh snapshot"
        )
    if cursor.seq < oldest_servable_seq(conn):
        raise SnapshotRequired(
            "changes after this cursor are no longer retained; "
            "fetch a fresh snapshot"
        )
    return epoch, last_seq


def build_snapshot(conn) -> dict:
    """Build a snapshot from the caller's single read transaction."""
    epoch, last_seq = read_sync_state(conn)
    generation, published = get_meta(conn)
    rows = conn.execute(
        "SELECT id, side, text, updated_generation FROM articles ORDER BY id"
    ).fetchall()
    cursor = encode_cursor(epoch, last_seq)
    return {
        "cursor": cursor,
        "next_cursor": cursor,
        "change_seq": last_seq,
        "generation": generation,
        "published_generation": published,
        "article_count": len(rows),
        "articles": [
            {
                "id": r["id"],
                "side": r["side"],
                "text": r["text"],
                "updated_generation": r["updated_generation"],
            }
            for r in rows
        ],
    }


def build_changes(conn, cursor: Cursor, max_batches: int) -> dict:
    """Build one changes page from the caller's single read transaction."""
    _, last_seq = validate_cursor(conn, cursor)
    generation, published = get_meta(conn)

    limit = max(1, min(int(max_batches), MAX_BATCHES_PER_PAGE))
    batch_rows = conn.execute(
        """
        SELECT seq, generation, committed_at
        FROM change_batches
        WHERE seq > ?
        ORDER BY seq
        LIMIT ?
        """,
        (cursor.seq, limit),
    ).fetchall()

    if batch_rows:
        first_seq = batch_rows[0]["seq"]
        last_batch_seq = batch_rows[-1]["seq"]
        upsert_rows = conn.execute(
            """
            SELECT seq, id, side, text FROM change_upserts
            WHERE seq BETWEEN ? AND ?
            ORDER BY seq, id
            """,
            (first_seq, last_batch_seq),
        ).fetchall()
        delete_rows = conn.execute(
            """
            SELECT seq, id FROM change_deletes
            WHERE seq BETWEEN ? AND ?
            ORDER BY seq, id
            """,
            (first_seq, last_batch_seq),
        ).fetchall()
        next_seq = last_batch_seq
    else:
        upsert_rows = []
        delete_rows = []
        next_seq = cursor.seq

    upserts_by_seq: dict[int, list] = {}
    for row in upsert_rows:
        upserts_by_seq.setdefault(row["seq"], []).append(
            {"id": row["id"], "side": row["side"], "text": row["text"]}
        )
    deletes_by_seq: dict[int, list] = {}
    for row in delete_rows:
        deletes_by_seq.setdefault(row["seq"], []).append(row["id"])

    batches = [
        {
            "seq": row["seq"],
            "generation": row["generation"],
            "committed_at": row["committed_at"],
            "upserts": upserts_by_seq.get(row["seq"], []),
            "deletes": deletes_by_seq.get(row["seq"], []),
        }
        for row in batch_rows
    ]

    next_cursor = encode_cursor(cursor.epoch, next_seq)
    return {
        "cursor": next_cursor,
        "next_cursor": next_cursor,
        "from_change_seq": cursor.seq,
        "change_seq": next_seq,
        "latest_change_seq": last_seq,
        "has_more": last_seq > next_seq,
        "batch_count": len(batches),
        "generation": generation,
        "published_generation": published,
        "batches": batches,
    }
