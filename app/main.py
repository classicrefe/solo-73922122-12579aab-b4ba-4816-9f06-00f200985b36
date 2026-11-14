"""Corpus contamination checker.

Finds near-duplicate articles across a train/test corpus before model
evaluation. Two (or more) API processes share one SQLite database; every
committed batch modification bumps the corpus generation and enqueues a
background verification job. Verification results (direct similarity edges
and connected groups) are persisted per generation and published
atomically; queries never compare texts on the fly.

Freshness rule: /contamination answers 200 only when the published results
cover the whole currently committed corpus (published_generation ==
current_generation); otherwise it answers 202 with the pending state, so
stale groups are never paired with new texts and a "clean" verdict is
never reported early.
"""

import logging
import os
import socket
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse

from .db import MAX_ARTICLES, connect, get_meta, init_db
from .schemas import BatchRequest, BatchResponse
from .sync import (
    MAX_BATCHES_PER_PAGE,
    InvalidCursor,
    SnapshotRequired,
    build_changes,
    build_snapshot,
    decode_cursor,
    read_sync_state,
)
from .worker import CompareWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("corpus.api")

DB_PATH = os.environ.get("DB_PATH", "/data/corpus.db")
INSTANCE_ID = os.environ.get("INSTANCE_ID") or socket.gethostname()
WORKER_INTERVAL_MS = int(os.environ.get("WORKER_INTERVAL_MS", "1000"))
COMPARE_DELAY_MS = int(os.environ.get("COMPARE_DELAY_MS", "0"))
# Chaos/restart-demo knob: when set, the process hard-exits (os._exit, like
# a crash) as soon as this file appears. Inert when unset.
EXIT_WHEN_FILE_EXISTS = os.environ.get("EXIT_WHEN_FILE_EXISTS", "")


def _watch_exit_file(path: str) -> None:
    while True:
        if os.path.exists(path):
            os._exit(1)
        time.sleep(0.2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(DB_PATH)
    worker = CompareWorker(
        DB_PATH,
        INSTANCE_ID,
        interval_ms=WORKER_INTERVAL_MS,
        compare_delay_ms=COMPARE_DELAY_MS,
    )
    # Restart recovery: if the corpus has unpublished changes (e.g. the
    # previous process died mid-verification), re-enqueue the job now.
    worker.ensure_pending_job()
    worker.start()
    app.state.worker = worker
    if EXIT_WHEN_FILE_EXISTS:
        log.warning("exit-file hook armed: %s", EXIT_WHEN_FILE_EXISTS)
        threading.Thread(
            target=_watch_exit_file, args=(EXIT_WHEN_FILE_EXISTS,), daemon=True
        ).start()
    log.info("instance %s ready, db=%s", INSTANCE_ID, DB_PATH)
    yield
    worker.stop()
    worker.join(timeout=5)


app = FastAPI(
    title="Corpus Contamination Checker",
    version="1.1.0",
    description=(
        "Detects near-duplicate articles (5-char shingle Jaccard >= 0.8) "
        "across train/test corpora. Results are computed in the background, "
        "persisted per corpus generation and served only when they cover "
        "the whole committed corpus.\n\n"
        "Offline corpora are kept in sync through `GET /sync/snapshot` "
        "(all articles + continuation cursor in one committed view) and "
        "`GET /sync/changes` (committed modification batches in commit "
        "order, at most 10 complete batches per page)."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.post("/articles/batch", response_model=BatchResponse)
def apply_batch(req: BatchRequest):
    """Apply one atomic batch of additions/corrections/deletions.

    Every article is validated first; the batch is applied in a single
    transaction together with the generation bump and the verification job.
    Any failure (invalid article, 300-article limit, id conflicts) rolls
    the whole batch back — nothing takes effect partially.
    """
    upsert_ids = [u.id for u in req.upserts]
    if len(upsert_ids) != len(set(upsert_ids)):
        raise HTTPException(422, "duplicate article ids in upserts")
    if len(req.deletes) != len(set(req.deletes)):
        raise HTTPException(422, "duplicate article ids in deletes")
    conflict = set(upsert_ids) & set(req.deletes)
    if conflict:
        raise HTTPException(
            422, f"article ids both upserted and deleted: {sorted(conflict)}"
        )

    conn = connect(DB_PATH)
    try:
        if not req.upserts and not req.deletes:
            current, published = get_meta(conn)
            _, last_seq = read_sync_state(conn)
            count = conn.execute("SELECT COUNT(*) AS c FROM articles").fetchone()["c"]
            return BatchResponse(
                changed=False,
                generation=current,
                published_generation=published,
                change_seq=last_seq,
                upserted=0,
                deleted=0,
                articles=count,
            )

        conn.execute("BEGIN IMMEDIATE")
        try:
            current, _ = get_meta(conn)
            new_generation = current + 1

            # The change record is written in this same transaction: a
            # committed batch and its change-feed entry either both exist
            # or neither does. seq is assigned here (under the write lock),
            # so it reflects commit order across concurrent instances.
            committed_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
            conn.execute(
                "INSERT INTO change_batches (generation, committed_at) VALUES (?, ?)",
                (new_generation, committed_at),
            )
            seq = conn.execute("SELECT last_insert_rowid() AS s").fetchone()["s"]

            deleted = 0
            deleted_ids = []
            for article_id in req.deletes:
                cur = conn.execute("DELETE FROM articles WHERE id = ?", (article_id,))
                if cur.rowcount:
                    deleted += cur.rowcount
                    deleted_ids.append(article_id)
            for u in req.upserts:
                conn.execute(
                    """
                    INSERT INTO articles (id, side, text, updated_generation)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        side = excluded.side,
                        text = excluded.text,
                        updated_generation = excluded.updated_generation
                    """,
                    (u.id, u.side, u.text, new_generation),
                )

            count = conn.execute("SELECT COUNT(*) AS c FROM articles").fetchone()["c"]
            if count > MAX_ARTICLES:
                raise HTTPException(
                    409,
                    f"corpus limit exceeded: batch would leave {count} articles "
                    f"(max {MAX_ARTICLES}); whole batch rolled back",
                )

            conn.executemany(
                "INSERT INTO change_upserts (seq, id, side, text) VALUES (?, ?, ?, ?)",
                [(seq, u.id, u.side, u.text) for u in req.upserts],
            )
            conn.executemany(
                "INSERT INTO change_deletes (seq, id) VALUES (?, ?)",
                [(seq, article_id) for article_id in deleted_ids],
            )
            conn.execute(
                "UPDATE sync_state SET last_seq = ? WHERE id = 1", (seq,)
            )
            conn.execute(
                "UPDATE meta SET current_generation = ? WHERE id = 1", (new_generation,)
            )
            conn.execute(
                "INSERT OR IGNORE INTO compute_jobs (generation, status)"
                " VALUES (?, 'pending')",
                (new_generation,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        _, published = get_meta(conn)
        log.info(
            "batch committed: generation=%d seq=%d upserts=%d deleted=%d articles=%d",
            new_generation,
            seq,
            len(req.upserts),
            deleted,
            count,
        )
        return BatchResponse(
            changed=True,
            generation=new_generation,
            published_generation=published,
            change_seq=seq,
            upserted=len(req.upserts),
            deleted=deleted,
            articles=count,
        )
    except HTTPException:
        raise
    except sqlite3.IntegrityError as exc:
        raise HTTPException(422, f"batch rejected, nothing applied: {exc}")
    finally:
        conn.close()


@app.get("/sync/snapshot")
def sync_snapshot():
    """Full committed corpus plus the cursor that continues from it.

    Articles, change sequence number and cursor are read in one SQLite read
    transaction, so the cursor can only ever denote the same committed view
    that produced the articles. The cursor is therefore never paired with a
    different generation's content.
    """
    conn = connect(DB_PATH)
    try:
        conn.execute("BEGIN")
        payload = build_snapshot(conn)
        conn.commit()
    finally:
        conn.close()
    return payload


@app.get(
    "/sync/changes",
    responses={
        400: {"description": "Malformed cursor (not issued by this service)"},
        409: {
            "description": (
                "Cursor cannot be continued (different database identity, "
                "change log behind the cursor, or truncated history); "
                "fetch GET /sync/snapshot"
            )
        },
    },
)
def sync_changes(
    cursor: str = Query(..., description="Continuation cursor from a snapshot or a previous page"),
    limit: int = Query(
        MAX_BATCHES_PER_PAGE,
        ge=1,
        le=MAX_BATCHES_PER_PAGE,
        description="Maximum complete batches per page (hard cap: 10)",
    ),
):
    """Committed modification batches after ``cursor``, in commit order.

    Each page holds at most 10 complete batches (never a partial batch); both
    upserted articles (with their new side and full new text) and deleted
    article IDs are included. The response cursor advances only over batches
    actually returned, so confirmed batches are never replayed and later
    batches are never skipped or reordered.

    A cursor that cannot be continued (another database identity, change log
    behind it, or truncated history) yields ``409 snapshot_required``; a
    malformed token yields ``400``.
    """
    try:
        parsed = decode_cursor(cursor)
    except InvalidCursor as exc:
        raise HTTPException(400, f"invalid cursor: {exc}")

    conn = connect(DB_PATH)
    try:
        conn.execute("BEGIN")
        try:
            payload = build_changes(conn, parsed, limit)
        except SnapshotRequired as exc:
            conn.rollback()
            return JSONResponse(
                status_code=409,
                content={
                    "status": "snapshot_required",
                    "detail": str(exc),
                    "action": "GET /sync/snapshot",
                },
            )
        conn.commit()
    finally:
        conn.close()
    return payload


@app.get("/articles")
def list_articles():
    """List the currently committed corpus."""
    conn = connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT id, side, text, updated_generation FROM articles ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return {
        "count": len(rows),
        "articles": [
            {
                "id": r["id"],
                "side": r["side"],
                "text": r["text"],
                "text_length": len(r["text"]),
                "updated_generation": r["updated_generation"],
            }
            for r in rows
        ],
    }


@app.get("/contamination")
def contamination():
    """Cross-set contamination report for the current corpus.

    200: results cover every committed article — returns contamination
    groups (connected components containing both train and test articles),
    their members and the direct similarity edges inside each group.
    202: verification is still pending for the current corpus state.
    """
    conn = connect(DB_PATH)
    try:
        # One read transaction => consistent view of pointer + results.
        conn.execute("BEGIN")
        current, published = get_meta(conn)
        if published != current:
            conn.rollback()
            return JSONResponse(
                status_code=202,
                content={
                    "status": "pending",
                    "detail": "verification does not cover the current corpus yet",
                    "current_generation": current,
                    "published_generation": published,
                },
                headers={"Retry-After": "1"},
            )
        group_rows = conn.execute(
            """
            SELECT g.group_id, g.article_id, g.side, a.text
            FROM article_groups g JOIN articles a ON a.id = g.article_id
            WHERE g.generation = ?
            ORDER BY g.group_id, g.article_id
            """,
            (published,),
        ).fetchall()
        edge_rows = conn.execute(
            "SELECT a, b, similarity FROM edges WHERE generation = ? ORDER BY a, b",
            (published,),
        ).fetchall()
        conn.commit()
    finally:
        conn.close()

    edges = [
        {"a": e["a"], "b": e["b"], "similarity": e["similarity"]} for e in edge_rows
    ]
    by_group = {}
    for r in group_rows:
        by_group.setdefault(r["group_id"], []).append(r)

    contamination_groups = []
    same_set_groups = 0
    for group_id, members in sorted(by_group.items()):
        sides = {m["side"] for m in members}
        if sides != {"train", "test"}:
            same_set_groups += 1
            continue
        member_ids = {m["article_id"] for m in members}
        contamination_groups.append(
            {
                "group_id": group_id,
                "size": len(members),
                "train_members": sorted(
                    m["article_id"] for m in members if m["side"] == "train"
                ),
                "test_members": sorted(
                    m["article_id"] for m in members if m["side"] == "test"
                ),
                "members": [
                    {"id": m["article_id"], "side": m["side"], "text": m["text"]}
                    for m in members
                ],
                "edges": [
                    e
                    for e in edges
                    if e["a"] in member_ids and e["b"] in member_ids
                ],
            }
        )

    return {
        "status": "ok",
        "generation": published,
        "contaminated": bool(contamination_groups),
        "contamination_group_count": len(contamination_groups),
        "same_set_group_count": same_set_groups,
        "groups": contamination_groups,
    }


@app.get("/status")
def status():
    """Instance, generation and background-job state."""
    conn = connect(DB_PATH)
    try:
        current, published = get_meta(conn)
        row = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(side = 'train'), 0) AS train,
                   COALESCE(SUM(side = 'test'), 0) AS test
            FROM articles
            """
        ).fetchone()
        jobs = conn.execute(
            "SELECT generation, status, claimed_by, claimed_at FROM compute_jobs"
            " ORDER BY generation"
        ).fetchall()
    finally:
        conn.close()
    return {
        "instance": INSTANCE_ID,
        "current_generation": current,
        "published_generation": published,
        "up_to_date": current == published,
        "articles": {"total": row["total"], "train": row["train"], "test": row["test"]},
        "jobs": [
            {
                "generation": j["generation"],
                "status": j["status"],
                "claimed_by": j["claimed_by"],
                "claimed_at": j["claimed_at"],
            }
            for j in jobs
        ],
        "config": {
            "max_articles": MAX_ARTICLES,
            "compare_delay_ms": COMPARE_DELAY_MS,
            "worker_interval_ms": WORKER_INTERVAL_MS,
        },
    }
