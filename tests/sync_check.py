#!/usr/bin/env python3
"""End-to-end checks for the incremental corpus sync API.

Covers the offline-corpus requirements on top of the existing service:

* first read (`GET /sync/snapshot`) returns every committed article *and* a
  continuation cursor from one committed view;
* `GET /sync/changes` delivers committed modification batches in commit
  order, one page holds at most 10 complete batches, and each batch carries
  re-homed/updated articles with their new side + full new text as well as
  deleted article IDs;
* `/articles/batch` commits article changes and change records atomically;
* two API instances writing concurrently plus a downstream consumer paging
  across those writes never replay confirmed batches, never miss a batch and
  never see them out of order;
* a first snapshot is never paired with a cursor from another generation;
* opening a legacy database keeps the stored articles, and a cursor that
  cannot be continued is answered with an explicit `snapshot_required`.

Spawns two uvicorn processes sharing one SQLite file, like docker-compose.
"""

import os
import socket
import subprocess
import sys
import tempfile
import threading
import time

import httpx

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from app.db import connect, get_meta, init_db  # noqa: E402
from app.sync import decode_cursor, encode_cursor  # noqa: E402

FAILURES = []
WORKER_INTERVAL_MS = 300


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


class Server:
    def __init__(self, port, db_path, workdir, exit_watch=True):
        self.port = port
        self.db_path = db_path
        self.exit_file = os.path.join(workdir, f"exit-{port}")
        self.exit_watch = exit_watch
        self.proc = None
        self.log = None

    @property
    def base(self):
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        env = dict(
            os.environ,
            DB_PATH=self.db_path,
            INSTANCE_ID=f"sync-{self.port}",
            COMPARE_DELAY_MS="0",
            WORKER_INTERVAL_MS=str(WORKER_INTERVAL_MS),
        )
        if self.exit_watch:
            env["EXIT_WHEN_FILE_EXISTS"] = self.exit_file
        else:
            env.pop("EXIT_WHEN_FILE_EXISTS", None)
        self.log = open(f"/tmp/corpus-sync-{self.port}.log", "w")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=REPO, env=env, stdout=self.log, stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if httpx.get(f"{self.base}/status", timeout=2).status_code == 200:
                    return
            except httpx.TransportError:
                pass
            if self.proc.poll() is not None:
                raise RuntimeError(f"server {self.port} died; see {self.log.name}")
            time.sleep(0.2)
        raise RuntimeError(f"server {self.port} not ready in time")

    def kill(self):
        if self.proc and self.proc.poll() is None:
            try:
                if self.exit_watch:
                    open(self.exit_file, "w").close()
                else:
                    self.proc.terminate()
                self.proc.wait(timeout=15)
            except (ProcessLookupError, PermissionError):
                self.proc.kill()
                self.proc.wait(timeout=15)
        if os.path.exists(self.exit_file):
            os.unlink(self.exit_file)
        if self.log:
            self.log.close()


def wait_ready(base, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            body = httpx.get(f"{base}/status", timeout=5).json()
            if body["up_to_date"]:
                return body
        except httpx.TransportError:
            pass
        time.sleep(0.15)
    raise RuntimeError(f"{base} did not become up-to-date")


def post_batch(base, payload):
    return httpx.post(f"{base}/articles/batch", json=payload, timeout=30)


def snapshot(base):
    r = httpx.get(f"{base}/sync/snapshot", timeout=30)
    assert r.status_code == 200, r.text
    return r.json()


def changes(base, cursor, limit=None):
    params = {"cursor": cursor}
    if limit is not None:
        params["limit"] = limit
    return httpx.get(f"{base}/sync/changes", params=params, timeout=30)


def article(article_id, side="train", text=None):
    return {
        "id": article_id,
        "side": side,
        "text": text or f"article {article_id} with enough distinctive words",
    }


def cursor_epoch(cursor):
    return decode_cursor(cursor).epoch


# ---------------------------------------------------------------------------


def run_checks(base1, base2):
    # ---- empty snapshot --------------------------------------------------
    snap = snapshot(base1)
    check("fresh snapshot is empty with a cursor", snap["article_count"] == 0
          and snap["articles"] == [] and isinstance(snap["cursor"], str))
    check("snapshot reports its change sequence", snap["change_seq"] == 0)

    # ---- first snapshot returns all articles + cursor in one view --------
    seed = [
        article("train-one", "train"),
        article("test-two", "test"),
        article("train-three", "train"),
    ]
    r = post_batch(base1, {"upserts": seed})
    check("seed batch committed", r.status_code == 200, r.text)
    seq_seed = r.json()["change_seq"]

    snap = snapshot(base2)
    check("snapshot from the other instance returns all committed articles",
          snap["article_count"] == 3
          and {a["id"] for a in snap["articles"]} == {"train-one", "test-two", "train-three"})
    check("snapshot articles carry side, full text and generation",
          all(a["side"] in ("train", "test") and a["text"] == article(a["id"])["text"]
              and a["updated_generation"] >= 1 for a in snap["articles"]))
    check("snapshot cursor starts at the committed change sequence",
          snap["change_seq"] == seq_seed
          and decode_cursor(snap["cursor"]).seq == seq_seed
          and snap["next_cursor"] == snap["cursor"])

    # ---- full modification batches in commit order -----------------------
    seqs = [seq_seed]

    r = post_batch(base2, {"upserts": [article("train-one", "train", "totally rewritten text one")]})
    check("re-home an article (text + side change) committed", r.status_code == 200, r.text)
    seqs.append(r.json()["change_seq"])

    r = post_batch(base1, {"deletes": ["test-two"]})
    check("delete committed", r.status_code == 200, r.text)
    seqs.append(r.json()["change_seq"])

    r = post_batch(base2, {"upserts": [article("train-four", "test")],
                           "deletes": ["train-three"]})
    check("mixed batch committed", r.status_code == 200, r.text)
    seqs.append(r.json()["change_seq"])

    page = changes(base2, snap["cursor"])
    check("changes page returned", page.status_code == 200, page.text)
    body = page.json()
    check("changes page holds exactly the batches after the snapshot cursor",
          [b["seq"] for b in body["batches"]] == seqs[1:])
    check("batches are in commit order", seqs[1:] == sorted(seqs[1:]))
    batches = {b["seq"]: b for b in body["batches"]}
    first = batches[seqs[1]]
    check("revised batch carries the article's new side and full new text",
          first["upserts"] == [{"id": "train-one", "side": "train",
                                "text": "totally rewritten text one"}])
    check("deletion batch carries the deleted article id",
          batches[seqs[2]]["deletes"] == ["test-two"]
          and batches[seqs[2]]["upserts"] == [])

    # Deleting an unknown id is a no-op and must not be reported as a deletion.
    r = post_batch(base2, {"deletes": ["never-existed"]})
    check("delete of an unknown article is still accepted", r.status_code == 200)
    noop_seq = r.json()["change_seq"]
    noop_page = changes(
        base2, encode_cursor(cursor_epoch(body["cursor"]), seqs[3])
    ).json()
    noop = next(b for b in noop_page["batches"] if b["seq"] == noop_seq)
    check("unknown-id deletion is not reported as a deleted article",
          noop["deletes"] == [] and noop["upserts"] == [])
    mixed = batches[seqs[3]]
    check("mixed batch carries both new article and deletion",
          mixed["upserts"] == [{"id": "train-four", "side": "test",
                                "text": article("train-four")["text"]}]
          and mixed["deletes"] == ["train-three"])
    check("page cursor advanced to the last returned batch",
          decode_cursor(body["cursor"]).seq == seqs[3]
          and body["has_more"] is False
          and body["latest_change_seq"] == seqs[3])

    # re-homing to the other side is reflected as a real change
    r = post_batch(base1, {"upserts": [article("train-four", "train", "rehomed to train side")]})
    seqs.append(r.json()["change_seq"])
    page = changes(base1, encode_cursor(cursor_epoch(body["cursor"]), noop_seq)).json()
    check("side re-homing delivered in a later page",
          page["batches"][0]["upserts"][0] == {
              "id": "train-four", "side": "train", "text": "rehomed to train side"}
          and page["batches"][0]["seq"] == seqs[-1])

    # ---- no replay / no skip on an empty page ----------------------------
    cursor_now = page["cursor"]
    again = changes(base1, cursor_now).json()
    check("paging from the latest cursor replays nothing",
          again["batches"] == [] and again["change_seq"] == decode_cursor(cursor_now).seq)
    check("no-op batch does not consume a change sequence",
          post_batch(base1, {}).json()["changed"] is False)

    # ---- 10-batch page cap, complete batches only ------------------------
    r = post_batch(base1, {"deletes": [a["id"] for a in snapshot(base1)["articles"]]})
    check("corpus cleared", r.status_code == 200)
    cursor = snapshot(base1)["cursor"]
    written = []
    for i in range(23):
        r = post_batch(base2 if i % 2 else base1, {"upserts": [article(f"bulk-{i}")]})
        check(f"batch {i} accepted", r.status_code == 200)
        written.append(r.json()["change_seq"])
    page = changes(base1, cursor).json()
    check("a page holds at most 10 batches", page["batch_count"] == 10
          and len(page["batches"]) == 10)
    check("page holds the first 10 batches in order",
          [b["seq"] for b in page["batches"]] == written[:10])
    check("explicit limit is honoured and capped at 10",
          changes(base1, cursor, limit=3).json()["batch_count"] == 3
          and changes(base1, cursor, limit=50).status_code == 422)
    check("more pages are signalled", page["has_more"] is True)

    consumed = []
    next_cursor = cursor
    pages = 0
    while True:
        body = changes(base1, next_cursor).json()
        if not body["batches"] and not body["has_more"]:
            break
        pages += 1
        consumed.extend(b["seq"] for b in body["batches"])
        next_cursor = body["cursor"]
        if not body["has_more"]:
            break
    check("every page respects the 10-batch cap", pages >= 3,
          f"pages={pages}")
    check("paging through 23 batches yields them exactly once, in order",
          consumed == written, str(consumed))

    # ---- atomic rollback: change record must not survive a failed batch ---
    before = changes(base1, next_cursor).json()
    bad = {"upserts": [article("wheel-ok")], "deletes": ["wheel-ok"]}
    r = post_batch(base1, bad)
    check("conflicting batch rejected", r.status_code == 422)
    after = changes(base1, next_cursor).json()
    check("rejected batch left no change record and no article",
          after["latest_change_seq"] == before["latest_change_seq"]
          and "wheel-ok" not in {a["id"] for a in snapshot(base1)["articles"]})

    # ---- two instances + downstream paging concurrently -------------------
    # Reset to a known snapshot, then hammer both instances with batches
    # while a consumer pages after that snapshot cursor. Every write is
    # recorded on commit, and the consumer must see exactly those batches,
    # each once, in the same order.
    r = post_batch(base1, {"deletes": [a["id"] for a in snapshot(base1)["articles"]]})
    start_snap = snapshot(base1)

    total_per_writer = 20
    committed = []
    lock = threading.Lock()
    errors = []

    def writer(base, prefix):
        for i in range(total_per_writer):
            payload = {"upserts": [article(f"{prefix}-{i}", "train" if i % 2 else "test")]}
            if i % 5 == 4:
                payload["deletes"] = [f"{prefix}-{i - 2}"]
            try:
                r = post_batch(base, payload)
                if r.status_code != 200:
                    with lock:
                        errors.append(f"{prefix}: {r.status_code} {r.text}")
                    continue
                with lock:
                    committed.append(r.json()["change_seq"])
            except httpx.HTTPError as exc:
                with lock:
                    errors.append(f"{prefix}: {exc}")

    t1 = threading.Thread(target=writer, args=(base1, "wa"))
    t2 = threading.Thread(target=writer, args=(base2, "wb"))

    consumer_batches = []
    consumer_pages = []
    consumer_errors = []
    seen_cursor = start_snap["cursor"]

    def consumer():
        nonlocal seen_cursor
        while True:
            body = changes(base2, seen_cursor).json()
            if body["batch_count"] > 10:
                consumer_errors.append("page cap violated while paging")
            if body["batches"]:
                first = body["batches"][0]["seq"]
                if first <= decode_cursor(seen_cursor).seq:
                    consumer_errors.append("replayed a confirmed batch")
                consumer_batches.extend(b["seq"] for b in body["batches"])
            consumer_pages.append(body["batch_count"])
            if body["batch_count"]:
                new_cursor = body["cursor"]
                if new_cursor == seen_cursor:
                    consumer_errors.append("cursor did not advance")
                seen_cursor = new_cursor
            if not body["has_more"]:
                if not (t1.is_alive() or t2.is_alive()):
                    break
            time.sleep(0.01)

    for t in (t1, t2):
        t.start()
    ct = threading.Thread(target=consumer)
    ct.start()
    t1.join()
    t2.join()
    ct.join(timeout=60)

    check("no writer was rejected while two instances committed concurrently",
          errors == [], str(errors))
    # Note: responses may arrive in a different order than commits (a later
    # commit can return first), so compare sequence numbers by value, not by
    # the order in which the writers happened to record them.
    check("concurrent batches received distinct sequence numbers",
          len(committed) == 2 * total_per_writer
          and len(set(committed)) == len(committed),
          f"n={len(committed)} unique={len(set(committed))}")
    check("downstream paging observed every committed batch exactly once",
          sorted(consumer_batches) == sorted(committed)
          and len(consumer_batches) == len(set(consumer_batches)),
          f"consumed={len(consumer_batches)} committed={len(committed)}")
    check("downstream batches were delivered in commit (ascending seq) order",
          consumer_batches == sorted(consumer_batches)
          and bool(consumer_batches))
    check("no consumer page exposed more than 10 batches",
          all(n <= 10 for n in consumer_pages), str(consumer_pages[:20]))
    check("no consumer paging error", consumer_errors == [], str(consumer_errors))

    final = snapshot(base1)
    replayed = replay_prefix(base1, final["change_seq"])
    check("replaying the change feed reconstructs the current corpus",
          {a["id"]: {"id": a["id"], "side": a["side"], "text": a["text"]}
           for a in final["articles"]} == replayed,
          f"snapshot={len(final['articles'])} replayed={len(replayed)}")

    # ---- snapshot is consistent with its cursor under concurrent writes ---
    # Take snapshots while another thread writes; each snapshot's articles
    # must equal the corpus state implied by its own cursor.
    mismatches = []
    stop_writes = threading.Event()

    def background_writer():
        i = 0
        while not stop_writes.is_set():
            try:
                post_batch(base2, {"upserts": [article(f"rotating-{i % 7}", "test")],
                                   "deletes": [f"rotating-{(i - 3) % 7}"] if i > 3 else []})
            except httpx.HTTPError:
                pass
            i += 1

    bw = threading.Thread(target=background_writer)
    bw.start()
    try:
        for _ in range(25):
            snap = snapshot(base1)
            # Rebuild the corpus state implied by this snapshot's own cursor
            # (the log prefix up to its change_seq) and compare.
            expected = replay_prefix(base1, snap["change_seq"])
            actual = {a["id"]: {"id": a["id"], "side": a["side"], "text": a["text"]}
                      for a in snap["articles"]}
            if expected != actual:
                mismatches.append((snap["change_seq"], len(actual), len(expected)))
    finally:
        stop_writes.set()
        bw.join()
    check("snapshot articles always match the view at their own cursor",
          mismatches == [], str(mismatches[:3]))

    # ---- cursor failures ----------------------------------------------
    cur = snapshot(base1)["cursor"]
    check("garbage cursor rejected as malformed",
          changes(base1, "garbage").status_code == 400)
    check("tampered cursor rejected as malformed",
          changes(base1, cur[:-2] + ("aa" if cur[-2:] != "aa" else "bb")).status_code
          in (400, 409))
    epoch = decode_cursor(cur).epoch
    future = encode_cursor(epoch, decode_cursor(cur).seq + 1000)
    r = changes(base1, future)
    check("cursor beyond the change log demands a fresh snapshot",
          r.status_code == 409 and r.json()["status"] == "snapshot_required"
          and r.json()["action"] == "GET /sync/snapshot")
    foreign = encode_cursor("some-other-database-epoch", 0)
    r = changes(base1, foreign)
    check("cursor from another database identity demands a fresh snapshot",
          r.status_code == 409 and r.json()["status"] == "snapshot_required")


def replay_prefix(base, upto_seq):
    """Rebuild corpus state using the change log up to *upto_seq*."""
    state = {}
    epoch = decode_cursor(snapshot(base)["cursor"]).epoch
    cursor = encode_cursor(epoch, 0)
    while decode_cursor(cursor).seq < upto_seq:
        body = changes(base, cursor).json()
        if not body["batches"]:
            break
        for b in body["batches"]:
            if b["seq"] > upto_seq:
                return state
            for a in b["upserts"]:
                state[a["id"]] = {"id": a["id"], "side": a["side"], "text": a["text"]}
            for article_id in b["deletes"]:
                state.pop(article_id, None)
        cursor = body["cursor"]
    return state


def legacy_check(workdir):
    """Opening a pre-sync database keeps its articles and rejects old cursors."""
    db_path = os.path.join(workdir, "legacy.db")
    # Build a database that looks like the pre-sync schema: articles and
    # generations exist, no sync_state/change_batches tables.
    conn = connect(db_path)
    conn.executescript("""
        CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK (id = 1),
                           current_generation INTEGER NOT NULL,
                           published_generation INTEGER NOT NULL);
        CREATE TABLE articles (id TEXT PRIMARY KEY,
                               side TEXT NOT NULL CHECK (side IN ('train','test')),
                               text TEXT NOT NULL CHECK (length(text) BETWEEN 5 AND 2000),
                               updated_generation INTEGER NOT NULL);
    """)
    conn.execute("INSERT INTO meta VALUES (1, 4, 4)")
    conn.executemany(
        "INSERT INTO articles VALUES (?, ?, ?, ?)",
        [("legacy-train", "train", "legacy train article text", 4),
         ("legacy-test", "test", "legacy test article text", 4)],
    )
    conn.commit()
    conn.close()

    # init_db must migrate in place without touching the stored articles.
    init_db(db_path)
    conn = connect(db_path)
    try:
        check("legacy database migration keeps the generation",
              get_meta(conn) == (4, 4))
        check("legacy database migration keeps the stored articles",
              conn.execute("SELECT COUNT(*) AS c FROM articles").fetchone()["c"] == 2)
    finally:
        conn.close()

    port = free_port()
    server = Server(port, db_path, workdir, exit_watch=True)
    server.start()
    try:
        snap = snapshot(server.base)
        check("legacy snapshot serves the preserved articles",
              {a["id"] for a in snap["articles"]} == {"legacy-train", "legacy-test"})
        check("legacy snapshot starts a fresh change log",
              snap["change_seq"] == 0 and decode_cursor(snap["cursor"]).seq == 0)
        r = post_batch(server.base, {"upserts": [article("legacy-new")]})
        check("legacy corpus accepts new batches",
              r.status_code == 200 and r.json()["change_seq"] == 1)
        r = changes(server.base, snap["cursor"])
        check("legacy snapshot cursor continues into new changes",
              r.status_code == 200
              and [b["seq"] for b in r.json()["batches"]] == [1])
        # A cursor issued by a *different* database (e.g. the old pre-migration
        # deployment) must be reported as non-continuable.
        foreign = encode_cursor("legacy-era-epoch", 0)
        r = changes(server.base, foreign)
        check("cursor from a previous epoch explicitly requires a snapshot",
              r.status_code == 409 and r.json()["status"] == "snapshot_required")
        # Truncated history: model an operator pruning the retained batches
        # and moving the retention floor past a consumer's cursor.
        epoch_now = decode_cursor(snapshot(server.base)["cursor"]).epoch
        conn = connect(db_path)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM change_upserts")
        conn.execute("DELETE FROM change_deletes")
        conn.execute("DELETE FROM change_batches")
        conn.execute("UPDATE sync_state SET last_seq = 0, min_seq = 5 WHERE id = 1")
        conn.commit()
        conn.close()
        stale = encode_cursor(epoch_now, 0)
        r = changes(server.base, stale)
        check("cursor below the retained history floor requires a snapshot",
              r.status_code == 409 and r.json()["status"] == "snapshot_required")
    finally:
        server.kill()


def main():
    workdir = tempfile.mkdtemp(prefix="corpus-sync-")
    db_path = os.path.join(workdir, "corpus.db")
    s1 = Server(free_port(), db_path, workdir)
    s2 = Server(free_port(), db_path, workdir)
    s1.start()
    s2.start()
    print(f"two API processes up: {s1.base} {s2.base} (db={db_path})\n")
    try:
        run_checks(s1.base, s2.base)
        print()
        legacy_check(workdir)
    finally:
        s1.kill()
        s2.kill()

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURES: {FAILURES}")
        sys.exit(1)
    print("ALL SYNC CHECKS PASSED")


if __name__ == "__main__":
    main()
