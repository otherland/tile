#!/usr/bin/env python3
"""tile — Local Issue Tracker for AI Coding Agents. Single-file, stdlib-only."""

__version__ = "0.1.0"

import argparse
import json
import os
import os.path
import shutil
import sqlite3
import sys
import tempfile
import textwrap
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class TileError(Exception):
    pass

# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------

SCHEMA = """\
CREATE TABLE issues (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL DEFAULT 'task',
    priority INTEGER NOT NULL DEFAULT 2,
    status TEXT NOT NULL DEFAULT 'open',
    assignee TEXT,
    labels TEXT NOT NULL DEFAULT '[]',
    close_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at TEXT,
    seq INTEGER NOT NULL DEFAULT 1,
    flush_seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE dependencies (
    child_id TEXT NOT NULL REFERENCES issues(id),
    parent_id TEXT NOT NULL REFERENCES issues(id),
    PRIMARY KEY (child_id, parent_id)
);
CREATE TABLE comments (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL REFERENCES issues(id),
    author TEXT,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

VALID_TYPES = ("task", "bug", "feature", "epic")
VALID_STATUSES = ("open", "in_progress", "closed")
VALID_PRIORITIES = range(5)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _gen_id(prefix="tl"):
    return f"{prefix}-{os.urandom(3).hex()}"


def _find_workspace(start=None):
    d = os.path.abspath(start or os.getcwd())
    while True:
        candidate = os.path.join(d, ".tile", "tile.db")
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(d)
        if parent == d:
            return None
        d = parent


def _connect(db_path):
    conn = sqlite3.connect(db_path, timeout=0.2)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

class Tile:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = _connect(db_path)

    def close(self):
        self.conn.close()

    # -- ID resolution (prefix matching) ------------------------------------

    def _resolve_issue_id(self, prefix):
        rows = self.conn.execute(
            "SELECT id FROM issues WHERE id LIKE ? || '%'", (prefix,)
        ).fetchall()
        if len(rows) == 1:
            return rows[0]["id"]
        if len(rows) == 0:
            raise TileError(f"Not found: {prefix}")
        ids = [r["id"] for r in rows]
        raise TileError(f"Ambiguous prefix '{prefix}', matches: {', '.join(ids)}")

    def _resolve_comment_id(self, prefix):
        rows = self.conn.execute(
            "SELECT id FROM comments WHERE id LIKE ? || '%'", (prefix,)
        ).fetchall()
        if len(rows) == 1:
            return rows[0]["id"]
        if len(rows) == 0:
            raise TileError(f"Comment not found: {prefix}")
        ids = [r["id"] for r in rows]
        raise TileError(f"Ambiguous comment prefix '{prefix}', matches: {', '.join(ids)}")

    # -- Row to dict --------------------------------------------------------

    def _issue_dict(self, row):
        d = dict(row)
        d["labels"] = json.loads(d["labels"])
        d.pop("flush_seq", None)
        return d

    def _issue_dict_full(self, issue_id):
        row = self.conn.execute("SELECT * FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")
        d = self._issue_dict(row)
        d["blocked_by"] = [r["parent_id"] for r in self.conn.execute(
            "SELECT parent_id FROM dependencies WHERE child_id=?", (issue_id,)
        )]
        d["blocks"] = [r["child_id"] for r in self.conn.execute(
            "SELECT child_id FROM dependencies WHERE parent_id=?", (issue_id,)
        )]
        d["comments"] = [dict(r) for r in self.conn.execute(
            "SELECT id, author, body, created_at FROM comments WHERE issue_id=? ORDER BY created_at", (issue_id,)
        )]
        return d

    # -- Seq bump -----------------------------------------------------------

    def _bump_seq(self, issue_id):
        now = _now()
        self.conn.execute(
            "UPDATE issues SET seq = seq + 1, updated_at = ? WHERE id = ?",
            (now, issue_id),
        )

    # -- Commands -----------------------------------------------------------

    @staticmethod
    def init(directory=None, force=False):
        d = directory or os.getcwd()
        tile_dir = os.path.join(d, ".tile")
        if os.path.exists(tile_dir):
            if not force:
                raise TileError(f".tile/ already exists in {d}")
            shutil.rmtree(tile_dir)
        os.makedirs(tile_dir)
        db_path = os.path.join(tile_dir, "tile.db")
        conn = _connect(db_path)
        conn.executescript(SCHEMA)
        conn.commit()
        conn.close()
        return db_path

    def create(self, title, type_="task", priority=2, description="",
               assignee=None, labels=None):
        if not title:
            raise TileError("Title cannot be empty")
        if type_ not in VALID_TYPES:
            raise TileError(f"Invalid type: {type_}. Must be one of {VALID_TYPES}")
        if priority not in VALID_PRIORITIES:
            raise TileError(f"Invalid priority: {priority}. Must be 0-4")
        labels_list = labels or []
        now = _now()
        issue_id = _gen_id("tl")
        try:
            self.conn.execute(
                """INSERT INTO issues (id, title, description, type, priority, status,
                   assignee, labels, created_at, updated_at, seq)
                   VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, 1)""",
                (issue_id, title, description, type_, priority, assignee,
                 json.dumps(labels_list), now, now),
            )
        except sqlite3.IntegrityError:
            # Collision — retry once
            issue_id = _gen_id("tl")
            try:
                self.conn.execute(
                    """INSERT INTO issues (id, title, description, type, priority, status,
                       assignee, labels, created_at, updated_at, seq)
                       VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, 1)""",
                    (issue_id, title, description, type_, priority, assignee,
                     json.dumps(labels_list), now, now),
                )
            except sqlite3.IntegrityError:
                raise TileError("ID collision after retry — this is astronomically unlikely")
        self.conn.commit()
        return self._issue_dict_full(issue_id)

    def show(self, issue_id_prefix):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        return self._issue_dict_full(issue_id)

    def update(self, issue_id_prefix, title=None, type_=None, priority=None,
               status=None, description=None, assignee=None, reason=None):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        row = self.conn.execute("SELECT * FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")

        if reason is not None and status != "closed":
            raise TileError("--reason requires --status closed")

        updates = {}
        if title is not None:
            if not title:
                raise TileError("Title cannot be empty")
            updates["title"] = title
        if type_ is not None:
            if type_ not in VALID_TYPES:
                raise TileError(f"Invalid type: {type_}")
            updates["type"] = type_
        if priority is not None:
            if priority not in VALID_PRIORITIES:
                raise TileError(f"Invalid priority: {priority}")
            updates["priority"] = priority
        if description is not None:
            updates["description"] = description
        if assignee is not None:
            updates["assignee"] = assignee

        if status is not None:
            if status not in VALID_STATUSES:
                raise TileError(f"Invalid status: {status}")
            if status == row["status"]:
                raise TileError(f"Already {status}")
            updates["status"] = status
            if status == "closed":
                updates["closed_at"] = _now()
                if reason is not None:
                    updates["close_reason"] = reason
            else:
                # Reopening
                if row["status"] == "closed":
                    updates["closed_at"] = None
                    updates["close_reason"] = None

        if not updates:
            raise TileError("No update flags provided")

        now = _now()
        sets = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values())
        self.conn.execute(
            f"UPDATE issues SET {sets}, seq = seq + 1, updated_at = ? WHERE id = ?",
            vals + [now, issue_id],
        )
        self.conn.commit()
        return self._issue_dict_full(issue_id)

    def delete(self, issue_id_prefix):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        row = self.conn.execute("SELECT id FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")
        self.conn.execute("DELETE FROM comments WHERE issue_id=?", (issue_id,))
        self.conn.execute("DELETE FROM dependencies WHERE child_id=? OR parent_id=?", (issue_id, issue_id))
        self.conn.execute("DELETE FROM issues WHERE id=?", (issue_id,))
        self.conn.commit()

    def list_issues(self, status=None, priority=None, type_=None, assignee=None,
                    label=None, search=None, ready=False, blocked=False,
                    stale=None, sort=None, reverse=False):
        conditions = []
        params = []

        if ready:
            conditions.append("status != 'closed'")
        elif blocked:
            conditions.append("status != 'closed'")
        elif stale is not None:
            conditions.append("status != 'closed'")

        if status:
            statuses = [s.strip() for s in status.split(",")]
            placeholders = ",".join("?" * len(statuses))
            conditions.append(f"status IN ({placeholders})")
            params.extend(statuses)

        if priority is not None:
            if isinstance(priority, str) and "-" in priority:
                lo, hi = priority.split("-", 1)
                conditions.append("priority >= ? AND priority <= ?")
                params.extend([int(lo), int(hi)])
            else:
                conditions.append("priority = ?")
                params.append(int(priority))

        if type_:
            conditions.append("type = ?")
            params.append(type_)

        if assignee:
            conditions.append("assignee = ?")
            params.append(assignee)

        if label:
            conditions.append("labels LIKE ?")
            params.append(f'%"{label}"%')

        if search:
            conditions.append("(title LIKE ? OR description LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        if stale is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=stale)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conditions.append("updated_at < ?")
            params.append(cutoff)

        where = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT * FROM issues WHERE {where}"

        rows = self.conn.execute(query, params).fetchall()
        issues = [self._issue_dict(r) for r in rows]

        if ready:
            # Filter to issues with all deps satisfied
            open_ids = {r["id"] for r in self.conn.execute(
                "SELECT id FROM issues WHERE status != 'closed'"
            )}
            issues = [
                i for i in issues
                if not any(
                    r["parent_id"] in open_ids
                    for r in self.conn.execute(
                        "SELECT parent_id FROM dependencies WHERE child_id=?", (i["id"],)
                    )
                )
            ]
            # Compute impact
            impact_map = self._compute_impact([i["id"] for i in issues])
            for i in issues:
                i["impact"] = impact_map.get(i["id"], 0)

        if blocked:
            open_ids = {r["id"] for r in self.conn.execute(
                "SELECT id FROM issues WHERE status != 'closed'"
            )}
            result = []
            for i in issues:
                blockers = [
                    r["parent_id"] for r in self.conn.execute(
                        "SELECT parent_id FROM dependencies WHERE child_id=?", (i["id"],)
                    ) if r["parent_id"] in open_ids
                ]
                if blockers:
                    i["blocked_by"] = blockers
                    result.append(i)
            issues = result

        # Sorting
        if sort:
            issues.sort(key=lambda i: (i.get(sort, ""),), reverse=reverse)
        elif ready:
            issues.sort(key=lambda i: (-i["impact"], i["priority"], i["created_at"]),
                        reverse=reverse)
        else:
            issues.sort(key=lambda i: (i["priority"], i["created_at"]),
                        reverse=reverse)

        return issues

    def _compute_impact(self, ready_ids):
        # Build adjacency: parent -> children
        adj = defaultdict(set)
        for r in self.conn.execute("SELECT parent_id, child_id FROM dependencies"):
            adj[r["parent_id"]].add(r["child_id"])
        open_ids = {r["id"] for r in self.conn.execute(
            "SELECT id FROM issues WHERE status != 'closed'"
        )}
        impact = {}
        for rid in ready_ids:
            visited = set()
            queue = list(adj.get(rid, set()))
            while queue:
                node = queue.pop()
                if node in visited:
                    continue
                visited.add(node)
                if node in open_ids:
                    queue.extend(adj.get(node, set()))
            impact[rid] = len(visited & open_ids)
        return impact

    def prime(self, limit=10, hours=24):
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        stale_cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

        # In progress
        in_progress = [self._issue_dict(r) for r in self.conn.execute(
            "SELECT * FROM issues WHERE status='in_progress'"
        )]

        # Ready (with impact)
        ready = self.list_issues(ready=True)[:limit]

        # Blocked
        blocked_rows = self.conn.execute(
            "SELECT DISTINCT child_id FROM dependencies d "
            "JOIN issues p ON d.parent_id = p.id "
            "JOIN issues c ON d.child_id = c.id "
            "WHERE p.status != 'closed' AND c.status != 'closed'"
        ).fetchall()
        blocked_count = len(blocked_rows)

        # Top blockers: open issues that transitively block the most others
        adj = defaultdict(set)
        for r in self.conn.execute("SELECT parent_id, child_id FROM dependencies"):
            adj[r["parent_id"]].add(r["child_id"])
        open_ids = {r["id"] for r in self.conn.execute(
            "SELECT id FROM issues WHERE status != 'closed'"
        )}
        blocker_impact = {}
        for pid in open_ids:
            if pid not in adj:
                continue
            visited = set()
            queue = list(adj[pid])
            while queue:
                node = queue.pop()
                if node in visited:
                    continue
                visited.add(node)
                if node in open_ids:
                    queue.extend(adj.get(node, set()))
            count = len(visited & open_ids)
            if count > 0:
                blocker_impact[pid] = count
        top_blocker_ids = sorted(blocker_impact, key=blocker_impact.get, reverse=True)[:5]
        top_blockers = []
        for bid in top_blocker_ids:
            d = self._issue_dict_full(bid)
            d["impact"] = blocker_impact[bid]
            top_blockers.append(d)

        # Recently closed/created
        recently_closed = [self._issue_dict(r) for r in self.conn.execute(
            "SELECT * FROM issues WHERE status='closed' AND closed_at >= ?", (cutoff,)
        )]
        recently_created = [self._issue_dict(r) for r in self.conn.execute(
            "SELECT * FROM issues WHERE created_at >= ?", (cutoff,)
        )]

        # Stats
        total = self.conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
        open_count = self.conn.execute("SELECT COUNT(*) FROM issues WHERE status='open'").fetchone()[0]
        ip_count = self.conn.execute("SELECT COUNT(*) FROM issues WHERE status='in_progress'").fetchone()[0]
        closed_count = self.conn.execute("SELECT COUNT(*) FROM issues WHERE status='closed'").fetchone()[0]

        # Stale count
        stale_count = self.conn.execute(
            "SELECT COUNT(*) FROM issues WHERE status != 'closed' AND updated_at < ?",
            (stale_cutoff,)
        ).fetchone()[0]

        return {
            "in_progress": in_progress,
            "ready": ready,
            "blocked_summary": {
                "count": blocked_count,
                "top_blockers": top_blockers,
            },
            "recently_closed": recently_closed,
            "recently_created": recently_created,
            "stats": {
                "total": total,
                "open": open_count,
                "in_progress": ip_count,
                "closed": closed_count,
            },
            "stale_count": stale_count,
        }

    def batch(self, operations):
        results = []
        try:
            self.conn.execute("BEGIN")
            for i, op in enumerate(operations):
                op = dict(op)  # copy
                # Resolve back-references
                for key in list(op.keys()):
                    val = op[key]
                    if isinstance(val, str) and val.startswith("$"):
                        ref_idx = int(val[1:])
                        if ref_idx >= len(results):
                            raise TileError(f"Back-reference {val} out of range")
                        op[key] = results[ref_idx].get("id", results[ref_idx].get("result"))

                op_type = op.pop("op")
                try:
                    result = self._batch_op(op_type, op)
                    results.append(result)
                except Exception as e:
                    raise TileError(f"Op {i} ({op_type}) failed: {e}")
            self.conn.commit()
        except TileError as e:
            self.conn.rollback()
            # Find the failed_at index from the error message
            msg = str(e)
            failed_at = len(results)
            if msg.startswith("Op "):
                try:
                    failed_at = int(msg.split(" ")[1])
                except (ValueError, IndexError):
                    pass
            raise TileError(json.dumps({"error": msg, "failed_at": failed_at}))
        except Exception as e:
            self.conn.rollback()
            raise TileError(json.dumps({"error": str(e), "failed_at": len(results)}))
        return results

    def _batch_op(self, op_type, params):
        if op_type == "create":
            labels = params.get("labels", [])
            if isinstance(labels, str):
                labels = [l.strip() for l in labels.split(",") if l.strip()]
            issue = self._create_no_commit(
                title=params["title"],
                type_=params.get("type", "task"),
                priority=params.get("priority", 2),
                description=params.get("description", ""),
                assignee=params.get("assignee"),
                labels=labels,
            )
            return {"ok": True, "id": issue["id"]}
        elif op_type == "update":
            issue_id = self._resolve_issue_id(params["id"])
            self._update_no_commit(
                issue_id,
                title=params.get("title"),
                type_=params.get("type"),
                priority=params.get("priority"),
                status=params.get("status"),
                description=params.get("description"),
                assignee=params.get("assignee"),
                reason=params.get("reason"),
            )
            return {"ok": True, "id": issue_id}
        elif op_type == "delete":
            issue_id = self._resolve_issue_id(params["id"])
            self._delete_no_commit(issue_id)
            return {"ok": True, "id": issue_id}
        elif op_type == "dep_add":
            child = self._resolve_issue_id(params["child"])
            parent = self._resolve_issue_id(params["parent"])
            self._dep_add_no_commit(child, parent)
            return {"ok": True}
        elif op_type == "dep_remove":
            child = self._resolve_issue_id(params["child"])
            parent = self._resolve_issue_id(params["parent"])
            self._dep_remove_no_commit(child, parent)
            return {"ok": True}
        elif op_type == "label_add":
            issue_id = self._resolve_issue_id(params["id"])
            label = params["label"]
            labels = [label] if isinstance(label, str) else label
            self._label_add_no_commit(issue_id, labels)
            return {"ok": True, "id": issue_id}
        elif op_type == "label_remove":
            issue_id = self._resolve_issue_id(params["id"])
            self._label_remove_no_commit(issue_id, params["label"])
            return {"ok": True, "id": issue_id}
        elif op_type == "comment_add":
            issue_id = self._resolve_issue_id(params["id"])
            cid = self._comment_add_no_commit(issue_id, params["body"], params.get("author"))
            return {"ok": True, "id": cid}
        else:
            raise TileError(f"Unknown batch op: {op_type}")

    # -- No-commit variants for batch (operate within caller's transaction) --

    def _create_no_commit(self, title, type_="task", priority=2, description="",
                          assignee=None, labels=None):
        if not title:
            raise TileError("Title cannot be empty")
        if type_ not in VALID_TYPES:
            raise TileError(f"Invalid type: {type_}")
        if int(priority) not in VALID_PRIORITIES:
            raise TileError(f"Invalid priority: {priority}")
        labels_list = labels or []
        now = _now()
        issue_id = _gen_id("tl")
        try:
            self.conn.execute(
                """INSERT INTO issues (id, title, description, type, priority, status,
                   assignee, labels, created_at, updated_at, seq)
                   VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, 1)""",
                (issue_id, title, description, type_, int(priority), assignee,
                 json.dumps(labels_list), now, now),
            )
        except sqlite3.IntegrityError:
            issue_id = _gen_id("tl")
            self.conn.execute(
                """INSERT INTO issues (id, title, description, type, priority, status,
                   assignee, labels, created_at, updated_at, seq)
                   VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, 1)""",
                (issue_id, title, description, type_, int(priority), assignee,
                 json.dumps(labels_list), now, now),
            )
        return {"id": issue_id}

    def _update_no_commit(self, issue_id, title=None, type_=None, priority=None,
                          status=None, description=None, assignee=None, reason=None):
        row = self.conn.execute("SELECT * FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")
        if reason is not None and status != "closed":
            raise TileError("--reason requires --status closed")
        updates = {}
        if title is not None:
            if not title:
                raise TileError("Title cannot be empty")
            updates["title"] = title
        if type_ is not None:
            if type_ not in VALID_TYPES:
                raise TileError(f"Invalid type: {type_}")
            updates["type"] = type_
        if priority is not None:
            if int(priority) not in VALID_PRIORITIES:
                raise TileError(f"Invalid priority: {priority}")
            updates["priority"] = int(priority)
        if description is not None:
            updates["description"] = description
        if assignee is not None:
            updates["assignee"] = assignee
        if status is not None:
            if status not in VALID_STATUSES:
                raise TileError(f"Invalid status: {status}")
            if status == row["status"]:
                raise TileError(f"Already {status}")
            updates["status"] = status
            if status == "closed":
                updates["closed_at"] = _now()
                if reason is not None:
                    updates["close_reason"] = reason
            elif row["status"] == "closed":
                updates["closed_at"] = None
                updates["close_reason"] = None
        if not updates:
            raise TileError("No update flags provided")
        now = _now()
        sets = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values())
        self.conn.execute(
            f"UPDATE issues SET {sets}, seq = seq + 1, updated_at = ? WHERE id = ?",
            vals + [now, issue_id],
        )

    def _delete_no_commit(self, issue_id):
        row = self.conn.execute("SELECT id FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")
        self.conn.execute("DELETE FROM comments WHERE issue_id=?", (issue_id,))
        self.conn.execute("DELETE FROM dependencies WHERE child_id=? OR parent_id=?", (issue_id, issue_id))
        self.conn.execute("DELETE FROM issues WHERE id=?", (issue_id,))

    def _dep_add_no_commit(self, child_id, parent_id):
        if child_id == parent_id:
            raise TileError("Cannot add self-referential dependency")
        for eid in (child_id, parent_id):
            if not self.conn.execute("SELECT 1 FROM issues WHERE id=?", (eid,)).fetchone():
                raise TileError(f"Not found: {eid}")
        if self.conn.execute(
            "SELECT 1 FROM dependencies WHERE child_id=? AND parent_id=?",
            (child_id, parent_id)
        ).fetchone():
            raise TileError("Dependency already exists")
        # Cycle detection: DFS from parent_id upward; if child_id reachable, reject
        if self._would_cycle(child_id, parent_id):
            raise TileError("Cycle detected")
        self.conn.execute(
            "INSERT INTO dependencies (child_id, parent_id) VALUES (?, ?)",
            (child_id, parent_id),
        )
        now = _now()
        self.conn.execute("UPDATE issues SET seq = seq + 1, updated_at = ? WHERE id = ?", (now, child_id))
        self.conn.execute("UPDATE issues SET seq = seq + 1, updated_at = ? WHERE id = ?", (now, parent_id))

    def _dep_remove_no_commit(self, child_id, parent_id):
        cur = self.conn.execute(
            "DELETE FROM dependencies WHERE child_id=? AND parent_id=?",
            (child_id, parent_id),
        )
        if cur.rowcount == 0:
            raise TileError("Dependency not found")
        now = _now()
        self.conn.execute("UPDATE issues SET seq = seq + 1, updated_at = ? WHERE id = ?", (now, child_id))
        self.conn.execute("UPDATE issues SET seq = seq + 1, updated_at = ? WHERE id = ?", (now, parent_id))

    def _label_add_no_commit(self, issue_id, labels):
        row = self.conn.execute("SELECT labels FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")
        current = json.loads(row["labels"])
        changed = False
        for l in labels:
            if l not in current:
                current.append(l)
                changed = True
        if changed:
            self.conn.execute(
                "UPDATE issues SET labels = ?, seq = seq + 1, updated_at = ? WHERE id = ?",
                (json.dumps(current), _now(), issue_id),
            )

    def _label_remove_no_commit(self, issue_id, label):
        row = self.conn.execute("SELECT labels FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")
        current = json.loads(row["labels"])
        if label not in current:
            raise TileError(f"Label '{label}' not present")
        current.remove(label)
        self.conn.execute(
            "UPDATE issues SET labels = ?, seq = seq + 1, updated_at = ? WHERE id = ?",
            (json.dumps(current), _now(), issue_id),
        )

    def _comment_add_no_commit(self, issue_id, body, author=None):
        if not self.conn.execute("SELECT 1 FROM issues WHERE id=?", (issue_id,)).fetchone():
            raise TileError(f"Not found: {issue_id}")
        if not body:
            raise TileError("Comment body cannot be empty")
        cid = _gen_id("c")
        try:
            self.conn.execute(
                "INSERT INTO comments (id, issue_id, author, body, created_at) VALUES (?, ?, ?, ?, ?)",
                (cid, issue_id, author, body, _now()),
            )
        except sqlite3.IntegrityError:
            cid = _gen_id("c")
            self.conn.execute(
                "INSERT INTO comments (id, issue_id, author, body, created_at) VALUES (?, ?, ?, ?, ?)",
                (cid, issue_id, author, body, _now()),
            )
        self.conn.execute(
            "UPDATE issues SET seq = seq + 1, updated_at = ? WHERE id = ?",
            (_now(), issue_id),
        )
        return cid

    def _would_cycle(self, child_id, parent_id):
        """Check if adding child->parent dep would create a cycle."""
        visited = set()
        stack = [parent_id]
        while stack:
            node = stack.pop()
            if node == child_id:
                return True
            if node in visited:
                continue
            visited.add(node)
            for r in self.conn.execute(
                "SELECT parent_id FROM dependencies WHERE child_id=?", (node,)
            ):
                stack.append(r["parent_id"])
        return False

    # -- Dep commands (with commit) -----------------------------------------

    def dep_add(self, child_prefix, parent_prefix):
        child_id = self._resolve_issue_id(child_prefix)
        parent_id = self._resolve_issue_id(parent_prefix)
        self._dep_add_no_commit(child_id, parent_id)
        self.conn.commit()

    def dep_remove(self, child_prefix, parent_prefix):
        child_id = self._resolve_issue_id(child_prefix)
        parent_id = self._resolve_issue_id(parent_prefix)
        self._dep_remove_no_commit(child_id, parent_id)
        self.conn.commit()

    def dep_list(self, issue_id_prefix):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        if not self.conn.execute("SELECT 1 FROM issues WHERE id=?", (issue_id,)).fetchone():
            raise TileError(f"Not found: {issue_id}")
        blocked_by = [r["parent_id"] for r in self.conn.execute(
            "SELECT parent_id FROM dependencies WHERE child_id=?", (issue_id,)
        )]
        blocks = [r["child_id"] for r in self.conn.execute(
            "SELECT child_id FROM dependencies WHERE parent_id=?", (issue_id,)
        )]
        return {"blocked_by": blocked_by, "blocks": blocks}

    def dep_tree(self, issue_id_prefix):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        if not self.conn.execute("SELECT 1 FROM issues WHERE id=?", (issue_id,)).fetchone():
            raise TileError(f"Not found: {issue_id}")
        adj = defaultdict(set)
        for r in self.conn.execute("SELECT parent_id, child_id FROM dependencies"):
            adj[r["parent_id"]].add(r["child_id"])
        return self._build_tree(issue_id, adj, set())

    def _build_tree(self, node, adj, visited):
        visited.add(node)
        children = []
        for child in sorted(adj.get(node, set())):
            if child in visited:
                children.append({"id": child, "visited": True})
            else:
                children.append({"id": child, "children": self._build_tree(child, adj, visited)})
        return children

    # -- Label commands (with commit) ---------------------------------------

    def label_add(self, issue_id_prefix, labels):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        self._label_add_no_commit(issue_id, labels)
        self.conn.commit()

    def label_remove(self, issue_id_prefix, label):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        self._label_remove_no_commit(issue_id, label)
        self.conn.commit()

    def label_list(self, issue_id_prefix=None, all_labels=False):
        if all_labels:
            rows = self.conn.execute("SELECT labels FROM issues").fetchall()
            all_l = set()
            for r in rows:
                all_l.update(json.loads(r["labels"]))
            return sorted(all_l)
        issue_id = self._resolve_issue_id(issue_id_prefix)
        row = self.conn.execute("SELECT labels FROM issues WHERE id=?", (issue_id,)).fetchone()
        if not row:
            raise TileError(f"Not found: {issue_id}")
        return json.loads(row["labels"])

    # -- Comment commands (with commit) -------------------------------------

    def comment_add(self, issue_id_prefix, body, author=None):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        cid = self._comment_add_no_commit(issue_id, body, author)
        self.conn.commit()
        return {"id": cid}

    def comment_list(self, issue_id_prefix):
        issue_id = self._resolve_issue_id(issue_id_prefix)
        if not self.conn.execute("SELECT 1 FROM issues WHERE id=?", (issue_id,)).fetchone():
            raise TileError(f"Not found: {issue_id}")
        return [dict(r) for r in self.conn.execute(
            "SELECT id, author, body, created_at FROM comments WHERE issue_id=? ORDER BY created_at",
            (issue_id,),
        )]

    # -- Sync ---------------------------------------------------------------

    def sync_push(self):
        rows = self.conn.execute("SELECT * FROM issues ORDER BY created_at").fetchall()
        if not rows:
            return
        lines = []
        for r in rows:
            d = self._issue_dict(r)
            d["blocked_by"] = [dep["parent_id"] for dep in self.conn.execute(
                "SELECT parent_id FROM dependencies WHERE child_id=?", (r["id"],)
            )]
            d["comments"] = [dict(c) for c in self.conn.execute(
                "SELECT id, author, body, created_at FROM comments WHERE issue_id=? ORDER BY created_at",
                (r["id"],),
            )]
            lines.append(json.dumps(d, ensure_ascii=False))

        tile_dir = os.path.dirname(self.db_path)
        jsonl_path = os.path.join(tile_dir, "issues.jsonl")
        fd, tmp_path = tempfile.mkstemp(dir=tile_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                for line in lines:
                    f.write(line + "\n")
            os.rename(tmp_path, jsonl_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

        self.conn.execute("UPDATE issues SET flush_seq = seq")
        self.conn.commit()

    def sync_pull(self, prefer_remote=False):
        tile_dir = os.path.dirname(self.db_path)
        jsonl_path = os.path.join(tile_dir, "issues.jsonl")
        if not os.path.exists(jsonl_path):
            raise TileError("issues.jsonl not found")
        with open(jsonl_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return
        all_imported_ids = set()
        for line in content.split("\n"):
            obj = json.loads(line)
            all_imported_ids.add(obj["id"])

        for line in content.split("\n"):
            obj = json.loads(line)
            remote_id = obj["id"]
            remote_seq = obj["seq"]

            local = self.conn.execute("SELECT seq FROM issues WHERE id=?", (remote_id,)).fetchone()
            if local is None:
                # Insert new
                self._import_issue(obj)
            elif local["seq"] < remote_seq:
                # Remote is newer
                self._import_issue(obj, overwrite=True)
            elif local["seq"] > remote_seq and prefer_remote:
                self._import_issue(obj, overwrite=True)
            # else: skip (equal or local is newer)

        self.conn.commit()

    def _import_issue(self, obj, overwrite=False):
        labels = obj.get("labels", [])
        if isinstance(labels, list):
            labels = json.dumps(labels)
        if overwrite:
            self.conn.execute("DELETE FROM comments WHERE issue_id=?", (obj["id"],))
            self.conn.execute("DELETE FROM dependencies WHERE child_id=?", (obj["id"],))
            self.conn.execute("DELETE FROM issues WHERE id=?", (obj["id"],))

        self.conn.execute(
            """INSERT INTO issues (id, title, description, type, priority, status,
               assignee, labels, close_reason, created_at, updated_at, closed_at, seq)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (obj["id"], obj["title"], obj.get("description", ""),
             obj.get("type", "task"), obj.get("priority", 2),
             obj.get("status", "open"), obj.get("assignee"),
             labels, obj.get("close_reason"),
             obj["created_at"], obj["updated_at"], obj.get("closed_at"),
             obj["seq"]),
        )

        for parent_id in obj.get("blocked_by", []):
            # Only add dep if parent exists in DB
            if self.conn.execute("SELECT 1 FROM issues WHERE id=?", (parent_id,)).fetchone():
                try:
                    self.conn.execute(
                        "INSERT INTO dependencies (child_id, parent_id) VALUES (?, ?)",
                        (obj["id"], parent_id),
                    )
                except sqlite3.IntegrityError:
                    pass

        for c in obj.get("comments", []):
            try:
                self.conn.execute(
                    "INSERT INTO comments (id, issue_id, author, body, created_at) VALUES (?, ?, ?, ?, ?)",
                    (c["id"], obj["id"], c.get("author"), c["body"], c["created_at"]),
                )
            except sqlite3.IntegrityError:
                pass

    def sync_status(self):
        tile_dir = os.path.dirname(self.db_path)
        jsonl_path = os.path.join(tile_dir, "issues.jsonl")

        db_issues = {r["id"]: dict(r) for r in self.conn.execute("SELECT * FROM issues")}

        jsonl_issues = {}
        if os.path.exists(jsonl_path):
            with open(jsonl_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        obj = json.loads(line)
                        jsonl_issues[obj["id"]] = obj

        db_only = sorted(set(db_issues) - set(jsonl_issues))
        jsonl_only = sorted(set(jsonl_issues) - set(db_issues))
        modified = sorted(
            k for k in db_issues
            if db_issues[k]["seq"] != db_issues[k]["flush_seq"]
        )
        seq_differs = sorted(
            k for k in set(db_issues) & set(jsonl_issues)
            if db_issues[k]["seq"] != jsonl_issues[k]["seq"]
        )

        return {
            "db_count": len(db_issues),
            "jsonl_count": len(jsonl_issues),
            "db_only": db_only,
            "jsonl_only": jsonl_only,
            "modified_since_push": modified,
            "seq_differs": seq_differs,
        }

    # -- Stats --------------------------------------------------------------

    def stats(self, by=None):
        if by:
            if by not in ("status", "type", "priority", "assignee"):
                raise TileError(f"Invalid --by field: {by}")
            rows = self.conn.execute(
                f"SELECT {by}, COUNT(*) as count FROM issues GROUP BY {by} ORDER BY {by}"
            ).fetchall()
            return {r[by] if r[by] is not None else "none": r["count"] for r in rows}

        total = self.conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
        open_c = self.conn.execute("SELECT COUNT(*) FROM issues WHERE status='open'").fetchone()[0]
        ip_c = self.conn.execute("SELECT COUNT(*) FROM issues WHERE status='in_progress'").fetchone()[0]
        closed_c = self.conn.execute("SELECT COUNT(*) FROM issues WHERE status='closed'").fetchone()[0]
        dep_c = self.conn.execute("SELECT COUNT(*) FROM dependencies").fetchone()[0]
        comment_c = self.conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]

        by_priority = {}
        for r in self.conn.execute("SELECT priority, COUNT(*) as c FROM issues GROUP BY priority ORDER BY priority"):
            by_priority[r["priority"]] = r["c"]
        by_type = {}
        for r in self.conn.execute("SELECT type, COUNT(*) as c FROM issues GROUP BY type ORDER BY type"):
            by_type[r["type"]] = r["c"]

        return {
            "total": total,
            "open": open_c,
            "in_progress": ip_c,
            "closed": closed_c,
            "dependencies": dep_c,
            "comments": comment_c,
            "by_priority": by_priority,
            "by_type": by_type,
        }

    # -- Doctor -------------------------------------------------------------

    def doctor(self):
        checks = []

        # Tables exist
        tables = {r[0] for r in self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        for t in ("issues", "dependencies", "comments"):
            ok = t in tables
            checks.append({"check": f"table_{t}", "ok": ok})

        # Orphan deps
        orphan_deps = self.conn.execute(
            """SELECT COUNT(*) FROM dependencies d
               WHERE NOT EXISTS (SELECT 1 FROM issues WHERE id = d.child_id)
               OR NOT EXISTS (SELECT 1 FROM issues WHERE id = d.parent_id)"""
        ).fetchone()[0]
        checks.append({"check": "no_orphan_deps", "ok": orphan_deps == 0,
                        "detail": f"{orphan_deps} orphan(s)" if orphan_deps else None})

        # Orphan comments
        orphan_comments = self.conn.execute(
            """SELECT COUNT(*) FROM comments c
               WHERE NOT EXISTS (SELECT 1 FROM issues WHERE id = c.issue_id)"""
        ).fetchone()[0]
        checks.append({"check": "no_orphan_comments", "ok": orphan_comments == 0,
                        "detail": f"{orphan_comments} orphan(s)" if orphan_comments else None})

        # Cycles
        has_cycle = self._detect_cycles()
        checks.append({"check": "no_cycles", "ok": not has_cycle})

        # JSONL parseable
        tile_dir = os.path.dirname(self.db_path)
        jsonl_path = os.path.join(tile_dir, "issues.jsonl")
        if os.path.exists(jsonl_path):
            try:
                with open(jsonl_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            json.loads(line)
                checks.append({"check": "jsonl_parseable", "ok": True})
            except (json.JSONDecodeError, Exception) as e:
                checks.append({"check": "jsonl_parseable", "ok": False, "detail": str(e)})

        all_ok = all(c["ok"] for c in checks)
        return checks, all_ok

    def _detect_cycles(self):
        adj = defaultdict(set)
        all_nodes = set()
        for r in self.conn.execute("SELECT child_id, parent_id FROM dependencies"):
            adj[r["child_id"]].add(r["parent_id"])
            all_nodes.add(r["child_id"])
            all_nodes.add(r["parent_id"])

        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in all_nodes}

        def dfs(node):
            color[node] = GRAY
            for neighbor in adj.get(node, set()):
                if color.get(neighbor, WHITE) == GRAY:
                    return True
                if color.get(neighbor, WHITE) == WHITE and dfs(neighbor):
                    return True
            color[node] = BLACK
            return False

        for node in all_nodes:
            if color[node] == WHITE:
                if dfs(node):
                    return True
        return False


# ---------------------------------------------------------------------------
# Human output formatting
# ---------------------------------------------------------------------------

class Formatter:
    PRIORITY_COLORS = {0: "\033[31m", 1: "\033[33m", 2: "", 3: "\033[2m", 4: "\033[2m"}
    STATUS_COLORS = {"open": "\033[32m", "in_progress": "\033[36m", "closed": "\033[2m"}
    RESET = "\033[0m"

    def __init__(self, color=True):
        self.color = color

    def _c(self, code, text):
        if not self.color or not code:
            return str(text)
        return f"{code}{text}{self.RESET}"

    def format_list(self, issues, show_impact=False):
        if not issues:
            return "No issues found."
        try:
            width = os.get_terminal_size().columns
        except OSError:
            width = 80

        lines = []
        if show_impact:
            header = f"{'ID':<11} {'IMPACT':>6} {'PRI':>3} {'TYPE':<8} {'STATUS':<12} TITLE"
        else:
            header = f"{'ID':<11} {'PRI':>3} {'TYPE':<8} {'STATUS':<12} TITLE"
        lines.append(header)

        for i in issues:
            pri_c = self.PRIORITY_COLORS.get(i["priority"], "")
            stat_c = self.STATUS_COLORS.get(i["status"], "")
            title = i["title"]
            if show_impact:
                prefix = f"{i['id']:<11} {i.get('impact', 0):>6} "
                max_title = width - 45
            else:
                prefix = f"{i['id']:<11} "
                max_title = width - 38
            if len(title) > max_title:
                title = title[:max_title - 1] + "…"
            pri_str = str(i["priority"]).rjust(3)
            type_str = i["type"].ljust(8)
            stat_str = i["status"].ljust(12)
            line = (
                prefix
                + self._c(pri_c, pri_str) + " "
                + type_str + " "
                + self._c(stat_c, stat_str) + " "
                + title
            )
            lines.append(line)
        return "\n".join(lines)

    def format_show(self, issue):
        lines = [
            f"ID:          {issue['id']}",
            f"Title:       {issue['title']}",
            f"Type:        {issue['type']}",
            f"Priority:    {self._c(self.PRIORITY_COLORS.get(issue['priority'], ''), issue['priority'])}",
            f"Status:      {self._c(self.STATUS_COLORS.get(issue['status'], ''), issue['status'])}",
            f"Assignee:    {issue.get('assignee') or '-'}",
            f"Labels:      {', '.join(issue.get('labels', [])) or '-'}",
            f"Description: {issue.get('description') or '-'}",
            f"Created:     {issue['created_at']}",
            f"Updated:     {issue['updated_at']}",
        ]
        if issue.get("closed_at"):
            lines.append(f"Closed:      {issue['closed_at']}")
        if issue.get("close_reason"):
            lines.append(f"Reason:      {issue['close_reason']}")
        lines.append(f"Seq:         {issue['seq']}")

        if issue.get("blocked_by"):
            lines.append(f"\nBlocked by:  {', '.join(issue['blocked_by'])}")
        if issue.get("blocks"):
            lines.append(f"Blocks:      {', '.join(issue['blocks'])}")

        if issue.get("comments"):
            lines.append("\nComments:")
            for c in issue["comments"]:
                author = c.get("author") or "anonymous"
                lines.append(f"  [{c['created_at']}] {author}: {c['body']}")

        return "\n".join(lines)

    def format_prime(self, data):
        lines = []
        stats = data["stats"]
        lines.append(f"=== tile dashboard ===")
        green, cyan, dim = "\033[32m", "\033[36m", "\033[2m"
        lines.append(f"Total: {stats['total']}  Open: {self._c(green, stats['open'])}  "
                      f"In Progress: {self._c(cyan, stats['in_progress'])}  "
                      f"Closed: {self._c(dim, stats['closed'])}")
        if data["stale_count"]:
            lines.append(f"Stale (30d+): {data['stale_count']}")
        lines.append("")

        if data["in_progress"]:
            lines.append("--- In Progress ---")
            lines.append(self.format_list(data["in_progress"]))
            lines.append("")

        if data["ready"]:
            lines.append("--- Ready (top) ---")
            lines.append(self.format_list(data["ready"], show_impact=True))
            lines.append("")

        bs = data["blocked_summary"]
        if bs["count"]:
            lines.append(f"--- Blocked: {bs['count']} issues ---")
            if bs["top_blockers"]:
                lines.append("Top blockers:")
                for b in bs["top_blockers"]:
                    lines.append(f"  {b['id']} (impact {b.get('impact', '?')}): {b['title']}")
            lines.append("")

        if data["recently_closed"]:
            lines.append("--- Recently Closed ---")
            for i in data["recently_closed"]:
                lines.append(f"  {i['id']}: {i['title']}")
            lines.append("")

        if data["recently_created"]:
            lines.append("--- Recently Created ---")
            for i in data["recently_created"]:
                lines.append(f"  {i['id']}: {i['title']}")

        return "\n".join(lines)

    def format_dep_tree(self, tree, issue_id, indent=0):
        lines = [f"{'  ' * indent}{issue_id}"]
        for child in tree:
            if child.get("visited"):
                lines.append(f"{'  ' * (indent + 1)}{child['id']} (already shown)")
            else:
                lines.extend(
                    self.format_dep_tree(child.get("children", []), child["id"], indent + 1).split("\n")
                )
        return "\n".join(lines)

    def format_doctor(self, checks):
        lines = []
        for c in checks:
            green, red = "\033[32m", "\033[31m"
            icon = self._c(green, "✓") if c["ok"] else self._c(red, "✗")
            line = f"  {icon} {c['check']}"
            if c.get("detail"):
                line += f" — {c['detail']}"
            lines.append(line)
        return "\n".join(lines)

    def format_stats(self, data):
        lines = [
            f"Total:        {data['total']}",
            f"Open:         {data['open']}",
            f"In Progress:  {data['in_progress']}",
            f"Closed:       {data['closed']}",
            f"Dependencies: {data['dependencies']}",
            f"Comments:     {data['comments']}",
        ]
        if data.get("by_priority"):
            lines.append("\nBy priority:")
            for p, c in sorted(data["by_priority"].items()):
                lines.append(f"  {p}: {c}")
        if data.get("by_type"):
            lines.append("\nBy type:")
            for t, c in sorted(data["by_type"].items()):
                lines.append(f"  {t}: {c}")
        return "\n".join(lines)

    def format_stats_by(self, data):
        lines = []
        for k, v in sorted(data.items(), key=lambda x: -x[1]):
            lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    def format_sync_status(self, data):
        lines = [
            f"DB issues:     {data['db_count']}",
            f"JSONL issues:  {data['jsonl_count']}",
        ]
        if data["db_only"]:
            lines.append(f"DB only:       {', '.join(data['db_only'])}")
        if data["jsonl_only"]:
            lines.append(f"JSONL only:    {', '.join(data['jsonl_only'])}")
        if data["modified_since_push"]:
            lines.append(f"Modified:      {', '.join(data['modified_since_push'])}")
        if data["seq_differs"]:
            lines.append(f"Seq differs:   {', '.join(data['seq_differs'])}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _output(data, json_mode, human_fn=None):
    if json_mode:
        print(json.dumps(data, ensure_ascii=False, default=str))
    elif human_fn:
        print(human_fn())
    else:
        print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


def _error(msg, json_mode):
    if json_mode:
        print(json.dumps({"error": msg}), file=sys.stderr)
    else:
        print(f"Error: {msg}", file=sys.stderr)
    return 1


def _is_json_mode(args):
    if getattr(args, "json", False):
        return True
    if getattr(args, "human", False):
        return False
    return not sys.stdout.isatty()


def _use_color(args):
    if getattr(args, "no_color", False):
        return False
    if os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


def _add_global_flags(parser):
    parser.add_argument("--json", action="store_true", help="Force JSON output")
    parser.add_argument("--human", action="store_true", help="Force human output")
    parser.add_argument("--quiet", "-q", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--db", help="Override workspace discovery")


def build_parser():
    p = argparse.ArgumentParser(prog="tile", description="Local issue tracker for AI coding agents")
    _add_global_flags(p)

    sub = p.add_subparsers(dest="command")

    # init
    sp = sub.add_parser("init")
    sp.add_argument("--force", action="store_true")

    # create
    sp = sub.add_parser("create")
    sp.add_argument("title")
    sp.add_argument("--type", dest="type_", default="task")
    sp.add_argument("--priority", "-p", type=int, default=2)
    sp.add_argument("--description", default="")
    sp.add_argument("--assignee")
    sp.add_argument("--labels", default="")

    # show
    sp = sub.add_parser("show")
    sp.add_argument("issue_id")

    # update
    sp = sub.add_parser("update")
    sp.add_argument("issue_id")
    sp.add_argument("--title")
    sp.add_argument("--type", dest="type_")
    sp.add_argument("--priority", type=int, default=None)
    sp.add_argument("--status")
    sp.add_argument("--description")
    sp.add_argument("--assignee")
    sp.add_argument("--reason")

    # delete
    sp = sub.add_parser("delete")
    sp.add_argument("issue_id")

    # list
    sp = sub.add_parser("list")
    sp.add_argument("--status")
    sp.add_argument("--priority")
    sp.add_argument("--type", dest="type_")
    sp.add_argument("--assignee")
    sp.add_argument("--label")
    sp.add_argument("--search")
    sp.add_argument("--ready", action="store_true")
    sp.add_argument("--blocked", action="store_true")
    sp.add_argument("--stale", nargs="?", const=30, type=int)
    sp.add_argument("--sort")
    sp.add_argument("--reverse", action="store_true")

    # prime
    sp = sub.add_parser("prime")
    sp.add_argument("--limit", type=int, default=10)
    sp.add_argument("--hours", type=int, default=24)

    # batch
    sub.add_parser("batch")

    # dep
    sp = sub.add_parser("dep")
    dep_sub = sp.add_subparsers(dest="dep_command")
    da = dep_sub.add_parser("add")
    da.add_argument("child_id")
    da.add_argument("parent_id")
    dr = dep_sub.add_parser("remove")
    dr.add_argument("child_id")
    dr.add_argument("parent_id")
    dl = dep_sub.add_parser("list")
    dl.add_argument("issue_id")
    dt = dep_sub.add_parser("tree")
    dt.add_argument("issue_id")

    # label
    sp = sub.add_parser("label")
    label_sub = sp.add_subparsers(dest="label_command")
    la = label_sub.add_parser("add")
    la.add_argument("issue_id")
    la.add_argument("labels", nargs="+")
    lr = label_sub.add_parser("remove")
    lr.add_argument("issue_id")
    lr.add_argument("label")
    ll = label_sub.add_parser("list")
    ll.add_argument("issue_id", nargs="?")
    ll.add_argument("--all", action="store_true", dest="all_labels")

    # comment
    sp = sub.add_parser("comment")
    comment_sub = sp.add_subparsers(dest="comment_command")
    ca = comment_sub.add_parser("add")
    ca.add_argument("issue_id")
    ca.add_argument("body")
    ca.add_argument("--author")
    cl = comment_sub.add_parser("list")
    cl.add_argument("issue_id")

    # sync
    sp = sub.add_parser("sync")
    sync_sub = sp.add_subparsers(dest="sync_command")
    sync_sub.add_parser("push")
    sp_pull = sync_sub.add_parser("pull")
    sp_pull.add_argument("--prefer-remote", action="store_true")
    sync_sub.add_parser("status")

    # stats
    sp = sub.add_parser("stats")
    sp.add_argument("--by")

    # doctor
    sub.add_parser("doctor")

    # version
    sub.add_parser("version")

    return p


_GLOBAL_FLAGS = {"--json", "--human", "--quiet", "-q", "--verbose", "-v", "--no-color"}
_GLOBAL_FLAGS_WITH_VALUE = {"--db"}


def _preprocess_argv(argv):
    """Move global flags to the front so argparse parent parser sees them."""
    if argv is None:
        return None
    front = []
    rest = []
    i = 0
    while i < len(argv):
        if argv[i] in _GLOBAL_FLAGS:
            front.append(argv[i])
        elif argv[i] in _GLOBAL_FLAGS_WITH_VALUE:
            front.append(argv[i])
            if i + 1 < len(argv):
                front.append(argv[i + 1])
                i += 1
        else:
            rest.append(argv[i])
        i += 1
    return front + rest


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(_preprocess_argv(argv))

    json_mode = _is_json_mode(args)

    # No subcommand = prime
    if not args.command:
        args.command = "prime"

    # init doesn't need a workspace
    if args.command == "init":
        try:
            Tile.init(force=getattr(args, "force", False))
            if not getattr(args, "quiet", False):
                if json_mode:
                    print(json.dumps({"ok": True}))
                else:
                    print("Initialized .tile/ workspace")
            return 0
        except TileError as e:
            return _error(str(e), json_mode)

    # version doesn't need a workspace
    if args.command == "version":
        info = f"tile {__version__} (Python {sys.version.split()[0]})"
        if json_mode:
            print(json.dumps({"version": __version__, "python": sys.version.split()[0]}))
        else:
            print(info)
        return 0

    # Find workspace
    db_path = args.db or _find_workspace()
    if not db_path:
        return _error("No workspace found. Run 'tile init' first.", json_mode)

    try:
        tile = Tile(db_path)
    except Exception as e:
        return _error(f"Cannot open database: {e}", json_mode)

    fmt = Formatter(color=_use_color(args))

    try:
        return _dispatch(args, tile, json_mode, fmt)
    except TileError as e:
        return _error(str(e), json_mode)
    finally:
        tile.close()


def _dispatch(args, tile, json_mode, fmt):
    cmd = args.command

    if cmd == "create":
        labels = [l.strip() for l in args.labels.split(",") if l.strip()] if args.labels else []
        result = tile.create(
            title=args.title, type_=args.type_, priority=args.priority,
            description=args.description, assignee=args.assignee, labels=labels,
        )
        if getattr(args, "quiet", False):
            print(result["id"])
        elif json_mode:
            print(json.dumps(result, ensure_ascii=False))
        else:
            print(result["id"])
        return 0

    elif cmd == "show":
        result = tile.show(args.issue_id)
        _output(result, json_mode, lambda: fmt.format_show(result))
        return 0

    elif cmd == "update":
        result = tile.update(
            args.issue_id,
            title=args.title, type_=args.type_, priority=args.priority,
            status=args.status, description=args.description,
            assignee=args.assignee, reason=args.reason,
        )
        if json_mode:
            print(json.dumps(result, ensure_ascii=False))
        elif not getattr(args, "quiet", False):
            print(f"Updated {result['id']}")
        return 0

    elif cmd == "delete":
        tile.delete(args.issue_id)
        if json_mode:
            print(json.dumps({"ok": True}))
        elif not getattr(args, "quiet", False):
            print(f"Deleted {args.issue_id}")
        return 0

    elif cmd == "list":
        result = tile.list_issues(
            status=args.status, priority=args.priority, type_=args.type_,
            assignee=args.assignee, label=args.label, search=args.search,
            ready=args.ready, blocked=args.blocked, stale=args.stale,
            sort=args.sort, reverse=args.reverse,
        )
        _output(result, json_mode, lambda: fmt.format_list(result, show_impact=args.ready))
        return 0

    elif cmd == "prime":
        result = tile.prime(
            limit=getattr(args, "limit", 10),
            hours=getattr(args, "hours", 24),
        )
        _output(result, json_mode, lambda: fmt.format_prime(result))
        return 0

    elif cmd == "batch":
        data = sys.stdin.read()
        operations = json.loads(data)
        try:
            result = tile.batch(operations)
            print(json.dumps(result, ensure_ascii=False))
            return 0
        except TileError as e:
            # Batch errors are pre-formatted as JSON with failed_at
            msg = str(e)
            try:
                err_obj = json.loads(msg)
                print(json.dumps(err_obj), file=sys.stderr)
            except json.JSONDecodeError:
                print(json.dumps({"error": msg}), file=sys.stderr)
            return 1

    elif cmd == "dep":
        dc = args.dep_command
        if dc == "add":
            tile.dep_add(args.child_id, args.parent_id)
            if json_mode:
                print(json.dumps({"ok": True}))
            elif not getattr(args, "quiet", False):
                print("Dependency added")
            return 0
        elif dc == "remove":
            tile.dep_remove(args.child_id, args.parent_id)
            if json_mode:
                print(json.dumps({"ok": True}))
            elif not getattr(args, "quiet", False):
                print("Dependency removed")
            return 0
        elif dc == "list":
            result = tile.dep_list(args.issue_id)
            _output(result, json_mode, lambda: (
                f"Blocked by: {', '.join(result['blocked_by']) or '-'}\n"
                f"Blocks:     {', '.join(result['blocks']) or '-'}"
            ))
            return 0
        elif dc == "tree":
            tree = tile.dep_tree(args.issue_id)
            if json_mode:
                print(json.dumps(tree, ensure_ascii=False))
            else:
                print(fmt.format_dep_tree(tree, args.issue_id))
            return 0

    elif cmd == "label":
        lc = args.label_command
        if lc == "add":
            tile.label_add(args.issue_id, args.labels)
            if json_mode:
                print(json.dumps({"ok": True}))
            elif not getattr(args, "quiet", False):
                print("Labels added")
            return 0
        elif lc == "remove":
            tile.label_remove(args.issue_id, args.label)
            if json_mode:
                print(json.dumps({"ok": True}))
            elif not getattr(args, "quiet", False):
                print("Label removed")
            return 0
        elif lc == "list":
            result = tile.label_list(
                issue_id_prefix=getattr(args, "issue_id", None),
                all_labels=getattr(args, "all_labels", False),
            )
            _output(result, json_mode, lambda: "\n".join(result) if result else "No labels")
            return 0

    elif cmd == "comment":
        cc = args.comment_command
        if cc == "add":
            result = tile.comment_add(args.issue_id, args.body, author=args.author)
            if json_mode:
                print(json.dumps(result, ensure_ascii=False))
            elif not getattr(args, "quiet", False):
                print(result["id"])
            return 0
        elif cc == "list":
            result = tile.comment_list(args.issue_id)
            _output(result, json_mode, lambda: "\n".join(
                f"[{c['created_at']}] {c.get('author') or 'anonymous'}: {c['body']}"
                for c in result
            ) if result else "No comments")
            return 0

    elif cmd == "sync":
        sc = args.sync_command
        if sc == "push":
            tile.sync_push()
            if json_mode:
                print(json.dumps({"ok": True}))
            elif not getattr(args, "quiet", False):
                print("Pushed to issues.jsonl")
            return 0
        elif sc == "pull":
            tile.sync_pull(prefer_remote=getattr(args, "prefer_remote", False))
            if json_mode:
                print(json.dumps({"ok": True}))
            elif not getattr(args, "quiet", False):
                print("Pulled from issues.jsonl")
            return 0
        elif sc == "status":
            result = tile.sync_status()
            _output(result, json_mode, lambda: fmt.format_sync_status(result))
            return 0

    elif cmd == "stats":
        by = getattr(args, "by", None)
        result = tile.stats(by=by)
        if by:
            _output(result, json_mode, lambda: fmt.format_stats_by(result))
        else:
            _output(result, json_mode, lambda: fmt.format_stats(result))
        return 0

    elif cmd == "doctor":
        checks, all_ok = tile.doctor()
        if json_mode:
            print(json.dumps({"checks": checks, "all_ok": all_ok}))
        else:
            print(fmt.format_doctor(checks))
        return 0 if all_ok else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
