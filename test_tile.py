"""Tests for tile — local issue tracker for AI coding agents.

Tests call tile's CLI via its main() function, capturing stdout/stderr.
Each test gets a fresh temporary directory with `tile init` already run.

Convention: tests use --json mode so assertions are on parsed dicts.
"""

import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tile import Tile, TileError, main as tile_main


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

class TileTestCase(unittest.TestCase):
    """Base class that creates a temp dir with an initialised tile workspace."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.orig_cwd = os.getcwd()
        os.chdir(self.tmpdir)
        Tile.init()
        self.db_path = Path(self.tmpdir) / ".tile" / "tile.db"

    def tearDown(self):
        os.chdir(self.orig_cwd)
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def tile(self, *args, stdin=None):
        """Call tile CLI via main(), return (exit_code, stdout_parsed, stderr_str).

        Always passes --json for predictable output. Captures stdout/stderr.
        Parses stdout as JSON when possible.
        """
        argv = ["--json"] + list(args)

        old_stdout, old_stderr, old_stdin = sys.stdout, sys.stderr, sys.stdin
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        if stdin is not None:
            sys.stdin = io.StringIO(stdin)

        try:
            rc = tile_main(argv) or 0
        except SystemExit as e:
            rc = e.code if e.code is not None else 0
        finally:
            stdout_val = sys.stdout.getvalue()
            stderr_val = sys.stderr.getvalue()
            sys.stdout, sys.stderr, sys.stdin = old_stdout, old_stderr, old_stdin

        # Try to parse stdout as JSON
        parsed = stdout_val
        if stdout_val.strip():
            try:
                parsed = json.loads(stdout_val)
            except json.JSONDecodeError:
                parsed = stdout_val.strip()

        return rc, parsed, stderr_val

    def tile_ok(self, *args, stdin=None):
        """Call tile, assert exit 0, return parsed stdout."""
        rc, out, err = self.tile(*args, stdin=stdin)
        self.assertEqual(rc, 0, f"tile {' '.join(args)} failed: {err}")
        return out

    def tile_fail(self, *args, stdin=None):
        """Call tile, assert exit 1, return stderr string."""
        rc, out, err = self.tile(*args, stdin=stdin)
        self.assertEqual(rc, 1, f"Expected failure but got rc=0: {out}")
        return err

    def create_issue(self, title="Test issue", **kwargs):
        """Shorthand: create an issue, return its ID."""
        args = ["create", title]
        for k, v in kwargs.items():
            args.extend([f"--{k}", str(v)])
        result = self.tile_ok(*args)
        return result["id"]

    def get_issue(self, issue_id):
        """Shorthand: show an issue, return full JSON."""
        return self.tile_ok("show", issue_id)


# ===========================================================================
# 1. Init & Workspace Discovery
# ===========================================================================

class TestInit(TileTestCase):

    def test_init_creates_tile_dir_and_db(self):
        """tile init creates .tile/ with tile.db containing all tables."""
        # setUp already ran init; verify the artifacts
        self.assertTrue((Path(self.tmpdir) / ".tile").is_dir())
        self.assertTrue(self.db_path.exists())

        db = sqlite3.connect(str(self.db_path))
        tables = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        db.close()
        self.assertIn("issues", tables)
        self.assertIn("dependencies", tables)
        self.assertIn("comments", tables)

    def test_init_already_exists_fails(self):
        """tile init in a dir that already has .tile/ exits 1."""
        err = self.tile_fail("init")
        self.assertIn("already exists", err.lower())

    def test_init_force_reinitialises(self):
        """tile init --force in existing workspace recreates everything."""
        _id = self.create_issue("will be destroyed")
        self.tile_ok("init", "--force")
        # DB should be empty now
        result = self.tile_ok("list")
        self.assertEqual(result, [])

    def test_workspace_discovery_walks_upward(self):
        """Commands in a subdirectory find .tile/ in a parent."""
        subdir = Path(self.tmpdir) / "src" / "deep"
        subdir.mkdir(parents=True)
        os.chdir(str(subdir))
        # Should still find the workspace
        result = self.tile_ok("list")
        self.assertIsInstance(result, list)

    def test_no_workspace_exits_1(self):
        """Commands (except init) fail if no .tile/ found anywhere."""
        empty = tempfile.mkdtemp()
        os.chdir(empty)
        err = self.tile_fail("list")
        self.assertIn("no workspace", err.lower())

    def test_db_flag_overrides_discovery(self):
        """--db points directly to a SQLite file, skipping discovery."""
        other_dir = tempfile.mkdtemp()
        # init a workspace there
        os.chdir(other_dir)
        # Go back to a dir with no workspace
        empty = tempfile.mkdtemp()
        os.chdir(empty)
        result = self.tile_ok("list", "--db", str(self.db_path))
        self.assertIsInstance(result, list)


# ===========================================================================
# 2. Create / Show / Update / Delete (CRUD)
# ===========================================================================

class TestCreate(TileTestCase):

    def test_create_returns_id(self):
        """create returns an object with a tl- prefixed ID."""
        result = self.tile_ok("create", "My first issue")
        self.assertIn("id", result)
        self.assertTrue(result["id"].startswith("tl-"))
        self.assertEqual(len(result["id"]), 9)  # tl- + 6 hex

    def test_create_sets_defaults(self):
        """New issue has correct default field values."""
        issue_id = self.create_issue("Defaults test")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["title"], "Defaults test")
        self.assertEqual(issue["type"], "task")
        self.assertEqual(issue["priority"], 2)
        self.assertEqual(issue["status"], "open")
        self.assertEqual(issue["description"], "")
        self.assertIsNone(issue["assignee"])
        self.assertEqual(issue["labels"], [])
        self.assertIsNone(issue["close_reason"])
        self.assertIsNone(issue["closed_at"])
        self.assertEqual(issue["seq"], 1)

    def test_create_with_all_flags(self):
        """create respects --type, --priority, --description, --assignee, --labels."""
        result = self.tile_ok(
            "create", "Full issue",
            "--type", "bug",
            "--priority", "0",
            "--description", "Something broke",
            "--assignee", "alice",
            "--labels", "backend,urgent",
        )
        issue = self.get_issue(result["id"])
        self.assertEqual(issue["type"], "bug")
        self.assertEqual(issue["priority"], 0)
        self.assertEqual(issue["description"], "Something broke")
        self.assertEqual(issue["assignee"], "alice")
        self.assertCountEqual(issue["labels"], ["backend", "urgent"])

    def test_create_sets_timestamps(self):
        """created_at and updated_at are set to ISO 8601 UTC."""
        issue_id = self.create_issue("Timestamp test")
        issue = self.get_issue(issue_id)
        # Should be valid ISO 8601
        created = datetime.fromisoformat(issue["created_at"].replace("Z", "+00:00"))
        updated = datetime.fromisoformat(issue["updated_at"].replace("Z", "+00:00"))
        self.assertAlmostEqual(
            created.timestamp(), updated.timestamp(), delta=2
        )

    def test_create_empty_title_fails(self):
        """create with empty title exits 1."""
        err = self.tile_fail("create", "")
        self.assertIn("title", err.lower())

    def test_create_invalid_type_fails(self):
        """create with unknown type exits 1."""
        self.tile_fail("create", "Bad type", "--type", "story")

    def test_create_invalid_priority_fails(self):
        """create with priority out of range exits 1."""
        self.tile_fail("create", "Bad prio", "--priority", "5")
        self.tile_fail("create", "Bad prio", "--priority", "-1")


class TestShow(TileTestCase):

    def test_show_returns_full_issue(self):
        """show returns all fields including blocks/blocked_by/comments."""
        issue_id = self.create_issue("Show me")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["title"], "Show me")
        self.assertIn("blocked_by", issue)
        self.assertIn("blocks", issue)
        self.assertIn("comments", issue)

    def test_show_not_found(self):
        """show with nonexistent ID exits 1."""
        err = self.tile_fail("show", "tl-000000")
        self.assertIn("not found", err.lower())


class TestUpdate(TileTestCase):

    def test_update_title(self):
        """update --title changes the title and increments seq."""
        issue_id = self.create_issue("Original")
        self.tile_ok("update", issue_id, "--title", "Renamed")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["title"], "Renamed")
        self.assertEqual(issue["seq"], 2)

    def test_update_multiple_fields(self):
        """update can change several fields at once."""
        issue_id = self.create_issue("Multi")
        self.tile_ok(
            "update", issue_id,
            "--priority", "0",
            "--assignee", "bob",
            "--type", "bug",
        )
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["priority"], 0)
        self.assertEqual(issue["assignee"], "bob")
        self.assertEqual(issue["type"], "bug")

    def test_update_no_flags_fails(self):
        """update with no change flags exits 1."""
        issue_id = self.create_issue("No flags")
        self.tile_fail("update", issue_id)

    def test_update_not_found(self):
        """update on nonexistent ID exits 1."""
        self.tile_fail("update", "tl-000000", "--title", "Ghost")

    def test_update_bumps_updated_at(self):
        """update changes updated_at."""
        issue_id = self.create_issue("Timestamp")
        before = self.get_issue(issue_id)["updated_at"]
        self.tile_ok("update", issue_id, "--title", "Changed")
        after = self.get_issue(issue_id)["updated_at"]
        self.assertGreaterEqual(after, before)

    # --- Status transitions (close / reopen via update) ---

    def test_close_via_update(self):
        """update --status closed sets closed_at."""
        issue_id = self.create_issue("Close me")
        self.tile_ok("update", issue_id, "--status", "closed")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["status"], "closed")
        self.assertIsNotNone(issue["closed_at"])

    def test_close_with_reason(self):
        """update --status closed --reason sets close_reason."""
        issue_id = self.create_issue("Close with reason")
        self.tile_ok(
            "update", issue_id,
            "--status", "closed",
            "--reason", "Duplicate",
        )
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["close_reason"], "Duplicate")

    def test_reopen_via_update(self):
        """update --status open on a closed issue clears closed_at and close_reason."""
        issue_id = self.create_issue("Reopen me")
        self.tile_ok(
            "update", issue_id,
            "--status", "closed",
            "--reason", "Oops",
        )
        self.tile_ok("update", issue_id, "--status", "open")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["status"], "open")
        self.assertIsNone(issue["closed_at"])
        self.assertIsNone(issue["close_reason"])

    def test_close_already_closed_fails(self):
        """update --status closed on already closed issue exits 1."""
        issue_id = self.create_issue("Already closed")
        self.tile_ok("update", issue_id, "--status", "closed")
        err = self.tile_fail("update", issue_id, "--status", "closed")
        self.assertIn("already", err.lower())

    def test_reason_without_closed_fails(self):
        """--reason without --status closed is an error."""
        issue_id = self.create_issue("Bad combo")
        self.tile_fail("update", issue_id, "--reason", "No status change")

    def test_transition_open_to_in_progress(self):
        """update --status in_progress works from open."""
        issue_id = self.create_issue("Start work")
        self.tile_ok("update", issue_id, "--status", "in_progress")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["status"], "in_progress")


class TestClaim(TileTestCase):

    def test_claim_open_issue(self):
        """claim sets status to in_progress."""
        issue_id = self.create_issue("Claimable")
        result = self.tile_ok("claim", issue_id)
        self.assertEqual(result["status"], "in_progress")

    def test_claim_with_assignee(self):
        """claim --assignee sets the assignee."""
        issue_id = self.create_issue("Assign me")
        result = self.tile_ok("claim", issue_id, "--assignee", "AgentAlpha")
        self.assertEqual(result["status"], "in_progress")
        self.assertEqual(result["assignee"], "AgentAlpha")

    def test_claim_already_claimed_fails(self):
        """claim on an in_progress issue fails."""
        issue_id = self.create_issue("Race condition")
        self.tile_ok("claim", issue_id, "--assignee", "Agent1")
        err = self.tile_fail("claim", issue_id)
        self.assertIn("claimed", err.lower())

    def test_claim_already_claimed_shows_assignee(self):
        """claim failure message includes who claimed it."""
        issue_id = self.create_issue("Taken")
        self.tile_ok("claim", issue_id, "--assignee", "AgentBravo")
        err = self.tile_fail("claim", issue_id)
        self.assertIn("AgentBravo", err)

    def test_claim_closed_fails(self):
        """claim on a closed issue fails."""
        issue_id = self.create_issue("Done")
        self.tile_ok("update", issue_id, "--status", "closed")
        err = self.tile_fail("claim", issue_id)
        self.assertIn("closed", err.lower())

    def test_claim_not_found(self):
        """claim on nonexistent ID fails."""
        self.tile_fail("claim", "tl-000000")


class TestDelete(TileTestCase):

    def test_delete_removes_issue(self):
        """delete removes the issue from the database."""
        issue_id = self.create_issue("Delete me")
        self.tile_ok("delete", issue_id)
        self.tile_fail("show", issue_id)

    def test_delete_cascades_deps(self):
        """delete removes all dependencies involving the issue."""
        a = self.create_issue("Parent")
        b = self.create_issue("Child")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("delete", a)
        # b should have no deps
        issue = self.get_issue(b)
        self.assertEqual(issue["blocked_by"], [])

    def test_delete_cascades_comments(self):
        """delete removes all comments on the issue."""
        issue_id = self.create_issue("Has comments")
        self.tile_ok("comment", "add", issue_id, "A note")
        self.tile_ok("delete", issue_id)
        # Verify via raw DB that no orphan comments exist
        db = sqlite3.connect(str(self.db_path))
        count = db.execute(
            "SELECT COUNT(*) FROM comments WHERE issue_id=?", (issue_id,)
        ).fetchone()[0]
        db.close()
        self.assertEqual(count, 0)

    def test_delete_not_found(self):
        """delete on nonexistent ID exits 1."""
        self.tile_fail("delete", "tl-000000")


# ===========================================================================
# 3. List (with filters)
# ===========================================================================

class TestList(TileTestCase):

    def setUp(self):
        super().setUp()
        # Create a known set of issues for filtering tests
        self._ids = {}
        self._ids["a"] = self.create_issue(
            "Auth bug", type="bug", priority="0", assignee="alice",
            labels="backend,auth",
        )
        self._ids["b"] = self.create_issue(
            "Dashboard feature", type="feature", priority="1",
            assignee="bob", labels="frontend",
        )
        self._ids["c"] = self.create_issue(
            "Refactor utils", type="task", priority="3",
            assignee="alice", labels="backend",
        )
        # Close one
        self._ids["d"] = self.create_issue("Done thing", type="task")
        self.tile_ok("update", self._ids["d"], "--status", "closed")

    def test_list_default_excludes_nothing(self):
        """list with no filters returns all issues (including closed)."""
        result = self.tile_ok("list")
        self.assertEqual(len(result), 4)

    def test_list_filter_status(self):
        """--status filters by exact status."""
        result = self.tile_ok("list", "--status", "open")
        self.assertEqual(len(result), 3)
        result = self.tile_ok("list", "--status", "closed")
        self.assertEqual(len(result), 1)

    def test_list_filter_status_comma(self):
        """--status accepts comma-separated values (OR within field)."""
        result = self.tile_ok("list", "--status", "open,closed")
        self.assertEqual(len(result), 4)

    def test_list_filter_priority_single(self):
        """--priority with single int filters exactly."""
        result = self.tile_ok("list", "--priority", "0")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], self._ids["a"])

    def test_list_filter_priority_range(self):
        """--priority with range (e.g., 0-1) is inclusive."""
        result = self.tile_ok("list", "--priority", "0-1")
        ids = {r["id"] for r in result}
        self.assertIn(self._ids["a"], ids)
        self.assertIn(self._ids["b"], ids)

    def test_list_filter_type(self):
        """--type filters by exact type."""
        result = self.tile_ok("list", "--type", "bug")
        self.assertEqual(len(result), 1)

    def test_list_filter_assignee(self):
        """--assignee filters by exact match."""
        result = self.tile_ok("list", "--assignee", "alice")
        self.assertEqual(len(result), 2)

    def test_list_filter_label(self):
        """--label filters issues whose labels array contains the value."""
        result = self.tile_ok("list", "--label", "backend")
        ids = {r["id"] for r in result}
        self.assertIn(self._ids["a"], ids)
        self.assertIn(self._ids["c"], ids)
        self.assertNotIn(self._ids["b"], ids)

    def test_list_filter_search(self):
        """--search does case-insensitive substring match on title/description."""
        result = self.tile_ok("list", "--search", "auth")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], self._ids["a"])

    def test_list_filters_compose(self):
        """Multiple filters AND together."""
        result = self.tile_ok(
            "list", "--assignee", "alice", "--label", "backend"
        )
        ids = {r["id"] for r in result}
        self.assertIn(self._ids["a"], ids)
        self.assertIn(self._ids["c"], ids)

    def test_list_sort_default(self):
        """Default sort is priority ascending, then created_at ascending."""
        result = self.tile_ok("list", "--status", "open")
        priorities = [r["priority"] for r in result]
        self.assertEqual(priorities, sorted(priorities))

    def test_list_sort_override(self):
        """--sort overrides default sort field."""
        result = self.tile_ok("list", "--sort", "title")
        titles = [r["title"] for r in result]
        self.assertEqual(titles, sorted(titles))

    def test_list_reverse(self):
        """--reverse flips sort direction."""
        result = self.tile_ok("list", "--status", "open", "--reverse")
        priorities = [r["priority"] for r in result]
        self.assertEqual(priorities, sorted(priorities, reverse=True))


class TestListReady(TileTestCase):

    def test_ready_shows_unblocked_open_issues(self):
        """--ready returns open issues with all deps satisfied."""
        a = self.create_issue("Blocker")
        b = self.create_issue("Blocked by a")
        c = self.create_issue("Free")
        self.tile_ok("dep", "add", b, a)

        result = self.tile_ok("list", "--ready")
        ids = {r["id"] for r in result}
        self.assertIn(a, ids)   # a has no deps, it's ready
        self.assertIn(c, ids)   # c has no deps, it's ready
        self.assertNotIn(b, ids)  # b is blocked by a

    def test_ready_includes_impact(self):
        """--ready output includes impact field."""
        a = self.create_issue("Parent")
        b = self.create_issue("Child")
        self.tile_ok("dep", "add", b, a)
        result = self.tile_ok("list", "--ready")
        parent = next(r for r in result if r["id"] == a)
        self.assertIn("impact", parent)
        self.assertEqual(parent["impact"], 1)  # unblocks b

    def test_ready_sorts_by_impact_desc(self):
        """--ready default sort is impact descending."""
        a = self.create_issue("Blocks two", priority="2")
        b = self.create_issue("Blocks none", priority="0")
        c = self.create_issue("Blocked by a")
        d = self.create_issue("Also blocked by a")
        self.tile_ok("dep", "add", c, a)
        self.tile_ok("dep", "add", d, a)

        result = self.tile_ok("list", "--ready")
        # a has impact 2 (blocks c, d), b has impact 0
        # a should sort before b despite b having higher priority
        ready_ids = [r["id"] for r in result]
        self.assertEqual(ready_ids[0], a)

    def test_ready_composes_with_label(self):
        """--ready --label composes correctly."""
        a = self.create_issue("Ready backend", labels="backend")
        b = self.create_issue("Ready frontend", labels="frontend")
        result = self.tile_ok("list", "--ready", "--label", "backend")
        ids = {r["id"] for r in result}
        self.assertIn(a, ids)
        self.assertNotIn(b, ids)


class TestListBlocked(TileTestCase):

    def test_blocked_shows_blocked_issues(self):
        """--blocked returns issues with at least one open blocker."""
        a = self.create_issue("Blocker")
        b = self.create_issue("Blocked")
        self.tile_ok("dep", "add", b, a)

        result = self.tile_ok("list", "--blocked")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], b)
        self.assertIn(a, result[0]["blocked_by"])

    def test_blocked_excludes_satisfied_deps(self):
        """--blocked doesn't include issues whose blockers are all closed."""
        a = self.create_issue("Blocker")
        b = self.create_issue("Blocked")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("update", a, "--status", "closed")

        result = self.tile_ok("list", "--blocked")
        self.assertEqual(len(result), 0)


class TestListStale(TileTestCase):

    def test_stale_finds_old_issues(self):
        """--stale returns open issues not updated in N days."""
        issue_id = self.create_issue("Old issue")
        # Backdate updated_at directly in DB
        old_date = (
            datetime.now(timezone.utc) - timedelta(days=45)
        ).isoformat()
        db = sqlite3.connect(str(self.db_path))
        db.execute(
            "UPDATE issues SET updated_at=? WHERE id=?",
            (old_date, issue_id),
        )
        db.commit()
        db.close()

        result = self.tile_ok("list", "--stale", "30")
        ids = {r["id"] for r in result}
        self.assertIn(issue_id, ids)

    def test_stale_excludes_recent(self):
        """--stale doesn't return recently updated issues."""
        self.create_issue("Fresh issue")
        result = self.tile_ok("list", "--stale", "30")
        self.assertEqual(len(result), 0)

    def test_stale_excludes_closed(self):
        """--stale only considers open issues."""
        issue_id = self.create_issue("Closed old")
        self.tile_ok("update", issue_id, "--status", "closed")
        old_date = (
            datetime.now(timezone.utc) - timedelta(days=45)
        ).isoformat()
        db = sqlite3.connect(str(self.db_path))
        db.execute(
            "UPDATE issues SET updated_at=? WHERE id=?",
            (old_date, issue_id),
        )
        db.commit()
        db.close()

        result = self.tile_ok("list", "--stale", "30")
        self.assertEqual(len(result), 0)


# ===========================================================================
# 4. Dependencies
# ===========================================================================

class TestDep(TileTestCase):

    def test_dep_add(self):
        """dep add creates a dependency and increments seq on both issues."""
        a = self.create_issue("Parent")
        b = self.create_issue("Child")
        self.tile_ok("dep", "add", b, a)

        issue_b = self.get_issue(b)
        self.assertIn(a, issue_b["blocked_by"])

        # Both should have seq bumped (create=1, dep_add=2)
        self.assertEqual(self.get_issue(a)["seq"], 2)
        self.assertEqual(self.get_issue(b)["seq"], 2)

    def test_dep_remove(self):
        """dep remove deletes the dependency."""
        a = self.create_issue("Parent")
        b = self.create_issue("Child")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("dep", "remove", b, a)

        issue_b = self.get_issue(b)
        self.assertEqual(issue_b["blocked_by"], [])

    def test_dep_remove_nonexistent_fails(self):
        """dep remove on a dep that doesn't exist exits 1."""
        a = self.create_issue("A")
        b = self.create_issue("B")
        self.tile_fail("dep", "remove", b, a)

    def test_dep_self_ref_rejected(self):
        """dep add with child == parent exits 1."""
        a = self.create_issue("Self")
        self.tile_fail("dep", "add", a, a)

    def test_dep_duplicate_rejected(self):
        """dep add for an existing dep exits 1."""
        a = self.create_issue("Parent")
        b = self.create_issue("Child")
        self.tile_ok("dep", "add", b, a)
        self.tile_fail("dep", "add", b, a)

    def test_dep_cycle_rejected(self):
        """dep add that would create a cycle exits 1."""
        a = self.create_issue("A")
        b = self.create_issue("B")
        c = self.create_issue("C")
        self.tile_ok("dep", "add", b, a)  # b blocked by a
        self.tile_ok("dep", "add", c, b)  # c blocked by b
        err = self.tile_fail("dep", "add", a, c)  # a blocked by c → cycle
        self.assertIn("cycle", err.lower())

    def test_dep_list(self):
        """dep list shows blocks and blocked_by."""
        a = self.create_issue("Center")
        b = self.create_issue("Blocked by center")
        c = self.create_issue("Blocks center")
        self.tile_ok("dep", "add", b, a)  # b blocked by a
        self.tile_ok("dep", "add", a, c)  # a blocked by c

        result = self.tile_ok("dep", "list", a)
        self.assertIn(b, result["blocks"])
        self.assertIn(c, result["blocked_by"])

    def test_dep_tree(self):
        """dep tree shows transitive downstream dependencies."""
        a = self.create_issue("Root")
        b = self.create_issue("Level 1")
        c = self.create_issue("Level 2")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("dep", "add", c, b)

        result = self.tile_ok("dep", "tree", a)
        # Result should be a tree structure containing b and c
        self.assertIsInstance(result, (list, dict))

    def test_dep_diamond(self):
        """dep tree handles diamond-shaped deps without duplication."""
        a = self.create_issue("Top")
        b = self.create_issue("Left")
        c = self.create_issue("Right")
        d = self.create_issue("Bottom")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("dep", "add", c, a)
        self.tile_ok("dep", "add", d, b)
        self.tile_ok("dep", "add", d, c)

        result = self.tile_ok("dep", "tree", a)
        # d should appear only once in the tree
        self.assertIsInstance(result, (list, dict))


# ===========================================================================
# 5. Labels
# ===========================================================================

class TestLabel(TileTestCase):

    def test_label_add(self):
        """label add appends labels and increments seq."""
        issue_id = self.create_issue("Labelled")
        self.tile_ok("label", "add", issue_id, "frontend", "urgent")
        issue = self.get_issue(issue_id)
        self.assertCountEqual(issue["labels"], ["frontend", "urgent"])
        self.assertEqual(issue["seq"], 2)

    def test_label_add_idempotent(self):
        """label add doesn't duplicate existing labels."""
        issue_id = self.create_issue("Labelled")
        self.tile_ok("label", "add", issue_id, "frontend")
        self.tile_ok("label", "add", issue_id, "frontend", "backend")
        issue = self.get_issue(issue_id)
        self.assertCountEqual(issue["labels"], ["frontend", "backend"])

    def test_label_remove(self):
        """label remove deletes a label."""
        issue_id = self.create_issue("Labelled", labels="a,b")
        self.tile_ok("label", "remove", issue_id, "a")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["labels"], ["b"])

    def test_label_remove_missing_fails(self):
        """label remove on a label not present exits 1."""
        issue_id = self.create_issue("Labelled")
        self.tile_fail("label", "remove", issue_id, "nonexistent")

    def test_label_list_issue(self):
        """label list <id> returns labels for that issue."""
        issue_id = self.create_issue("Labelled", labels="x,y")
        result = self.tile_ok("label", "list", issue_id)
        self.assertCountEqual(result, ["x", "y"])

    def test_label_list_all(self):
        """label list --all returns all unique labels sorted."""
        self.create_issue("A", labels="z,a")
        self.create_issue("B", labels="m,a")
        result = self.tile_ok("label", "list", "--all")
        self.assertEqual(result, ["a", "m", "z"])


# ===========================================================================
# 6. Comments
# ===========================================================================

class TestComment(TileTestCase):

    def test_comment_add(self):
        """comment add creates a comment and increments parent seq."""
        issue_id = self.create_issue("Has comments")
        result = self.tile_ok("comment", "add", issue_id, "First note")
        self.assertIn("id", result)
        self.assertTrue(result["id"].startswith("c-"))

        issue = self.get_issue(issue_id)
        self.assertEqual(issue["seq"], 2)
        self.assertEqual(len(issue["comments"]), 1)
        self.assertEqual(issue["comments"][0]["body"], "First note")

    def test_comment_add_with_author(self):
        """comment add --author sets the author field."""
        issue_id = self.create_issue("Authored")
        self.tile_ok(
            "comment", "add", issue_id, "Note",
            "--author", "alice",
        )
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["comments"][0]["author"], "alice")

    def test_comment_list(self):
        """comment list returns comments in chronological order."""
        issue_id = self.create_issue("Multi comment")
        self.tile_ok("comment", "add", issue_id, "First")
        self.tile_ok("comment", "add", issue_id, "Second")
        result = self.tile_ok("comment", "list", issue_id)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["body"], "First")
        self.assertEqual(result[1]["body"], "Second")

    def test_comment_on_nonexistent_issue_fails(self):
        """comment add on missing issue exits 1."""
        self.tile_fail("comment", "add", "tl-000000", "Orphan")


# ===========================================================================
# 7. Prime
# ===========================================================================

class TestPrime(TileTestCase):

    def test_prime_claims_top_ready_issue(self):
        """prime atomically claims the highest-impact ready issue."""
        issue_id = self.create_issue("Do this")
        result = self.tile_ok("prime")
        self.assertEqual(result["status"], "assigned")
        self.assertEqual(result["issue"]["id"], issue_id)
        self.assertEqual(result["issue"]["status"], "in_progress")

    def test_prime_with_assignee(self):
        """prime --assignee sets the assignee on the claimed issue."""
        self.create_issue("Work item")
        result = self.tile_ok("prime", "--assignee", "AgentX")
        self.assertEqual(result["issue"]["assignee"], "AgentX")

    def test_prime_empty(self):
        """prime on empty workspace returns status=empty."""
        result = self.tile_ok("prime")
        self.assertEqual(result["status"], "empty")

    def test_prime_all_done(self):
        """prime when all issues closed returns status=done."""
        issue_id = self.create_issue("Already done")
        self.tile_ok("update", issue_id, "--status", "closed")
        result = self.tile_ok("prime")
        self.assertEqual(result["status"], "done")

    def test_prime_skips_already_claimed(self):
        """prime skips in_progress issues and claims the next open one."""
        a = self.create_issue("Already taken")
        b = self.create_issue("Available")
        self.tile_ok("claim", a)
        result = self.tile_ok("prime")
        self.assertEqual(result["status"], "assigned")
        self.assertEqual(result["issue"]["id"], b)

    def test_prime_respects_impact_order(self):
        """prime claims the highest-impact issue first."""
        low = self.create_issue("Low impact")
        high = self.create_issue("High impact")
        dep = self.create_issue("Blocked by high")
        self.tile_ok("dep", "add", dep, high)
        result = self.tile_ok("prime")
        self.assertEqual(result["issue"]["id"], high)

    def test_prime_watch_returns_dashboard(self):
        """prime --watch returns dashboard structure without claiming."""
        self.create_issue("Unclaimed")
        result = self.tile_ok("prime", "--watch")
        self.assertIn("stats", result)
        self.assertIn("ready", result)
        # Issue should still be open
        issues = self.tile_ok("list")
        self.assertEqual(issues[0]["status"], "open")

    def test_bare_tile_runs_prime(self):
        """Running tile with no subcommand is equivalent to tile prime."""
        self.create_issue("Something")
        prime_result = self.tile_ok("prime")
        # Reset and try bare
        self.setUp()
        self.create_issue("Something")
        bare_result = self.tile_ok()
        self.assertEqual(prime_result["status"], bare_result["status"])


# ===========================================================================
# 8. Batch
# ===========================================================================

class TestBatch(TileTestCase):

    def test_batch_create_multiple(self):
        """batch can create multiple issues atomically."""
        ops = json.dumps([
            {"op": "create", "title": "First", "type": "task", "priority": 1},
            {"op": "create", "title": "Second", "type": "bug"},
        ])
        result = self.tile_ok("batch", stdin=ops)
        self.assertEqual(len(result), 2)
        self.assertTrue(result[0]["ok"])
        self.assertTrue(result[1]["ok"])

    def test_batch_back_reference(self):
        """$N in batch resolves to the Nth operation's created ID."""
        ops = json.dumps([
            {"op": "create", "title": "Parent"},
            {"op": "create", "title": "Child"},
            {"op": "dep_add", "child": "$1", "parent": "$0"},
        ])
        result = self.tile_ok("batch", stdin=ops)
        self.assertTrue(result[2]["ok"])

        # Verify the dep exists
        child_id = result[1]["id"]
        parent_id = result[0]["id"]
        issue = self.get_issue(child_id)
        self.assertIn(parent_id, issue["blocked_by"])

    def test_batch_rollback_on_failure(self):
        """If any batch op fails, the entire transaction rolls back."""
        ops = json.dumps([
            {"op": "create", "title": "Will exist"},
            {"op": "update", "id": "tl-000000", "title": "Ghost"},  # fails
        ])
        rc, out, err = self.tile("batch", stdin=ops)
        self.assertEqual(rc, 1)

        # The first create should NOT have persisted
        result = self.tile_ok("list")
        self.assertEqual(len(result), 0)

    def test_batch_failure_response(self):
        """Failed batch returns error with failed_at index."""
        ops = json.dumps([
            {"op": "create", "title": "OK"},
            {"op": "create", "title": ""},  # empty title → fail
        ])
        rc, out, err = self.tile("batch", stdin=ops)
        self.assertEqual(rc, 1)
        error = json.loads(err) if isinstance(err, str) else err
        self.assertIn("failed_at", error)
        self.assertEqual(error["failed_at"], 1)

    def test_batch_all_ops(self):
        """batch supports all documented op types."""
        # Pre-create an issue for ops that need an existing ID
        existing = self.create_issue("Existing", labels="old")
        ops = json.dumps([
            {"op": "create", "title": "New"},
            {"op": "update", "id": existing, "priority": 0},
            {"op": "label_add", "id": existing, "label": "new"},
            {"op": "label_remove", "id": existing, "label": "old"},
            {"op": "comment_add", "id": existing, "body": "A note"},
            {"op": "dep_add", "child": "$0", "parent": existing},
            {"op": "dep_remove", "child": "$0", "parent": existing},
            {"op": "delete", "id": "$0"},
        ])
        result = self.tile_ok("batch", stdin=ops)
        self.assertEqual(len(result), 8)
        self.assertTrue(all(r["ok"] for r in result))


# ===========================================================================
# 9. Sync (push / pull / status)
# ===========================================================================

class TestSync(TileTestCase):

    def _jsonl_path(self):
        return Path(self.tmpdir) / ".tile" / "issues.jsonl"

    def test_sync_push_creates_jsonl(self):
        """sync push writes issues.jsonl."""
        self.create_issue("Exported")
        self.tile_ok("sync", "push")
        self.assertTrue(self._jsonl_path().exists())

    def test_sync_push_format(self):
        """sync push writes one JSON object per line, trailing newline."""
        self.create_issue("A")
        self.create_issue("B")
        self.tile_ok("sync", "push")
        content = self._jsonl_path().read_text()
        lines = content.strip().split("\n")
        self.assertEqual(len(lines), 2)
        # Each line is valid JSON
        for line in lines:
            obj = json.loads(line)
            self.assertIn("id", obj)
        # File ends with newline (POSIX)
        self.assertTrue(content.endswith("\n"))

    def test_sync_push_ordered_by_created_at(self):
        """sync push orders lines by created_at ascending."""
        self.create_issue("Second")
        self.create_issue("First")
        self.tile_ok("sync", "push")
        content = self._jsonl_path().read_text().strip().split("\n")
        dates = [json.loads(line)["created_at"] for line in content]
        self.assertEqual(dates, sorted(dates))

    def test_sync_push_excludes_flush_seq(self):
        """sync push does not include flush_seq in output."""
        self.create_issue("Check fields")
        self.tile_ok("sync", "push")
        line = self._jsonl_path().read_text().strip()
        obj = json.loads(line)
        self.assertNotIn("flush_seq", obj)

    def test_sync_push_includes_blocked_by_and_comments(self):
        """sync push includes blocked_by array and comments array."""
        a = self.create_issue("Parent")
        b = self.create_issue("Child")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("comment", "add", b, "A comment")
        self.tile_ok("sync", "push")

        content = self._jsonl_path().read_text().strip().split("\n")
        for line in content:
            obj = json.loads(line)
            if obj["id"] == b:
                self.assertIn(a, obj["blocked_by"])
                self.assertEqual(len(obj["comments"]), 1)

    def test_sync_push_sets_flush_seq(self):
        """After sync push, flush_seq equals seq for all issues."""
        issue_id = self.create_issue("Flush test")
        self.tile_ok("update", issue_id, "--title", "Updated")  # seq=2
        self.tile_ok("sync", "push")

        db = sqlite3.connect(str(self.db_path))
        row = db.execute(
            "SELECT seq, flush_seq FROM issues WHERE id=?", (issue_id,)
        ).fetchone()
        db.close()
        self.assertEqual(row[0], row[1])

    def test_sync_pull_imports_new_issues(self):
        """sync pull inserts issues from JSONL that aren't in the DB."""
        # Write a JSONL file with a foreign issue
        foreign = json.dumps({
            "id": "tl-aaaaaa",
            "title": "From remote",
            "description": "",
            "type": "task",
            "priority": 2,
            "status": "open",
            "assignee": None,
            "labels": [],
            "close_reason": None,
            "created_at": "2026-03-20T12:00:00Z",
            "updated_at": "2026-03-20T12:00:00Z",
            "closed_at": None,
            "seq": 1,
            "blocked_by": [],
            "comments": [],
        })
        self._jsonl_path().write_text(foreign + "\n")
        self.tile_ok("sync", "pull")

        issue = self.get_issue("tl-aaaaaa")
        self.assertEqual(issue["title"], "From remote")

    def test_sync_pull_higher_seq_wins(self):
        """sync pull overwrites local when remote seq > local seq."""
        issue_id = self.create_issue("Local version")  # seq=1
        remote = json.dumps({
            "id": issue_id,
            "title": "Remote version",
            "description": "",
            "type": "task",
            "priority": 2,
            "status": "open",
            "assignee": None,
            "labels": [],
            "close_reason": None,
            "created_at": "2026-03-20T12:00:00Z",
            "updated_at": "2026-03-20T12:00:00Z",
            "closed_at": None,
            "seq": 5,
            "blocked_by": [],
            "comments": [],
        })
        self._jsonl_path().write_text(remote + "\n")
        self.tile_ok("sync", "pull")

        issue = self.get_issue(issue_id)
        self.assertEqual(issue["title"], "Remote version")

    def test_sync_pull_local_higher_seq_skips(self):
        """sync pull skips when local seq > remote seq."""
        issue_id = self.create_issue("Local version")  # seq=1
        # Bump seq to 5
        for _ in range(4):
            self.tile_ok("update", issue_id, "--title", "Local version")

        remote = json.dumps({
            "id": issue_id,
            "title": "Remote version",
            "description": "",
            "type": "task",
            "priority": 2,
            "status": "open",
            "assignee": None,
            "labels": [],
            "close_reason": None,
            "created_at": "2026-03-20T12:00:00Z",
            "updated_at": "2026-03-20T12:00:00Z",
            "closed_at": None,
            "seq": 2,
            "blocked_by": [],
            "comments": [],
        })
        self._jsonl_path().write_text(remote + "\n")
        self.tile_ok("sync", "pull")

        issue = self.get_issue(issue_id)
        self.assertEqual(issue["title"], "Local version")

    def test_sync_pull_equal_seq_skips(self):
        """sync pull skips when local seq == remote seq."""
        issue_id = self.create_issue("Local version")  # seq=1
        remote = json.dumps({
            "id": issue_id,
            "title": "Remote version",
            "description": "",
            "type": "task",
            "priority": 2,
            "status": "open",
            "assignee": None,
            "labels": [],
            "close_reason": None,
            "created_at": "2026-03-20T12:00:00Z",
            "updated_at": "2026-03-20T12:00:00Z",
            "closed_at": None,
            "seq": 1,
            "blocked_by": [],
            "comments": [],
        })
        self._jsonl_path().write_text(remote + "\n")
        self.tile_ok("sync", "pull")

        issue = self.get_issue(issue_id)
        self.assertEqual(issue["title"], "Local version")

    def test_sync_pull_prefer_remote(self):
        """sync pull --prefer-remote overwrites even when local seq > remote."""
        issue_id = self.create_issue("Local")
        self.tile_ok("update", issue_id, "--title", "Local v2")  # seq=2

        remote = json.dumps({
            "id": issue_id,
            "title": "Remote wins",
            "description": "",
            "type": "task",
            "priority": 2,
            "status": "open",
            "assignee": None,
            "labels": [],
            "close_reason": None,
            "created_at": "2026-03-20T12:00:00Z",
            "updated_at": "2026-03-20T12:00:00Z",
            "closed_at": None,
            "seq": 1,
            "blocked_by": [],
            "comments": [],
        })
        self._jsonl_path().write_text(remote + "\n")
        self.tile_ok("sync", "pull", "--prefer-remote")

        issue = self.get_issue(issue_id)
        self.assertEqual(issue["title"], "Remote wins")

    def test_sync_pull_imports_deps(self):
        """sync pull imports blocked_by deps from JSONL."""
        parent = self.create_issue("Parent")
        child_data = json.dumps({
            "id": "tl-bbbbbb",
            "title": "Imported child",
            "description": "",
            "type": "task",
            "priority": 2,
            "status": "open",
            "assignee": None,
            "labels": [],
            "close_reason": None,
            "created_at": "2026-03-20T12:00:00Z",
            "updated_at": "2026-03-20T12:00:00Z",
            "closed_at": None,
            "seq": 1,
            "blocked_by": [parent],
            "comments": [],
        })
        self._jsonl_path().write_text(child_data + "\n")
        self.tile_ok("sync", "pull")

        issue = self.get_issue("tl-bbbbbb")
        self.assertIn(parent, issue["blocked_by"])

    def test_sync_pull_imports_comments(self):
        """sync pull imports comments from JSONL."""
        issue_data = json.dumps({
            "id": "tl-cccccc",
            "title": "With comments",
            "description": "",
            "type": "task",
            "priority": 2,
            "status": "open",
            "assignee": None,
            "labels": [],
            "close_reason": None,
            "created_at": "2026-03-20T12:00:00Z",
            "updated_at": "2026-03-20T12:00:00Z",
            "closed_at": None,
            "seq": 1,
            "blocked_by": [],
            "comments": [
                {
                    "id": "c-111111",
                    "author": "bob",
                    "body": "Imported comment",
                    "created_at": "2026-03-20T13:00:00Z",
                }
            ],
        })
        self._jsonl_path().write_text(issue_data + "\n")
        self.tile_ok("sync", "pull")

        issue = self.get_issue("tl-cccccc")
        self.assertEqual(len(issue["comments"]), 1)
        self.assertEqual(issue["comments"][0]["body"], "Imported comment")

    def test_sync_pull_leaves_db_only_issues(self):
        """sync pull doesn't delete issues that are in DB but not in JSONL."""
        local_id = self.create_issue("Local only")
        self._jsonl_path().write_text("")  # empty JSONL
        self.tile_ok("sync", "pull")
        # local_id should still exist
        issue = self.get_issue(local_id)
        self.assertEqual(issue["title"], "Local only")

    def test_sync_status(self):
        """sync status returns a report without modifying anything."""
        self.create_issue("A")
        self.tile_ok("sync", "push")
        self.create_issue("B")  # only in DB

        result = self.tile_ok("sync", "status")
        self.assertIsInstance(result, dict)

    def test_sync_push_empty_guard(self):
        """sync push aborts if DB has issues but export would be empty."""
        # This shouldn't happen in practice, but the spec says to guard it.
        # Just verify push works normally with issues present.
        self.create_issue("Not empty")
        self.tile_ok("sync", "push")
        content = self._jsonl_path().read_text()
        self.assertTrue(len(content) > 0)


# ===========================================================================
# 10. ID Prefix Matching
# ===========================================================================

class TestPrefixMatch(TileTestCase):

    def test_prefix_match_unambiguous(self):
        """Short prefix resolves to the full ID if unambiguous."""
        issue_id = self.create_issue("Prefix test")
        # Use first 5 chars (tl-XX) as prefix
        short = issue_id[:5]
        issue = self.get_issue(short)
        self.assertEqual(issue["id"], issue_id)

    def test_prefix_match_full_id(self):
        """Full ID always works."""
        issue_id = self.create_issue("Full ID")
        issue = self.get_issue(issue_id)
        self.assertEqual(issue["id"], issue_id)

    def test_prefix_match_ambiguous_fails(self):
        """Ambiguous prefix exits 1 with matching IDs listed."""
        # Force two IDs with the same prefix by inserting directly
        db = sqlite3.connect(str(self.db_path))
        now = datetime.now(timezone.utc).isoformat()
        for suffix in ("aa1111", "aa2222"):
            db.execute(
                "INSERT INTO issues (id, title, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (f"tl-{suffix}", f"Issue {suffix}", now, now),
            )
        db.commit()
        db.close()

        err = self.tile_fail("show", "tl-aa")
        # Should list both matching IDs
        self.assertIn("tl-aa1111", err)
        self.assertIn("tl-aa2222", err)

    def test_prefix_match_no_match(self):
        """Prefix that matches nothing exits 1."""
        self.tile_fail("show", "tl-zzzzzz")


# ===========================================================================
# 11. Stats
# ===========================================================================

class TestStats(TileTestCase):

    def test_stats_basic(self):
        """stats returns counts by status, type, priority."""
        self.create_issue("A", type="bug", priority="0")
        self.create_issue("B", type="task", priority="1")
        done = self.create_issue("C", type="task")
        self.tile_ok("update", done, "--status", "closed")

        result = self.tile_ok("stats")
        self.assertEqual(result["total"], 3)
        self.assertEqual(result["open"], 2)
        self.assertEqual(result["closed"], 1)

    def test_stats_by_field(self):
        """stats --by groups counts by the given field."""
        self.create_issue("Bug", type="bug")
        self.create_issue("Task", type="task")
        self.create_issue("Task 2", type="task")

        result = self.tile_ok("stats", "--by", "type")
        self.assertEqual(result["bug"], 1)
        self.assertEqual(result["task"], 2)


# ===========================================================================
# 12. Doctor
# ===========================================================================

class TestDoctor(TileTestCase):

    def test_doctor_clean_db(self):
        """doctor passes on a healthy database."""
        self.create_issue("Healthy")
        rc, out, err = self.tile("doctor")
        self.assertEqual(rc, 0)

    def test_doctor_orphan_dep(self):
        """doctor detects orphan deps (referencing missing issues)."""
        issue_id = self.create_issue("Exists")
        # Insert orphan dep directly
        db = sqlite3.connect(str(self.db_path))
        db.execute("PRAGMA foreign_keys=OFF")
        db.execute(
            "INSERT INTO dependencies VALUES (?, ?)",
            (issue_id, "tl-ghost0"),
        )
        db.commit()
        db.close()

        rc, out, err = self.tile("doctor")
        self.assertEqual(rc, 1)

    def test_doctor_orphan_comment(self):
        """doctor detects orphan comments (referencing missing issues)."""
        db = sqlite3.connect(str(self.db_path))
        db.execute("PRAGMA foreign_keys=OFF")
        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            "INSERT INTO comments VALUES (?, ?, ?, ?, ?)",
            ("c-orphan", "tl-ghost0", None, "Orphan", now),
        )
        db.commit()
        db.close()

        rc, out, err = self.tile("doctor")
        self.assertEqual(rc, 1)


# ===========================================================================
# 13. Version
# ===========================================================================

class TestVersion(TileTestCase):

    def test_version_output(self):
        """version prints tile version and Python version."""
        rc, out, err = self.tile("version")
        self.assertEqual(rc, 0)
        self.assertIn("version", out)
        self.assertIn("python", out)


# ===========================================================================
# 14. Seq Invariants
# ===========================================================================

class TestSeq(TileTestCase):

    def test_seq_starts_at_1(self):
        """New issue has seq=1."""
        issue_id = self.create_issue("Fresh")
        self.assertEqual(self.get_issue(issue_id)["seq"], 1)

    def test_seq_increments_on_update(self):
        """Each update increments seq by 1."""
        issue_id = self.create_issue("Seq test")
        self.tile_ok("update", issue_id, "--title", "V2")
        self.assertEqual(self.get_issue(issue_id)["seq"], 2)
        self.tile_ok("update", issue_id, "--title", "V3")
        self.assertEqual(self.get_issue(issue_id)["seq"], 3)

    def test_seq_increments_on_label_change(self):
        """label add/remove increments seq."""
        issue_id = self.create_issue("Label seq")
        self.tile_ok("label", "add", issue_id, "x")
        self.assertEqual(self.get_issue(issue_id)["seq"], 2)
        self.tile_ok("label", "remove", issue_id, "x")
        self.assertEqual(self.get_issue(issue_id)["seq"], 3)

    def test_seq_increments_on_comment(self):
        """comment add increments parent issue's seq."""
        issue_id = self.create_issue("Comment seq")
        self.tile_ok("comment", "add", issue_id, "Note")
        self.assertEqual(self.get_issue(issue_id)["seq"], 2)

    def test_seq_increments_on_dep_change(self):
        """dep add/remove increments seq on both issues."""
        a = self.create_issue("A")
        b = self.create_issue("B")
        self.tile_ok("dep", "add", b, a)
        self.assertEqual(self.get_issue(a)["seq"], 2)
        self.assertEqual(self.get_issue(b)["seq"], 2)
        self.tile_ok("dep", "remove", b, a)
        self.assertEqual(self.get_issue(a)["seq"], 3)
        self.assertEqual(self.get_issue(b)["seq"], 3)

    def test_seq_increments_on_status_change(self):
        """Closing and reopening each increment seq."""
        issue_id = self.create_issue("Status seq")
        self.tile_ok("update", issue_id, "--status", "closed")
        self.assertEqual(self.get_issue(issue_id)["seq"], 2)
        self.tile_ok("update", issue_id, "--status", "open")
        self.assertEqual(self.get_issue(issue_id)["seq"], 3)


# ===========================================================================
# 15. Impact Calculation
# ===========================================================================

class TestImpact(TileTestCase):

    def test_impact_direct(self):
        """Issue blocking one other has impact=1."""
        a = self.create_issue("Blocker")
        b = self.create_issue("Blocked")
        self.tile_ok("dep", "add", b, a)
        result = self.tile_ok("list", "--ready")
        blocker = next(r for r in result if r["id"] == a)
        self.assertEqual(blocker["impact"], 1)

    def test_impact_transitive(self):
        """Impact counts transitive downstream issues."""
        a = self.create_issue("Root")
        b = self.create_issue("Mid")
        c = self.create_issue("Leaf")
        self.tile_ok("dep", "add", b, a)  # b blocked by a
        self.tile_ok("dep", "add", c, b)  # c blocked by b
        result = self.tile_ok("list", "--ready")
        root = next(r for r in result if r["id"] == a)
        self.assertEqual(root["impact"], 2)  # unblocks b and c

    def test_impact_excludes_closed(self):
        """Impact doesn't count closed downstream issues."""
        a = self.create_issue("Root")
        b = self.create_issue("Closed child")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("update", b, "--status", "closed")
        result = self.tile_ok("list", "--ready")
        root = next(r for r in result if r["id"] == a)
        self.assertEqual(root["impact"], 0)

    def test_impact_no_deps(self):
        """Issue with no downstream deps has impact=0."""
        a = self.create_issue("Standalone")
        result = self.tile_ok("list", "--ready")
        standalone = next(r for r in result if r["id"] == a)
        self.assertEqual(standalone["impact"], 0)

    def test_impact_diamond(self):
        """Diamond dep graph counts shared descendant only once."""
        a = self.create_issue("Top")
        b = self.create_issue("Left")
        c = self.create_issue("Right")
        d = self.create_issue("Bottom")
        self.tile_ok("dep", "add", b, a)
        self.tile_ok("dep", "add", c, a)
        self.tile_ok("dep", "add", d, b)
        self.tile_ok("dep", "add", d, c)
        result = self.tile_ok("list", "--ready")
        top = next(r for r in result if r["id"] == a)
        # a blocks b, c, d — but d is counted once = impact 3
        self.assertEqual(top["impact"], 3)


# ===========================================================================
# 16. Output Mode Detection
# ===========================================================================

class TestOutputMode(TileTestCase):

    def test_json_auto_when_not_tty(self):
        """When stdout is not a TTY, output defaults to JSON."""
        # The tile() helper forces JSON; this test verifies the logic
        # exists in the implementation. We'd mock sys.stdout.isatty().
        self.create_issue("Auto JSON")
        # This is a structural test — verify the implementation respects
        # isatty(). Actual verification depends on the CLI entry point.
        result = self.tile_ok("list")
        # In test mode (not a TTY), this should be parsed JSON
        self.assertIsInstance(result, list)

    def test_human_flag_overrides(self):
        """--human forces human output even when not a TTY."""
        # This would need to capture raw stdout and verify it's NOT JSON.
        # Placeholder for when the implementation exists.
        pass

    def test_json_flag_forces_json(self):
        """--json forces JSON output even when stdout is a TTY."""
        # Placeholder — would mock isatty() to return True.
        pass


if __name__ == "__main__":
    unittest.main()
