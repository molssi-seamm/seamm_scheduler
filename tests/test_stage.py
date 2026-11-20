# -*- coding: utf-8 -*-

"""Tests for seamm_scheduler.stage (LocalStager/RsyncStager)."""

from unittest.mock import patch, MagicMock

import pytest

from seamm_scheduler.stage import (
    STAGE_LOCK_FILENAME,
    LocalStager,
    RsyncStager,
    StageError,
)


def test_stage_lock_filename_is_a_plain_relative_filename():
    """Consumers join this under a job's own wdir (e.g.
    ``Path(wdir) / STAGE_LOCK_FILENAME``) -- it must not itself be a path
    with directory components."""
    assert "/" not in STAGE_LOCK_FILENAME
    assert STAGE_LOCK_FILENAME


def test_local_stager_stage_in_is_a_no_op_returning_local_wdir():
    stager = LocalStager()
    assert stager.stage_in("/local/Job_1", "/remote/Job_1") == "/local/Job_1"


def test_local_stager_stage_out_is_a_no_op():
    stager = LocalStager()
    assert stager.stage_out("/remote/Job_1", "/local/Job_1") is None


def test_rsync_stager_stage_in_makes_remote_dir_then_pushes():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        stager = RsyncStager("molssi10")
        result = stager.stage_in("/local/Job_1", "/remote/Job_1")

    assert result == "/remote/Job_1"
    assert run.call_count == 3
    mkdir_call, rsync_call, side_files_call = run.call_args_list
    # The mirrored files there are listed, to remove those no longer here
    assert "find . -type f" in side_files_call.args[0][-1]

    assert mkdir_call.args[0] == ["ssh", "molssi10", "mkdir -p /remote/Job_1"]

    rsync_argv = rsync_call.args[0]
    assert rsync_argv[0] == "rsync"
    assert rsync_argv[1:3] == ["-e", "ssh"]
    assert "-a" in rsync_argv
    assert rsync_argv[-2] == "/local/Job_1/"
    assert rsync_argv[-1] == "molssi10:/remote/Job_1/"


def test_rsync_stager_stage_out_pulls_in_reverse():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        stager = RsyncStager("molssi10")
        stager.stage_out("/remote/Job_1", "/local/Job_1")

    # Nothing here to mirror (no SQLite side files), so only the copy
    assert run.call_count == 1
    rsync_argv = run.call_args_list[0].args[0]
    assert "--delete" not in rsync_argv
    assert rsync_argv[-2] == "molssi10:/remote/Job_1/"
    assert rsync_argv[-1] == "/local/Job_1/"


def test_rsync_stager_stage_in_raises_on_mkdir_failure():
    fake_proc = MagicMock(returncode=1, stdout="", stderr="permission denied")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc):
        stager = RsyncStager("molssi10")
        with pytest.raises(StageError, match="permission denied"):
            stager.stage_in("/local/Job_1", "/remote/Job_1")


def test_rsync_stager_stage_in_raises_on_rsync_failure():
    ok = MagicMock(returncode=0, stdout="", stderr="")
    failed = MagicMock(returncode=1, stdout="", stderr="connection refused")
    with patch("seamm_scheduler.stage.subprocess.run", side_effect=[ok, failed]):
        stager = RsyncStager("molssi10")
        with pytest.raises(StageError, match="connection refused"):
            stager.stage_in("/local/Job_1", "/remote/Job_1")


def test_rsync_stager_stage_out_raises_on_rsync_failure():
    fake_proc = MagicMock(returncode=1, stdout="", stderr="no such file")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc):
        stager = RsyncStager("molssi10")
        with pytest.raises(StageError, match="no such file"):
            stager.stage_out("/remote/Job_1", "/local/Job_1")


def test_rsync_stager_custom_commands():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        stager = RsyncStager(
            "molssi10", ssh_command="/usr/bin/ssh", rsync_command="/usr/bin/rsync"
        )
        stager.stage_in("/local/Job_1", "/remote/Job_1")

    mkdir_call, rsync_call, _ = run.call_args_list
    assert mkdir_call.args[0][0] == "/usr/bin/ssh"
    assert rsync_call.args[0][0] == "/usr/bin/rsync"
    assert rsync_call.args[0][2] == "/usr/bin/ssh"


# ---- push/pull of many paths (the task layer) ----------------------------


def test_local_stager_push_pull_are_no_ops():
    stager = LocalStager()
    assert stager.push("/a", "/b", ["x"]) is None
    assert stager.pull("/b", "/a", ["x"]) is None


def test_rsync_push_uses_one_files_from_rsync():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        RsyncStager("tc").push("/l/Job_1", "/r/Job_1", ["tasks/a", "tasks/b"])
    mkdir, rsync = run.call_args_list
    assert mkdir.args[0] == ["ssh", "tc", "mkdir -p /r/Job_1"]
    assert rsync.args[0] == [
        "rsync",
        "-e",
        "ssh",
        "-a",
        "-r",
        "--files-from=-",
        "/l/Job_1/",
        "tc:/r/Job_1/",
    ]
    assert rsync.kwargs["input"] == "tasks/a\ntasks/b\n"


def test_rsync_pull_with_excludes():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        RsyncStager("tc").pull("/r", "/l", ["tasks/a"], exclude=["*.tmp"])
    (rsync,) = run.call_args_list
    assert rsync.args[0] == [
        "rsync",
        "-e",
        "ssh",
        "-a",
        "-r",
        "--files-from=-",
        "--exclude",
        "*.tmp",
        "tc:/r/",
        "/l/",
    ]


def test_rsync_push_nothing_does_nothing():
    with patch("seamm_scheduler.stage.subprocess.run") as run:
        RsyncStager("tc").push("/l", "/r", [])
    run.assert_not_called()


def test_rsync_push_failure_raises():
    ok = MagicMock(returncode=0, stdout="", stderr="")
    bad = MagicMock(returncode=23, stdout="", stderr="some files vanished")
    with patch("seamm_scheduler.stage.subprocess.run", side_effect=[ok, bad]):
        with pytest.raises(StageError, match="vanished"):
            RsyncStager("tc").push("/l", "/r", ["x"])


def test_rsync_with_ssh_options_and_timeout():
    import subprocess

    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        RsyncStager("tc", ssh_options=["-o", "BatchMode=yes"], timeout=9).pull(
            "/r", "/l", ["a"]
        )
    argv = run.call_args.args[0]
    assert argv[:3] == ["rsync", "-e", "ssh -o BatchMode=yes"]
    assert run.call_args.kwargs["timeout"] == 9
    with patch(
        "seamm_scheduler.stage.subprocess.run",
        side_effect=subprocess.TimeoutExpired("rsync", 9),
    ):
        with pytest.raises(StageError, match="^ssh: rsync .* timed out"):
            RsyncStager("tc", timeout=9).pull("/r", "/l", ["a"])


def test_stage_out_removes_a_stale_sqlite_log(tmp_path):
    """A job database pulled twice: the first pull brought a write-ahead log,
    the job then folded it into the database and deleted it. The second pull
    must remove the local log too, or SQLite replays it over the newer database.
    Uses rsync for real, with an 'ssh' that runs the remote side locally."""
    import shutil
    import stat

    if shutil.which("rsync") is None:
        pytest.skip("rsync is not available")
    fake_ssh = tmp_path / "fake_ssh"
    fake_ssh.write_text('#!/bin/sh\nshift\nexec sh -c "$*"\n')
    fake_ssh.chmod(fake_ssh.stat().st_mode | stat.S_IEXEC)

    remote = tmp_path / "remote"
    local = tmp_path / "local"
    (remote / "sub").mkdir(parents=True)
    local.mkdir()
    (remote / "seamm.db").write_text("database, first state")
    (remote / "seamm.db-wal").write_text("log")
    (remote / "seamm.db-shm").write_text("index")
    (remote / "sub" / "other.db-wal").write_text("a nested log")
    (remote / "job.out").write_text("output")
    (remote / "sub" / "loop_entry.db").write_text("a parallel loop's copy")
    (local / ".stage.lock").write_text("")  # only here: must survive
    (local / "only_here").mkdir()  # an empty directory only here: must survive

    stager = RsyncStager("remotehost", ssh_command=str(fake_ssh))
    stager.stage_out(str(remote), str(local))
    assert (local / "seamm.db-wal").exists()

    # The job finishes: its log is folded into the database and removed.
    (remote / "seamm.db").write_text("database, the final state")
    (remote / "seamm.db-wal").unlink()
    (remote / "seamm.db-shm").unlink()
    (remote / "sub" / "loop_entry.db").unlink()  # the loop ended
    stager.stage_out(str(remote), str(local))
    assert not (local / "sub" / "loop_entry.db").exists()
    # A nested log too (an iteration's own database)
    (remote / "sub" / "other.db-wal").unlink()
    stager.stage_out(str(remote), str(local))
    assert not (local / "sub" / "other.db-wal").exists()
    (remote / "sub" / "other.db-wal").write_text("a nested log")  # for below
    stager.stage_out(str(remote), str(local))

    assert (local / "seamm.db").read_text() == "database, the final state"
    assert not (local / "seamm.db-wal").exists()
    assert not (local / "seamm.db-shm").exists()
    assert (local / "sub" / "other.db-wal").exists()  # still there remotely
    assert (local / ".stage.lock").exists()
    assert (local / "only_here").is_dir()
    assert (local / "job.out").exists()


def test_stage_in_removes_a_stale_log_there(tmp_path):
    """Pushing a job back (a resubmit) removes the side files gone here."""
    import stat

    fake_ssh = tmp_path / "fake_ssh"
    fake_ssh.write_text('#!/bin/sh\nshift\nexec sh -c "$*"\n')
    fake_ssh.chmod(fake_ssh.stat().st_mode | stat.S_IEXEC)
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    (local / "4" / "iter_1" / "_evaluator").mkdir(parents=True)
    (remote / "4" / "iter_1" / "_evaluator").mkdir(parents=True)
    (local / "seamm.db").write_text("db")
    (remote / "seamm.db-wal").write_text("stale log")
    (remote / "4" / "iter_1" / "_evaluator" / "seamm.db-shm").write_text("stale")
    (remote / "4" / "loop_entry.db").write_text("stale copy")
    (local / "4" / "iter_1" / "_evaluator" / "seamm.db-wal").write_text("current")
    stager = RsyncStager("remotehost", ssh_command=str(fake_ssh))
    stager.stage_in(str(local), str(remote))
    assert not (remote / "seamm.db-wal").exists()
    assert not (remote / "4" / "iter_1" / "_evaluator" / "seamm.db-shm").exists()
    assert not (remote / "4" / "loop_entry.db").exists()
    assert (remote / "4" / "iter_1" / "_evaluator" / "seamm.db-wal").read_text() == (
        "current"
    )
    assert (remote / "seamm.db").read_text() == "db"


def test_local_stager_remote_exists_is_always_true():
    assert LocalStager().remote_exists("/anything") is True


@pytest.mark.parametrize("stdout, expected", [("yes\n", True), ("", False)])
def test_rsync_stager_remote_exists(stdout, expected):
    """Whether a resumed or resubmitted job's remote directory is already there,
    so that it need not be staged in again (ChemAI 5398: 28 GB re-uploaded on a
    resubmit blocked the JobServer for 3.5 h, 2026-10-31)."""
    fake_proc = MagicMock(returncode=0, stdout=stdout, stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        stager = RsyncStager("host")
        assert stager.remote_exists("/remote/Job_1") is expected
    argv = run.call_args[0][0]
    assert argv[:2] == ["ssh", "host"]
    assert "test -d /remote/Job_1" in argv[-1]


def _gnu_find():
    """The remote listing uses GNU find's -printf, as on the clusters."""
    import subprocess

    try:
        return (
            subprocess.run(["find", "--version"], capture_output=True).returncode == 0
        )
    except OSError:
        return False


def test_local_stager_removes_nothing():
    """The local stager's remote directory is the local one: never removed."""
    stager = LocalStager()
    assert stager.missing_here("/local/Job_1", "/local/Job_1") == []
    assert stager.remove_remote("/local/Job_1", "/local/Job_1") is False


def test_missing_here_compares_the_remote_listing(tmp_path):
    """A remote file counts as copied back when it is here with the same size,
    or here and newer (the JobServer rewrites job_data.json after copying)."""
    import os

    (tmp_path / "sub").mkdir()
    (tmp_path / "same").write_text("12345")
    (tmp_path / "rewritten").write_text("a longer, newer file")
    (tmp_path / "sub" / "older").write_text("123")
    os.utime(tmp_path / "sub" / "older", (1000.0, 1000.0))
    listing = (
        "same\t5\t2000.0\tf\n"
        "rewritten\t5\t1000.0\tf\n"
        "sub/older\t9\t2000.0\tf\n"
        "gone\t1\t1000.0\tf\n"
        "link\t4\t1000.0\tl\n"
    )
    fake_proc = MagicMock(returncode=0, stdout=listing, stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=fake_proc) as run:
        missing = RsyncStager("host").missing_here("/r/x/Job_1", str(tmp_path))
    assert "-printf" in run.call_args[0][0][-1]
    assert [name for name, _ in missing] == ["sub/older", "gone", "link"]


def test_remove_remote_refuses_a_shallow_path():
    with patch("seamm_scheduler.stage.subprocess.run") as run:
        for path in ("/", "/projects", "/projects/seamm", "relative/a/b/Job_1"):
            with pytest.raises(StageError, match="refusing to remove"):
                RsyncStager("host").remove_remote(path, "/local/Job_1")
    run.assert_not_called()


def test_remove_remote_keeps_a_directory_not_all_here(tmp_path):
    """Nothing is removed when a file there is not here."""
    exists = MagicMock(returncode=0, stdout="yes\n", stderr="")
    listing = MagicMock(returncode=0, stdout="job.out\t3\t1000.0\tf\n", stderr="")
    with patch(
        "seamm_scheduler.stage.subprocess.run", side_effect=[exists, listing]
    ) as run:
        with pytest.raises(StageError, match="1 file.*so it was kept: job.out"):
            RsyncStager("host").remove_remote("/r/x/y/Job_1", str(tmp_path))
    assert run.call_count == 2  # no rm


def test_remove_remote_nothing_there():
    gone = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.stage.subprocess.run", return_value=gone) as run:
        assert RsyncStager("host").remove_remote("/r/x/y/Job_1", "/l") is False
    assert run.call_count == 1


@pytest.mark.skipif(not _gnu_find(), reason="needs GNU find (-printf)")
def test_remove_remote_after_stage_out(tmp_path):
    """End to end, with an 'ssh' that runs the remote side locally: a directory
    staged back is removed there; one with a file not here is kept."""
    import shutil
    import stat

    if shutil.which("rsync") is None:
        pytest.skip("rsync is not available")
    fake_ssh = tmp_path / "fake_ssh"
    fake_ssh.write_text('#!/bin/sh\nshift\nexec sh -c "$*"\n')
    fake_ssh.chmod(fake_ssh.stat().st_mode | stat.S_IEXEC)
    remote = tmp_path / "remote" / "Job_1"
    local = tmp_path / "local" / "Job_1"
    (remote / "1" / "iter_1").mkdir(parents=True)
    local.mkdir(parents=True)
    (remote / "job.out").write_text("output")
    (remote / "job_data.json").write_text("{}")
    (remote / "1" / "iter_1" / "step.out").write_text("step")
    (remote / "1" / "link").symlink_to("iter_1")
    stager = RsyncStager("remotehost", ssh_command=str(fake_ssh))
    stager.stage_out(str(remote), str(local))
    (local / "job_data.json").write_text('{"state": "finished"}')  # rewritten here

    (remote / "late.txt").write_text("written after the copy back")
    with pytest.raises(StageError, match="late.txt: not here"):
        stager.remove_remote(str(remote), str(local))
    assert remote.is_dir()

    stager.stage_out(str(remote), str(local))
    assert stager.remove_remote(str(remote), str(local)) is True
    assert not remote.exists()
    assert (local / "1" / "iter_1" / "step.out").read_text() == "step"
