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
    assert "--delete" in side_files_call.args[0]

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

    assert run.call_count == 2
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
    (local / ".stage.lock").write_text("")  # only here: must survive

    stager = RsyncStager("remotehost", ssh_command=str(fake_ssh))
    stager.stage_out(str(remote), str(local))
    assert (local / "seamm.db-wal").exists()

    # The job finishes: its log is folded into the database and removed.
    (remote / "seamm.db").write_text("database, final state")
    (remote / "seamm.db-wal").unlink()
    (remote / "seamm.db-shm").unlink()
    stager.stage_out(str(remote), str(local))

    assert (local / "seamm.db").read_text() == "database, final state"
    assert not (local / "seamm.db-wal").exists()
    assert not (local / "seamm.db-shm").exists()
    assert (local / "sub" / "other.db-wal").exists()  # still there remotely
    assert (local / ".stage.lock").exists()
    assert (local / "job.out").exists()
