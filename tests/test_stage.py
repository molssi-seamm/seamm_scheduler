# -*- coding: utf-8 -*-

"""Tests for seamm_slurm.stage (LocalStager/RsyncStager)."""

from unittest.mock import patch, MagicMock

import pytest

from seamm_slurm.stage import (
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
    with patch("seamm_slurm.stage.subprocess.run", return_value=fake_proc) as run:
        stager = RsyncStager("molssi10")
        result = stager.stage_in("/local/Job_1", "/remote/Job_1")

    assert result == "/remote/Job_1"
    assert run.call_count == 2
    mkdir_call, rsync_call = run.call_args_list

    assert mkdir_call.args[0] == ["ssh", "molssi10", "mkdir -p /remote/Job_1"]

    rsync_argv = rsync_call.args[0]
    assert rsync_argv[0] == "rsync"
    assert rsync_argv[1:3] == ["-e", "ssh"]
    assert "-a" in rsync_argv
    assert rsync_argv[-2] == "/local/Job_1/"
    assert rsync_argv[-1] == "molssi10:/remote/Job_1/"


def test_rsync_stager_stage_out_pulls_in_reverse():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_slurm.stage.subprocess.run", return_value=fake_proc) as run:
        stager = RsyncStager("molssi10")
        stager.stage_out("/remote/Job_1", "/local/Job_1")

    assert run.call_count == 1
    rsync_argv = run.call_args.args[0]
    assert rsync_argv[-2] == "molssi10:/remote/Job_1/"
    assert rsync_argv[-1] == "/local/Job_1/"


def test_rsync_stager_stage_in_raises_on_mkdir_failure():
    fake_proc = MagicMock(returncode=1, stdout="", stderr="permission denied")
    with patch("seamm_slurm.stage.subprocess.run", return_value=fake_proc):
        stager = RsyncStager("molssi10")
        with pytest.raises(StageError, match="permission denied"):
            stager.stage_in("/local/Job_1", "/remote/Job_1")


def test_rsync_stager_stage_in_raises_on_rsync_failure():
    ok = MagicMock(returncode=0, stdout="", stderr="")
    failed = MagicMock(returncode=1, stdout="", stderr="connection refused")
    with patch("seamm_slurm.stage.subprocess.run", side_effect=[ok, failed]):
        stager = RsyncStager("molssi10")
        with pytest.raises(StageError, match="connection refused"):
            stager.stage_in("/local/Job_1", "/remote/Job_1")


def test_rsync_stager_stage_out_raises_on_rsync_failure():
    fake_proc = MagicMock(returncode=1, stdout="", stderr="no such file")
    with patch("seamm_slurm.stage.subprocess.run", return_value=fake_proc):
        stager = RsyncStager("molssi10")
        with pytest.raises(StageError, match="no such file"):
            stager.stage_out("/remote/Job_1", "/local/Job_1")


def test_rsync_stager_custom_commands():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_slurm.stage.subprocess.run", return_value=fake_proc) as run:
        stager = RsyncStager(
            "molssi10", ssh_command="/usr/bin/ssh", rsync_command="/usr/bin/rsync"
        )
        stager.stage_in("/local/Job_1", "/remote/Job_1")

    mkdir_call, rsync_call = run.call_args_list
    assert mkdir_call.args[0][0] == "/usr/bin/ssh"
    assert rsync_call.args[0][0] == "/usr/bin/rsync"
    assert rsync_call.args[0][2] == "/usr/bin/ssh"
