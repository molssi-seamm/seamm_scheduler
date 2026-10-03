# -*- coding: utf-8 -*-

"""Tests for seamm_scheduler.local.LocalSlurm."""

from unittest.mock import patch, MagicMock

from seamm_scheduler.slurm import LocalSlurm


def test_local_run_invokes_subprocess_directly():
    fake_proc = MagicMock(returncode=0, stdout="42\n", stderr="")
    with patch("seamm_scheduler.local.subprocess.run", return_value=fake_proc) as run:
        backend = LocalSlurm()
        rc, out, err = backend._run(["sbatch", "--parsable"], input_text="script")

    assert rc == 0
    assert out == "42\n"
    assert err == ""
    run.assert_called_once_with(
        ["sbatch", "--parsable"],
        input="script",
        capture_output=True,
        text=True,
    )


def test_local_submit_end_to_end_with_mocked_subprocess():
    fake_proc = MagicMock(returncode=0, stdout="7\n", stderr="")
    with patch("seamm_scheduler.local.subprocess.run", return_value=fake_proc):
        backend = LocalSlurm()
        job_id = backend.submit("#!/bin/bash\necho hi\n")

    assert job_id == "7"
