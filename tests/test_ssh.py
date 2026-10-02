# -*- coding: utf-8 -*-

"""Tests for seamm_slurm.ssh.SshSlurm."""

from unittest.mock import patch, MagicMock

from seamm_slurm.ssh import SshSlurm


def test_ssh_run_wraps_command_through_ssh():
    fake_proc = MagicMock(returncode=0, stdout="42\n", stderr="")
    with patch("seamm_slurm.ssh.subprocess.run", return_value=fake_proc) as run:
        backend = SshSlurm("molssi10")
        rc, out, err = backend._run(["sbatch", "--parsable"], input_text="script")

    assert rc == 0
    assert out == "42\n"
    run.assert_called_once_with(
        ["ssh", "molssi10", "sbatch --parsable"],
        input="script",
        capture_output=True,
        text=True,
    )


def test_ssh_run_quotes_arguments_with_special_characters():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_slurm.ssh.subprocess.run", return_value=fake_proc) as run:
        backend = SshSlurm("chemai")
        backend._run(["squeue", "--jobs", "1,2,3"])

    called_argv = run.call_args.args[0]
    assert called_argv[0] == "ssh"
    assert called_argv[1] == "chemai"
    # A simple, space-free token should pass through unquoted, but must be
    # reconstructible by a remote shell either way.
    import shlex

    assert shlex.split(called_argv[2]) == ["squeue", "--jobs", "1,2,3"]


def test_ssh_custom_ssh_command():
    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_slurm.ssh.subprocess.run", return_value=fake_proc) as run:
        backend = SshSlurm("molssi10", ssh_command="/usr/bin/ssh")
        backend._run(["squeue"])

    assert run.call_args.args[0][0] == "/usr/bin/ssh"
