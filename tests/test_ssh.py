# -*- coding: utf-8 -*-

"""Tests for seamm_scheduler.ssh.SshSlurm."""

from unittest.mock import patch, MagicMock

from seamm_scheduler.slurm import SshSlurm


def test_ssh_run_wraps_command_through_ssh():
    fake_proc = MagicMock(returncode=0, stdout="42\n", stderr="")
    with patch("seamm_scheduler.ssh.subprocess.run", return_value=fake_proc) as run:
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
    with patch("seamm_scheduler.ssh.subprocess.run", return_value=fake_proc) as run:
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
    with patch("seamm_scheduler.ssh.subprocess.run", return_value=fake_proc) as run:
        backend = SshSlurm("molssi10", ssh_command="/usr/bin/ssh")
        backend._run(["squeue"])

    assert run.call_args.args[0][0] == "/usr/bin/ssh"


def test_ssh_options_and_timeout():
    import subprocess

    from seamm_scheduler.ssh import TASK_SSH_OPTIONS, SshTransport

    fake_proc = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.ssh.subprocess.run", return_value=fake_proc) as run:
        SshTransport("tc", ssh_options=TASK_SSH_OPTIONS, timeout=5).run(["squeue"])
    argv = run.call_args.args[0]
    assert argv[:2] == ["ssh", "-o"] and argv[-2:] == ["tc", "squeue"]
    assert "ClearAllForwardings=yes" in argv
    assert run.call_args.kwargs["timeout"] == 5

    with patch(
        "seamm_scheduler.ssh.subprocess.run",
        side_effect=subprocess.TimeoutExpired("ssh", 5),
    ):
        rc, out, err = SshTransport("tc", timeout=5).run(["squeue"])
    assert rc == 255 and err.startswith("ssh: tc: timed out")


def test_task_backend_uses_task_ssh_options_jobserver_does_not():
    from seamm_scheduler import TargetSection

    s = TargetSection(name="x", transport="ssh", host="tc", type="local", tasks="queue")
    assert "BatchMode=yes" in s.build_task_backend().transport.ssh_options
    assert s.build_task_stager().ssh_options
    assert s.build_task_backend().transport.timeout == 300
    j = TargetSection(name="x", transport="ssh", host="tc")
    assert j.build_backend().transport.ssh_options == []
    assert j.build_stager().ssh_options == []
