# -*- coding: utf-8 -*-

"""Run queueing-system commands on a remote host over ssh."""

import shlex
import subprocess


class SshTransport:
    """Runs commands on a remote host over passwordless ssh -- the case where
    the caller is not on a submit host and reaches the cluster's login node
    over ssh instead.

    Assumes key-based, passwordless auth is already set up for ``host`` (e.g.
    an ``~/.ssh/config`` alias), the same way a user would run
    ``ssh <host> sbatch ...`` by hand.
    """

    name = "ssh"

    def __init__(self, host, *, ssh_command="ssh", ssh_options=(), timeout=None):
        self.host = host
        self.ssh_command = ssh_command
        # e.g. TASK_SSH_OPTIONS; none by default, so the JobServer's commands
        # are exactly as they always were.
        self.ssh_options = list(ssh_options)
        # Seconds before a command that hangs (a connection dead after the
        # laptop slept) is given up, as returncode 255.
        self.timeout = timeout

    def run(self, argv, input_text=None):
        """Run a command on the remote host.

        Returns
        -------
        (int, str, str)
            ``(returncode, stdout, stderr)``.
        """
        remote_cmd = " ".join(shlex.quote(str(a)) for a in argv)
        kwargs = {} if self.timeout is None else {"timeout": self.timeout}
        try:
            proc = subprocess.run(
                [self.ssh_command, *self.ssh_options, self.host, remote_cmd],
                input=input_text,
                capture_output=True,
                text=True,
                **kwargs,
            )
        except subprocess.TimeoutExpired:
            return 255, "", f"ssh: {self.host}: timed out after {self.timeout} s"
        return proc.returncode, proc.stdout, proc.stderr


#: ssh options for the task layer's many short connections from a machine whose
#: network comes and goes (a laptop that sleeps or changes networks): fail fast
#: instead of hanging, never prompt, and do not set up the host alias's port
#: forwards on every command.
TASK_SSH_OPTIONS = (
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=30",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=4",
    "-o",
    "ClearAllForwardings=yes",
)
