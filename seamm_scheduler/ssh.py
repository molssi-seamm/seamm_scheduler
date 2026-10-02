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

    def __init__(self, host, *, ssh_command="ssh"):
        self.host = host
        self.ssh_command = ssh_command

    def run(self, argv, input_text=None):
        """Run a command on the remote host.

        Returns
        -------
        (int, str, str)
            ``(returncode, stdout, stderr)``.
        """
        remote_cmd = " ".join(shlex.quote(str(a)) for a in argv)
        proc = subprocess.run(
            [self.ssh_command, self.host, remote_cmd],
            input=input_text,
            capture_output=True,
            text=True,
        )
        return proc.returncode, proc.stdout, proc.stderr
