# -*- coding: utf-8 -*-

"""Run queueing-system commands directly on the current host."""

import subprocess


class LocalTransport:
    """Runs commands on this host -- the case where the caller (a JobServer, or
    an evaluator inside an allocation) is itself on a submit host with
    ``sbatch``/``qsub`` and friends on the ``PATH``."""

    name = "local"
    host = None

    def run(self, argv, input_text=None):
        """Run a command.

        Returns
        -------
        (int, str, str)
            ``(returncode, stdout, stderr)``.
        """
        proc = subprocess.run(
            argv,
            input=input_text,
            capture_output=True,
            text=True,
        )
        return proc.returncode, proc.stdout, proc.stderr
