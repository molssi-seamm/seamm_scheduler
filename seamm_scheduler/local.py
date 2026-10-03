# -*- coding: utf-8 -*-

"""Run queueing-system commands directly on the current host."""

import os
import subprocess

# Site settings, not the allocation's: never dropped
_KEEP = {"SLURM_CONF"}


class LocalTransport:
    """Runs commands on this host -- the case where the caller (a JobServer, or
    an evaluator inside an allocation) is itself on a submit host with
    ``sbatch``/``qsub`` and friends on the ``PATH``."""

    name = "local"
    host = None

    def __init__(self, *, drop_env_prefixes=()):
        # An evaluator that is itself a batch job must not pass its own
        # allocation (SLURM_MEM_PER_CPU, ...) on to the jobs it submits.
        self.drop_env_prefixes = tuple(drop_env_prefixes)

    def run(self, argv, input_text=None):
        """Run a command.

        Returns
        -------
        (int, str, str)
            ``(returncode, stdout, stderr)``.
        """
        if self.drop_env_prefixes:
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(self.drop_env_prefixes) or k in _KEEP
            }
            proc = subprocess.run(
                argv, input=input_text, capture_output=True, text=True, env=env
            )
        else:
            proc = subprocess.run(
                argv,
                input=input_text,
                capture_output=True,
                text=True,
            )
        return proc.returncode, proc.stdout, proc.stderr
