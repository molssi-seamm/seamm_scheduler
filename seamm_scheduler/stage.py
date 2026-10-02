# -*- coding: utf-8 -*-

"""Stage a job's working directory to/from a remote host.

Needed when a JobServer's ``transport = ssh`` points at a host that shares
no filesystem with the JobServer itself -- unlike every case validated so
far, where the JobServer runs on (or already shares storage with) the
SLURM submit host. Paired with the transport the same way
``LocalSlurm``/``SshSlurm`` are: ``LocalStager`` for the shared-storage
case (a no-op) and ``RsyncStager`` for the no-shared-filesystem case.

Lives here, not in ``seamm_jobserver``, for the same reason ``config.py``
does: a lightweight consumer should be able to use the transport without
pulling in the rest of ``seamm_jobserver`` (psutil, GUI code, job-running
machinery). See ``seamm_jobserver``'s ``docs/developer_guide/campaigns/
2026-08-05/`` (Phase 8) for the full design this implements.

The task layer (``seamm_exec``'s scheduler back end) moves many small task
directories at once: :meth:`JobStager.push` and :meth:`JobStager.pull` copy a
list of paths relative to a base directory in one ``rsync``.
"""

import shlex
import subprocess
from abc import ABC, abstractmethod

# Shared filename (not a full path -- callers join it under a job's own
# local working directory) for an advisory lock guarding concurrent
# stage_out() calls for the same job -- e.g. seamm_jobserver's real
# end-of-run pull racing a Dashboard's on-demand "sync a still-running
# job's files now" pull. Lives here (a plain string constant, no new
# dependency) rather than as locking logic in this module, since
# seamm_scheduler is deliberately dependency-free (see pyproject.toml) --
# each consumer already depends on `fasteners` for its own reasons and
# uses this filename to agree on where to acquire an
# InterProcessLock, without seamm_scheduler itself needing to depend on it.
STAGE_LOCK_FILENAME = ".stage.lock"


class StageError(RuntimeError):
    """Raised when staging a job's files to or from a remote host fails."""


class JobStager(ABC):
    """Moves a job's working directory to/from wherever it actually needs
    to run."""

    @abstractmethod
    def stage_in(self, local_wdir, remote_wdir):
        """Make ``remote_wdir`` ready to run the job, from ``local_wdir``.

        Returns the working directory the job should actually use (for the
        ``#SBATCH --chdir`` directive, and for the job's own command line)
        -- ``local_wdir`` itself for ``LocalStager``, ``remote_wdir`` for
        ``RsyncStager``.
        """

    @abstractmethod
    def stage_out(self, remote_wdir, local_wdir):
        """Pull a finished job's files back, so ``job_data.json`` and the
        flowchart's results land in ``local_wdir`` where the JobServer
        (and the dashboard) can see them. No-op for ``LocalStager``."""

    def push(self, local_base, remote_base, paths, *, delete=False):
        """Copy ``paths`` (relative to ``local_base``) to the same places under
        ``remote_base``. No-op for ``LocalStager``."""

    def pull(self, remote_base, local_base, paths, *, exclude=()):
        """Copy ``paths`` (relative to ``remote_base``) back under
        ``local_base``. No-op for ``LocalStager``."""


class LocalStager(JobStager):
    """No-op: the JobServer already shares storage with the SLURM submit
    host (every case validated so far), so there's nothing to move."""

    def stage_in(self, local_wdir, remote_wdir):
        return local_wdir

    def stage_out(self, remote_wdir, local_wdir):
        pass


class RsyncStager(JobStager):
    """Pushes/pulls a job's working directory to/from a remote host with no
    shared filesystem, over ``rsync -e ssh``. Assumes the same
    passwordless SSH access ``SshSlurm`` does.

    A job's working directory is already self-contained by the time a
    JobServer picks it up: ``flowchart.flow``, the ``job_data.json`` stub,
    and a ``data/`` directory holding every file the flowchart references
    -- SEAMM's existing ``type: "file"`` control-parameter mechanism
    (``seamm_dashboard_client.dashboard.Dashboard.submit()`` +
    ``safe_filename()``) already stages those into ``data/`` and rewrites
    the command line to ``job:data/...`` at submission time, before this
    ever runs. So staging is just "copy the whole directory," not a
    general arbitrary-path resolver.

    Known limitation, not handled here: a ``job://<n>/...`` cross-job file
    reference (e.g. a restart checkpoint from another job) points outside
    the referencing job's own working directory, so it isn't staged --
    such a reference in a job routed through this stager will fail to
    resolve on the remote host.
    """

    def __init__(self, host, *, ssh_command="ssh", rsync_command="rsync"):
        self.host = host
        self.ssh_command = ssh_command
        self.rsync_command = rsync_command

    def stage_in(self, local_wdir, remote_wdir):
        self._run_ssh(["mkdir", "-p", str(remote_wdir)])
        self._rsync(f"{local_wdir}/", f"{self.host}:{remote_wdir}/")
        return remote_wdir

    def stage_out(self, remote_wdir, local_wdir):
        self._rsync(f"{self.host}:{remote_wdir}/", f"{local_wdir}/")

    def push(self, local_base, remote_base, paths, *, delete=False):
        """One ``rsync --files-from`` for many paths: a bundle's task
        directories, say, rather than one ssh connection each."""
        paths = [str(p) for p in paths]
        if not paths:
            return
        self._run_ssh(["mkdir", "-p", str(remote_base)])
        extra = ["--delete"] if delete else []
        self._rsync(
            f"{local_base}/",
            f"{self.host}:{remote_base}/",
            extra=["-r", "--files-from=-"] + extra,
            input_text="\n".join(paths) + "\n",
        )

    def pull(self, remote_base, local_base, paths, *, exclude=()):
        paths = [str(p) for p in paths]
        if not paths:
            return
        extra = ["-r", "--files-from=-"]
        for pattern in exclude:
            extra += ["--exclude", pattern]
        self._rsync(
            f"{self.host}:{remote_base}/",
            f"{local_base}/",
            extra=extra,
            input_text="\n".join(paths) + "\n",
        )

    def _run_ssh(self, argv):
        remote_cmd = " ".join(shlex.quote(a) for a in argv)
        proc = subprocess.run(
            [self.ssh_command, self.host, remote_cmd],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise StageError(
                f"{self.ssh_command} {self.host} {remote_cmd!r} failed "
                f"({proc.returncode}): {proc.stderr.strip()}"
            )

    def _rsync(self, src, dst, *, extra=(), input_text=None):
        argv = [self.rsync_command, "-e", self.ssh_command, "-a", *extra, src, dst]
        if input_text is None:
            proc = subprocess.run(argv, capture_output=True, text=True)
        else:
            proc = subprocess.run(
                argv, input=input_text, capture_output=True, text=True
            )
        if proc.returncode != 0:
            raise StageError(
                f"rsync {src} -> {dst} failed ({proc.returncode}): "
                f"{proc.stderr.strip()}"
            )
