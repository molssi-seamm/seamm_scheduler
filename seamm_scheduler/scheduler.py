# -*- coding: utf-8 -*-

"""The interface every queueing system implements, and the job status it reports.

A :class:`Scheduler` knows one queueing system's syntax and nothing else: how
to write its directives, which commands submit, poll and cancel, how to read
their output, and the names of the environment variables it sets inside a job.
It never runs anything itself. A :class:`~seamm_scheduler.backend.QueueBackend`
pairs a scheduler with a transport (local commands or ssh) to actually talk to
a cluster.

Adding a queueing system is one module with a ``Scheduler`` subclass, plus an
entry in :data:`SCHEDULERS`.
"""

from dataclasses import dataclass, field
from typing import Optional


class SchedulerError(RuntimeError):
    """Raised when a queueing-system command fails unexpectedly."""


class SubmitError(SchedulerError):
    """Raised when a job cannot be submitted."""


# The categories every scheduler classifies its own states into.
PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
CANCELLED = "cancelled"
FAILED = "failed"
UNKNOWN = "unknown"

_TERMINAL_CATEGORIES = {COMPLETED, CANCELLED, FAILED}

#: The category of a job -> the task layer's vocabulary
#: (queued | running | finished | failed | lost). A cancelled job is "lost"
#: to the task layer: something outside it removed the job, so the tasks in
#: it are worth resubmitting.
TASK_STATES = {
    PENDING: "queued",
    RUNNING: "running",
    COMPLETED: "finished",
    FAILED: "failed",
    CANCELLED: "lost",
    UNKNOWN: "running",
}


@dataclass
class JobStatus:
    """The status of one queued job, as last polled."""

    job_id: str
    state: str
    category: str
    exit_code: Optional[str] = None
    reason: Optional[str] = None
    raw: dict = field(default_factory=dict)

    @property
    def is_terminal(self):
        """Whether this job has finished (successfully or not) and is no
        longer pending or running."""
        return self.category in _TERMINAL_CATEGORIES

    @property
    def task_state(self):
        """The state in the task layer's vocabulary (see :data:`TASK_STATES`)."""
        return TASK_STATES.get(self.category, "running")


class Scheduler:
    """One queueing system's syntax: directives, commands and states.

    Subclasses set :attr:`name`, :attr:`directive_prefix` and
    :attr:`env_names`, and implement the ``*_cmd``/``parse_*`` methods.
    :meth:`poll` composes them; a scheduler whose status needs more than one
    command (SLURM: ``squeue`` then ``sacct``) overrides it.
    """

    #: "slurm", "pbs", ...
    name = None
    #: "#SBATCH", "#PBS", ...
    directive_prefix = None
    #: Set by :meth:`poll`: True when the queue could not be asked (a failed
    #: command, e.g. ssh could not connect), so a job missing from the answer
    #: may still exist. Callers must not take "missing" as "gone" then.
    poll_failed = False
    #: The scheduler-neutral names -> the environment variables a job sees,
    #: e.g. ``{"job_id": "SLURM_JOB_ID", "ntasks": "SLURM_NTASKS"}``. Used by
    #: ``seamm_exec.computational_environment()`` to recognize and read an
    #: allocation.
    env_names = {}

    # ------------------------------------------------------------------
    # Directives
    # ------------------------------------------------------------------
    def directives(self, resources, extra=None):
        """Translate scheduler-neutral resources into this scheduler's
        directive dict (the form :meth:`directive_lines` and
        ``seamm_scheduler.script.build_script`` take).

        Parameters
        ----------
        resources : object or dict
            Anything with the attributes (or keys) ``ntasks``,
            ``cpus_per_task``, ``mem_per_cpu`` (bytes), ``ngpus``, ``walltime``
            (seconds), ``partition``, ``account``, ``qos`` and ``nodes``, e.g.
            ``seamm_exec.Resources``. Missing or None values are left out.
        extra : dict, optional
            Directives in this scheduler's own spelling (a target section's
            site defaults), applied first; the resources override them.

        Returns
        -------
        dict
        """
        raise NotImplementedError

    def directive_lines(self, directives):
        """The script's directive lines, e.g. ``["#SBATCH --ntasks=4"]``."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------
    def submit_cmd(self, script_path=None, *, job_name=None):
        """The command that submits a script. With ``script_path`` None the
        script is fed on the command's standard input."""
        raise NotImplementedError

    def parse_submit(self, stdout):
        """The job id from the submit command's output."""
        raise NotImplementedError

    def status_cmd(self, ids):
        """The command that reports the state of ``ids``."""
        raise NotImplementedError

    def parse_status(self, stdout, ids):
        """``{id: JobStatus}`` from the status command's output. An id the
        output does not mention is left out."""
        raise NotImplementedError

    def cancel_cmd(self, ids):
        """The command that cancels ``ids``."""
        raise NotImplementedError

    def log_directives(self, directory):
        """Directives that put the job's own output in ``directory``."""
        return {}

    def find_cmd(self, job_name):
        """A command printing the ids of the user's queued or running jobs
        called ``job_name``, one per line, or None if the scheduler cannot."""
        return None

    def find(self, run, job_name):
        """The ids of the user's jobs called ``job_name``, queued, running or
        (where the scheduler remembers them) finished; None if that cannot be
        known now."""
        argv = self.find_cmd(job_name)
        if argv is None:
            return None
        rc, out, err = run(argv)
        if rc != 0:
            return None
        return [line.split()[0] for line in out.splitlines() if line.strip()]

    def count_cmd(self):
        """A command listing the user's own jobs, one per line, or None if the
        scheduler cannot. Used to respect per-user queued-job limits."""
        return None

    # ------------------------------------------------------------------
    # Composition
    # ------------------------------------------------------------------
    def poll(self, run, ids):
        """The status of ``ids``, using ``run(argv) -> (rc, out, err)``.

        Returns
        -------
        {str: JobStatus}
            An id the scheduler has no record of is absent.
        """
        ids = [str(i) for i in ids]
        if not ids:
            return {}
        rc, out, err = run(self.status_cmd(ids))
        self.poll_failed = rc != 0 and not out.strip()
        if self.poll_failed:
            return {}
        return self.parse_status(out, ids)

    def classify(self, raw_state):
        """This scheduler's state string -> one of the categories above."""
        raise NotImplementedError


def _get(resources, name):
    """An attribute or key of ``resources``, or None."""
    if resources is None:
        return None
    if isinstance(resources, dict):
        return resources.get(name)
    return getattr(resources, name, None)


def format_walltime(seconds):
    """Seconds -> ``HH:MM:SS`` (hours may exceed 24), the form SLURM and PBS
    both accept."""
    seconds = int(round(float(seconds)))
    hours, rest = divmod(seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_memory_mb(nbytes):
    """Bytes -> whole megabytes with an ``M`` suffix, rounded up."""
    mb = -(-int(nbytes) // (1024 * 1024))
    return f"{max(1, mb)}M"


def get_scheduler(name):
    """The scheduler called ``name`` ("slurm", "pbs")."""
    key = (name or "").strip().lower()
    if key not in SCHEDULERS:
        raise ValueError(
            f"Unknown scheduler '{name}' (expected one of {sorted(SCHEDULERS)})"
        )
    module, cls = SCHEDULERS[key]
    import importlib

    return getattr(importlib.import_module(module), cls)()


#: name -> (module, class)
SCHEDULERS = {
    "slurm": ("seamm_scheduler.slurm", "Slurm"),
    "pbs": ("seamm_scheduler.pbs", "Pbs"),
}
