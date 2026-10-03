# -*- coding: utf-8 -*-

"""SLURM: ``#SBATCH`` directives, ``sbatch``/``squeue``/``sacct``/``scancel``.

Not every SLURM version supports ``squeue --json``/``sacct --json``: SLURM
21.08 has it, 20.11 (MolSSI10) does not. :class:`Slurm` probes for it once per
instance and falls back to the text formats. SLURM 25.11 (TinkerCliffs) nests
the exit code of ``sacct --json`` one level deeper; both shapes are read.
"""

import json
import logging

from .backend import QueueBackend
from .local import LocalTransport
from .scheduler import (
    CANCELLED,
    COMPLETED,
    FAILED,
    PENDING,
    RUNNING,
    UNKNOWN,
    JobStatus,
    Scheduler,
    SchedulerError,
    SubmitError,
    _get,
    format_memory_mb,
    format_walltime,
)
from .ssh import SshTransport

logger = logging.getLogger("seamm_scheduler")

# SLURM job states, classified into the categories this library exposes. Not
# exhaustive of every SLURM release's state list, but covers the states
# relevant to whether/how a job should be treated as done.
_PENDING_STATES = {
    "PENDING",
    "CONFIGURING",
    "REQUEUE_HOLD",
    "REQUEUE_FED",
    "REQUEUED",
    "RESIZING",
    "RESV_DEL_HOLD",
}
_RUNNING_STATES = {
    "RUNNING",
    "COMPLETING",
    "SUSPENDED",
    "STAGE_OUT",
    "SIGNALING",
    "STOPPED",
}
_COMPLETED_STATES = {"COMPLETED"}
_CANCELLED_STATES = {"CANCELLED"}
_FAILED_STATES = {
    "FAILED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
}


def classify(raw_state):
    """Classify a raw SLURM state string into one of this library's
    categories.

    Parameters
    ----------
    raw_state : str
        A SLURM job state, e.g. ``"RUNNING"``, ``"COMPLETED"``, or (as
        ``sacct`` sometimes reports for a cancelled job) ``"CANCELLED by
        1234"``.

    Returns
    -------
    str
        One of ``"pending"``, ``"running"``, ``"completed"``,
        ``"cancelled"``, ``"failed"``, or ``"unknown"``.
    """
    state = raw_state.split()[0].upper() if raw_state else ""
    if state in _PENDING_STATES:
        return PENDING
    if state in _RUNNING_STATES:
        return RUNNING
    if state in _COMPLETED_STATES:
        return COMPLETED
    if state in _CANCELLED_STATES:
        return CANCELLED
    if state in _FAILED_STATES:
        return FAILED
    return UNKNOWN


# Directive keys with a dedicated, human-friendly name (rather than the
# literal `--flag` spelling) and the #SBATCH flag they map to. Order here is
# the order they're emitted in, for reproducible script text.
_DIRECTIVE_FLAGS = {
    "job_name": "--job-name",
    "partition": "--partition",
    "account": "--account",
    "qos": "--qos",
    "nodes": "--nodes",
    "ntasks": "--ntasks",
    "cpus_per_task": "--cpus-per-task",
    "time": "--time",
    "mem": "--mem",
    "gpus": "--gpus",
    "output": "--output",
    "error": "--error",
    "chdir": "--chdir",
}


class Slurm(Scheduler):
    """The SLURM queueing system."""

    name = "slurm"
    directive_prefix = "#SBATCH"
    env_names = {
        "job_id": "SLURM_JOB_ID",
        "ntasks": "SLURM_NTASKS",
        "cpus_per_task": "SLURM_CPUS_PER_TASK",
        "nnodes": "SLURM_NNODES",
        "nodelist": "SLURM_NODELIST",
        "ntasks_per_node": "SLURM_NTASKS_PER_NODE",
        "mem_per_cpu": "SLURM_MEM_PER_CPU",
        "mem_per_node": "SLURM_MEM_PER_NODE",
        "gpus": "SLURM_JOB_GPUS",
        "submit_dir": "SLURM_SUBMIT_DIR",
    }
    #: The prefixes of every variable SLURM sets in a job.
    env_prefixes = ("SLURM_", "SBATCH_")

    def __init__(self):
        # Tri-state: None = not yet probed, True/False = known support.
        self._squeue_json = None
        self._sacct_json = None
        self._failed = False
        self._squeue_failed = False
        self._no_accounting = False

    # ------------------------------------------------------------------
    # Directives
    # ------------------------------------------------------------------
    def directives(self, resources, extra=None):
        directives = dict(extra or {})
        for name, key in (
            ("partition", "partition"),
            ("account", "account"),
            ("qos", "qos"),
            ("nodes", "nodes"),
            ("ntasks", "ntasks"),
            ("cpus_per_task", "cpus_per_task"),
        ):
            value = _get(resources, name)
            if value not in (None, ""):
                directives[key] = value
        mem_per_cpu = _get(resources, "mem_per_cpu")
        if mem_per_cpu:
            directives["mem_per_cpu"] = format_memory_mb(mem_per_cpu)
            # --mem and --mem-per-cpu are mutually exclusive.
            directives.pop("mem", None)
        walltime = _get(resources, "walltime")
        if walltime:
            directives["time"] = format_walltime(walltime)
        ngpus = _get(resources, "ngpus")
        if ngpus:
            directives["gpus"] = ngpus
        return directives

    def directive_lines(self, directives):
        """``#SBATCH --flag=value`` lines.

        Keys are the plain names in ``_DIRECTIVE_FLAGS`` (e.g. ``"partition"``,
        not ``"--partition"``). A blank or None value is skipped. Other keys
        pass through, underscores becoming dashes (``{"gres": "gpu:1"}`` ->
        ``#SBATCH --gres=gpu:1``), so a target section can carry options this
        module does not special-case.
        """
        lines = []
        seen = set()
        for key, flag in _DIRECTIVE_FLAGS.items():
            seen.add(key)
            value = directives.get(key)
            if value in (None, ""):
                continue
            lines.append(f"#SBATCH {flag}={value}")
        for key, value in directives.items():
            if key in seen or value in (None, ""):
                continue
            flag = "--" + key.replace("_", "-")
            lines.append(f"#SBATCH {flag}={value}")
        return lines

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------
    def submit_cmd(self, script_path=None, *, job_name=None):
        argv = ["sbatch", "--parsable"]
        if job_name:
            argv += ["--job-name", job_name]
        if script_path is not None:
            argv.append(str(script_path))
        return argv

    def parse_submit(self, stdout):
        # --parsable prints "<jobid>" or "<jobid>;<cluster>"
        lines = [line for line in stdout.strip().splitlines() if line.strip()]
        if not lines:
            raise ValueError("no job id")
        job_id = lines[-1].split(";")[0].strip()
        if not job_id:
            raise ValueError("no job id")
        return job_id

    def status_cmd(self, ids):
        return [
            "squeue",
            "--noheader",
            "--format=%i|%T|%R",
            "--jobs",
            ",".join(str(i) for i in ids),
        ]

    def parse_status(self, stdout, ids):
        return self._parse_squeue_text(stdout)

    def cancel_cmd(self, ids):
        return ["scancel"] + [str(i) for i in ids]

    def log_directives(self, directory):
        return {"output": f"{directory}/slurm-%j.out"}

    def find_cmd(self, job_name):
        return ["squeue", "--noheader", "--me", f"--name={job_name}", "--format=%i"]

    def count_cmd(self):
        # -r: one line per array element, since each counts against a QOS's
        # per-user job limit.
        return ["squeue", "--noheader", "--me", "-r", "--format=%i"]

    def classify(self, raw_state):
        return classify(raw_state)

    # ------------------------------------------------------------------
    # Polling: squeue for live jobs, then sacct for the ones it no longer lists
    # ------------------------------------------------------------------
    def poll(self, run, ids):
        ids = [str(i) for i in ids]
        self.poll_failed = False
        if not ids:
            return {}
        self._failed = False
        self._squeue_failed = False
        result = self._squeue(run, ids)
        missing = [j for j in ids if j not in result]
        if missing:
            if self._no_accounting:
                # Without accounting squeue is all there is; trust it unless
                # it failed.
                self.poll_failed = self._failed or self._squeue_failed
                return result
            self._failed = False
            result.update(self._sacct(run, missing))
            # sacct is the authority for jobs squeue no longer lists; if it
            # could not answer, a missing job may well still exist.
            self.poll_failed = self._failed or (
                self._no_accounting and self._squeue_failed
            )
        return result

    def _squeue(self, run, job_ids):
        ids = ",".join(job_ids)

        if self._squeue_json is not False:
            rc, out, err = run(["squeue", "--json", "--jobs", ids])
            if rc == 0:
                self._squeue_json = True
                try:
                    return self._parse_squeue_json(out)
                except (ValueError, KeyError, TypeError) as e:
                    # Never an empty, trusted answer: that would read as
                    # "every job is gone".
                    logger.warning(f"Could not parse squeue --json output: {e}")
                    self._failed = True
                    self._squeue_failed = True
                    return {}
            if _is_unrecognized_option(err):
                self._squeue_json = False
            else:
                # Transient/other failure (e.g. all the ids are already gone)
                # -- not a hard error, just nothing to report from squeue.
                return {}

        rc, out, err = run(self.status_cmd(job_ids))
        if rc != 0:
            if not _is_invalid_job_id(err):
                self._squeue_failed = True
            return {}
        return self._parse_squeue_text(out)

    @staticmethod
    def _parse_squeue_json(out):
        data = json.loads(out)
        result = {}
        for j in data.get("jobs", []):
            job_id = str(j["job_id"])
            state = j.get("job_state", "")
            if isinstance(state, list):
                # Some SLURM/OpenAPI versions report job_state as a list of
                # flags rather than a single string.
                state = state[0] if state else ""
            result[job_id] = JobStatus(
                job_id=job_id,
                state=state,
                category=classify(state),
                raw=j,
            )
        return result

    @staticmethod
    def _parse_squeue_text(out):
        result = {}
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 2:
                continue
            job_id, state = parts[0].strip(), parts[1].strip()
            reason = parts[2].strip() if len(parts) > 2 else None
            result[job_id] = JobStatus(
                job_id=job_id,
                state=state,
                category=classify(state),
                reason=reason,
            )
        return result

    def _sacct(self, run, job_ids):
        ids = ",".join(job_ids)

        if self._sacct_json is not False:
            rc, out, err = run(["sacct", "--json", "--jobs", ids])
            if rc == 0:
                self._sacct_json = True
                try:
                    return self._parse_sacct_json(out)
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"Could not parse sacct --json output: {e}")
                    self._failed = True
                    return {}
            if _is_unrecognized_option(err):
                self._sacct_json = False
            elif _no_accounting(err):
                self._no_accounting = True
                return {}
            else:
                self._failed = True
                return {}

        fmt = "JobID,State,ExitCode"
        rc, out, err = run(
            ["sacct", "--parsable2", "--noheader", f"--format={fmt}", "--jobs", ids]
        )
        if rc != 0:
            if _no_accounting(err):
                self._no_accounting = True
            else:
                self._failed = True
            return {}
        return self._parse_sacct_text(out)

    @staticmethod
    def _parse_sacct_json(out):
        data = json.loads(out)
        result = {}
        for j in data.get("jobs", []):
            job_id = str(j["job_id"])
            state_field = j.get("state", {})
            if isinstance(state_field, dict):
                state = state_field.get("current", "")
                reason = state_field.get("reason")
            else:
                # Fall back gracefully if a SLURM version reports state as a
                # plain string here too.
                state = state_field
                reason = None
            if isinstance(state, list):
                state = state[0] if state else ""
            exit_field = j.get("exit_code", {})
            exit_code = (
                exit_field.get("return_code")
                if isinstance(exit_field, dict)
                else exit_field
            )
            if isinstance(exit_code, dict):
                # SLURM 25.x: {"set": true, "infinite": false, "number": 0}
                exit_code = exit_code.get("number") if exit_code.get("set") else None
            result[job_id] = JobStatus(
                job_id=job_id,
                state=state,
                category=classify(state),
                exit_code=exit_code,
                reason=reason,
                raw=j,
            )
        return result

    @staticmethod
    def _parse_sacct_text(out):
        result = {}
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 2:
                continue
            job_id, state = parts[0].strip(), parts[1].strip()
            # Sub-step rows (e.g. "123.batch", "123.extern") describe pieces
            # of the job, not the job itself -- skip them.
            if "." in job_id:
                continue
            exit_code = parts[2].strip() if len(parts) > 2 else None
            result[job_id] = JobStatus(
                job_id=job_id,
                state=state,
                category=classify(state),
                exit_code=exit_code,
            )
        return result


def _is_invalid_job_id(stderr):
    """squeue's answer when none of the ids is in the queue any more."""
    return "invalid job id" in stderr.lower()


def _no_accounting(stderr):
    """sacct on a cluster without accounting storage."""
    return "accounting storage is disabled" in stderr.lower()


def _is_unrecognized_option(stderr):
    stderr = stderr.lower()
    return "unrecognized option" in stderr or "invalid option" in stderr


# ----------------------------------------------------------------------
# Backends with SLURM built in: the classes seamm_slurm has always offered.
# ----------------------------------------------------------------------
class SlurmError(SchedulerError):
    """Raised when a SLURM CLI command fails unexpectedly."""


class SlurmSubmitError(SlurmError, SubmitError):
    """Raised when ``sbatch`` fails to submit a job."""


class SlurmBackend(QueueBackend):
    """A :class:`QueueBackend` for SLURM. Subclasses either pass a transport or
    implement ``_run`` themselves."""

    def __init__(self, transport=None):
        super().__init__(Slurm(), transport)

    # The probe results live on the scheduler; kept visible here as before.
    @property
    def _squeue_json(self):
        return self.scheduler._squeue_json

    @property
    def _sacct_json(self):
        return self.scheduler._sacct_json

    def _error(self, message):
        return SlurmError(message)

    def _submit_error(self, message):
        return SlurmSubmitError(message)


class LocalSlurm(SlurmBackend):
    """SLURM's CLI directly on the current host -- the case where the caller
    runs on a SLURM submit host with those commands on ``PATH``."""

    def __init__(self, *, drop_env_prefixes=()):
        super().__init__(LocalTransport(drop_env_prefixes=drop_env_prefixes))


class SshSlurm(SlurmBackend):
    """SLURM's CLI on a remote host over passwordless ssh."""

    def __init__(self, host, *, ssh_command="ssh", ssh_options=(), timeout=None):
        super().__init__(
            SshTransport(
                host, ssh_command=ssh_command, ssh_options=ssh_options, timeout=timeout
            )
        )

    @property
    def host(self):
        return self.transport.host

    @property
    def ssh_command(self):
        return self.transport.ssh_command
