# -*- coding: utf-8 -*-

"""The ``seamm`` scheduler: the TaskServer (:mod:`seamm_scheduler.taskserver`).

Driven like SLURM or PBS -- locally, or over ssh with rsync staging -- by the
task layer's ``SchedulerBackend`` and by the JobServer. Its commands run the
queue with the target machine's own Python (``python -m
seamm_scheduler.taskserver``), never a command found on the PATH.
"""

import json
import logging
import shlex
import sys

from .scheduler import (
    CANCELLED,
    COMPLETED,
    FAILED,
    PENDING,
    RUNNING,
    UNKNOWN,
    JobStatus,
    Scheduler,
    _get,
)

logger = logging.getLogger(__name__)

#: The queue's states -> the scheduler categories. A lost job (its runner
#: vanished, e.g. after a reboot) is "cancelled": something outside the job
#: removed it, so the task layer resubmits it.
_CATEGORIES = {
    "queued": PENDING,
    "starting": PENDING,
    "running": RUNNING,
    "completed": COMPLETED,
    "failed": FAILED,
    "timeout": FAILED,
    "memory": FAILED,
    "cancelled": CANCELLED,
    "lost": CANCELLED,
}


def classify(state):
    return _CATEGORIES.get((state or "").strip().lower(), UNKNOWN)


class Seamm(Scheduler):
    """The TaskServer on a machine without a queueing system."""

    name = "seamm"
    directive_prefix = "#SEAMM"
    env_names = {
        "job_id": "SEAMM_TASKSERVER_JOB_ID",
        "ntasks": "SEAMM_TASKSERVER_NTASKS",
        "memory": "SEAMM_TASKSERVER_MEMORY",
    }
    env_prefixes = ("SEAMM_TASKSERVER_",)

    def __init__(self, python=None, root=None):
        #: The Python that runs the queue on the target machine
        self.python = python or sys.executable
        #: The SEAMM root there (None: its default, ~/SEAMM)
        self.root = root

    # ------------------------------------------------------------------
    # Directives
    # ------------------------------------------------------------------
    def directives(self, resources, extra=None):
        """``cores``, ``memory`` (bytes), ``time`` (seconds) and the rest.

        ``extra`` may use the portable spellings of a target section (``ntasks``,
        ``cpus_per_task``, ``mem``, ``mem_per_cpu``, ``time``, ``job_name``,
        ``chdir``, ``output``); queue settings the TaskServer does not have
        (``partition``, ``account``, ``qos``, ``export``, ...) are dropped.
        """
        from .config import _parse_size, _parse_time

        extra = dict(extra or {})
        directives = {}
        for key in ("chdir", "output", "kind"):
            if extra.get(key) not in (None, ""):
                directives[key] = str(extra[key])
        name = extra.get("job_name") or extra.get("name")
        if name:
            directives["name"] = str(name)
        ntasks = _get(resources, "ntasks") or extra.get("ntasks")
        cpus_per_task = _get(resources, "cpus_per_task") or extra.get("cpus_per_task")
        cores = None
        if ntasks is not None or cpus_per_task is not None:
            cores = int(ntasks or 1) * int(cpus_per_task or 1)
        if extra.get("cores") not in (None, ""):
            cores = int(extra["cores"])
        if cores is not None:
            directives["cores"] = cores
        memory = None
        mem_per_cpu = _get(resources, "mem_per_cpu")
        if mem_per_cpu is None and extra.get("mem_per_cpu") not in (None, ""):
            mem_per_cpu = _parse_size(extra["mem_per_cpu"])
        if mem_per_cpu is not None:
            memory = int(mem_per_cpu) * int(cores or 1)
        if extra.get("mem") not in (None, ""):
            memory = int(_parse_size(extra["mem"]))
        if extra.get("memory") not in (None, ""):
            memory = int(_parse_size(extra["memory"]))
        if memory is not None:
            directives["memory"] = memory
        walltime = _get(resources, "walltime")
        if walltime is None:
            for key in ("time", "walltime"):
                if extra.get(key) not in (None, ""):
                    walltime = _parse_time(extra[key])
                    break
        if walltime:
            directives["time"] = int(round(float(walltime)))
        return directives

    def directive_lines(self, directives):
        lines = []
        for key in ("name", "kind", "cores", "memory", "time", "chdir", "output"):
            value = directives.get(key)
            if value in (None, ""):
                continue
            lines.append(f"#SEAMM --{key} {shlex.quote(str(value))}")
        return lines

    def log_directives(self, directory):
        return {"output": f"{directory}/taskserver.out"}

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------
    def _queue(self, *args):
        argv = [self.python, "-m", "seamm_scheduler.taskserver"]
        if self.root:
            argv += ["--root", str(self.root)]
        return argv + list(args)

    def submit_cmd(self, script_path=None, *, job_name=None):
        argv = self._queue("submit")
        if job_name:
            argv += ["--name", job_name]
        if script_path is not None:
            argv.append(str(script_path))
        return argv

    def parse_submit(self, stdout):
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        if not lines or not lines[-1].isdigit():
            raise ValueError(f"no job id in {stdout!r}")
        return lines[-1]

    def status_cmd(self, ids):
        return self._queue("status", "--json", *[str(i) for i in ids])

    def parse_status(self, stdout, ids):
        text = stdout.strip()
        start = text.find("[")
        if start < 0:
            return {}
        result = {}
        for record in json.loads(text[start:]):
            job_id = str(record["id"])
            state = record.get("state") or ""
            raw_state = "TIMEOUT" if state == "timeout" else state.upper()
            exit_code = record.get("exit_code")
            result[job_id] = JobStatus(
                job_id=job_id,
                state=raw_state,
                category=classify(state),
                exit_code=None if exit_code is None else str(exit_code),
                reason=record.get("reason"),
                raw=record,
            )
        return result

    def cancel_cmd(self, ids):
        return self._queue("cancel", *[str(i) for i in ids])

    def find_cmd(self, job_name):
        return self._queue("find", job_name)

    def count_cmd(self):
        return self._queue("count")

    def classify(self, raw_state):
        return classify(raw_state)
