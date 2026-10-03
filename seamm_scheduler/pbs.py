# -*- coding: utf-8 -*-

"""PBS (PBS Professional and OpenPBS): ``#PBS`` directives, ``qsub``/``qstat``/``qdel``.

This module implements the :class:`~seamm_scheduler.scheduler.Scheduler`
interface for a second queueing system. It was validated on a real OpenPBS
23.06 site (MolSSI10, 2026-10-03), whose recorded output is replayed by the
tests, alongside mocked ``qsub``/``qstat`` output.

A PBS job starts in the user's home directory, so ``chdir`` becomes a ``cd`` at
the top of the script. Dependencies and other job attributes are ``-W``
options (SLURM's ``dependency`` is accepted). Without ``-V`` only the
``PBS_O_*`` variables reach the job, as with SLURM's ``export=NONE``.

Resources become one ``select`` statement: ``nodes`` chunks, each with its
share of the MPI ranks (``mpiprocs``), cores (``ncpus``), threads, memory and
GPUs. ``partition`` is the PBS queue (``-q``). PBS has no QOS, so ``qos`` is
ignored.

Status prefers ``qstat -x -f -F json`` (PBS Pro 18 and later) and falls back to
the text table of ``qstat -x``. ``-x`` includes finished jobs, whose
``Exit_status`` decides between completed and failed.
"""

import json
import logging

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
    format_walltime,
)

logger = logging.getLogger("seamm_scheduler")

_PENDING_STATES = {"Q", "H", "W", "T", "M"}
_RUNNING_STATES = {"R", "E", "B", "S", "U"}
# F: finished (PBS Pro, with -x); X: a finished subjob; C: completed (Torque)
_FINISHED_STATES = {"F", "X", "C"}

# Exit_status values PBS uses for jobs it removed itself: a qdel of a running
# job gives 271 (256 + SIGTERM); negative values are PBS's own failures to run
# the job (e.g. -1 JOB_EXEC_FAIL1, -11 JOB_EXEC_RERUN ...).
_CANCELLED_EXIT = {271}

# Job attributes qsub takes with -W rather than as resources (-l).
_W_ATTRIBUTES = (
    "depend",
    "group_list",
    "umask",
    "sandbox",
    "block",
    "stagein",
    "stageout",
    "run_count",
)

# Directive keys -> qsub option. Anything else becomes "-l key=value".
_DIRECTIVE_FLAGS = {
    "job_name": "-N",
    "queue": "-q",
    "account": "-A",
    "output": "-o",
    "error": "-e",
    "join": "-j",
}


def classify(raw_state, exit_status=None):
    """A PBS ``job_state`` letter -> one of this library's categories."""
    state = (raw_state or "").strip()[:1].upper()
    if state in _PENDING_STATES:
        return PENDING
    if state in _RUNNING_STATES:
        return RUNNING
    if state in _FINISHED_STATES:
        if exit_status is None or str(exit_status).strip() == "":
            # F without an exit status: deleted before it ran.
            return COMPLETED if state == "C" else CANCELLED
        try:
            code = int(str(exit_status).strip())
        except ValueError:
            return UNKNOWN
        if code == 0:
            return COMPLETED
        if code in _CANCELLED_EXIT:
            return CANCELLED
        return FAILED
    return UNKNOWN


class Pbs(Scheduler):
    """PBS Professional / OpenPBS."""

    name = "pbs"
    directive_prefix = "#PBS"
    env_names = {
        "job_id": "PBS_JOBID",
        "ncpus": "NCPUS",
        "nodefile": "PBS_NODEFILE",
        "ngpus": "NGPUS",
        "submit_dir": "PBS_O_WORKDIR",
        "threads": "OMP_NUM_THREADS",
    }
    env_prefixes = ("PBS_",)

    def __init__(self):
        self._qstat_json = None

    # ------------------------------------------------------------------
    # Directives
    # ------------------------------------------------------------------
    def directives(self, resources, extra=None):
        directives = dict(extra or {})
        # Portable spellings a target section may use
        if "partition" in directives:
            directives.setdefault("queue", directives.pop("partition"))
        if "time" in directives:
            directives.setdefault("walltime", directives.pop("time"))
        if "dependency" in directives:
            directives.setdefault("depend", directives.pop("dependency"))
        # SLURM's export=NONE is what PBS does without -V; ALL is -V.
        export = directives.pop("export", None)
        if export is not None and str(export).strip().upper() == "ALL":
            directives["export_all"] = True
        # SLURM spellings that are not PBS resources; resources set the select.
        for key in _SLURM_ONLY:
            if key in directives:
                logger.warning(f"PBS ignores the SLURM directive '{key}'")
                directives.pop(key)

        partition = _get(resources, "partition")
        if partition:
            directives["queue"] = partition
        account = _get(resources, "account")
        if account:
            directives["account"] = account
        walltime = _get(resources, "walltime")
        if walltime:
            directives["walltime"] = format_walltime(walltime)

        ntasks = _get(resources, "ntasks")
        cpus_per_task = int(_get(resources, "cpus_per_task") or 1)
        if ntasks is not None:
            nodes = int(_get(resources, "nodes") or 1)
            ranks = -(-int(ntasks) // nodes)  # per chunk, rounded up
            chunk = [
                f"ncpus={ranks * cpus_per_task}",
                f"mpiprocs={ranks}",
            ]
            if cpus_per_task > 1:
                chunk.append(f"ompthreads={cpus_per_task}")
            mem_per_cpu = _get(resources, "mem_per_cpu")
            if mem_per_cpu:
                mb = -(-int(mem_per_cpu) * ranks * cpus_per_task // (1024 * 1024))
                chunk.append(f"mem={mb}mb")
            ngpus = _get(resources, "ngpus")
            if ngpus:
                chunk.append(f"ngpus={-(-int(ngpus) // nodes)}")
            directives["select"] = f"{nodes}:" + ":".join(chunk)
        return directives

    def directive_lines(self, directives):
        lines = []
        # chdir becomes a cd in the prologue: PBS has no working-directory option.
        seen = {"chdir", "export_all"}
        if directives.get("export_all"):
            lines.append("#PBS -V")
        for key, flag in _DIRECTIVE_FLAGS.items():
            seen.add(key)
            value = directives.get(key)
            if value in (None, ""):
                continue
            lines.append(f"#PBS {flag} {value}")
        for key in _W_ATTRIBUTES:
            seen.add(key)
            value = directives.get(key)
            if value not in (None, ""):
                lines.append(f"#PBS -W {key}={value}")
        for key in ("select", "walltime"):
            seen.add(key)
            value = directives.get(key)
            if value not in (None, ""):
                lines.append(f"#PBS -l {key}={value}")
        for key, value in directives.items():
            if key in seen or value in (None, ""):
                continue
            lines.append(f"#PBS -l {key}={value}")
        return lines

    def prologue_lines(self, directives):
        # A PBS job starts in the user's home directory.
        chdir = directives.get("chdir")
        if chdir:
            return [f"cd {_sh_quote(str(chdir))}"]
        return []

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------
    def submit_cmd(self, script_path=None, *, job_name=None):
        argv = ["qsub"]
        if job_name:
            argv += ["-N", job_name]
        if script_path is not None:
            argv.append(str(script_path))
        return argv

    def parse_submit(self, stdout):
        # qsub prints "<sequence>.<server>"
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        if not lines:
            raise ValueError("no job id")
        return lines[-1].split()[0]

    def status_cmd(self, ids):
        return ["qstat", "-x"] + [str(i) for i in ids]

    def parse_status(self, stdout, ids):
        """The table of ``qstat -x``::

            Job id            Name             User              Time Use S Queue
            ----------------  ---------------- ----------------  -------- - -----
            1234.pbs01        seamm-task       psaxe             00:00:01 F workq

        The table has no exit status, so a finished job's category comes from
        ``qstat -x -f`` in :meth:`poll`.
        """
        result = {}
        for line in stdout.splitlines():
            parts = line.split()
            if len(parts) < 6 or parts[0].startswith("-") or parts[0] == "Job":
                continue
            job_id, state = parts[0], parts[4]
            result[_match_id(job_id, ids)] = JobStatus(
                job_id=_match_id(job_id, ids),
                state=state,
                category=classify(state),
            )
        return result

    def log_directives(self, directory):
        return {"join": "oe", "output": f"{directory}/pbs.out"}

    def find_cmd(self, job_name):
        # -x includes finished jobs (job history), so a bundle that finished while
        # the evaluator was away is still found.
        return ["sh", "-c", f'qselect -x -u "$USER" -N {_sh_quote(job_name)}']

    def count_cmd(self):
        # qselect lists the user's jobs that have not finished, one per line
        return ["sh", "-c", 'qselect -u "$USER"']

    def cancel_cmd(self, ids):
        return ["qdel"] + [str(i) for i in ids]

    def classify(self, raw_state):
        return classify(raw_state)

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------
    def poll(self, run, ids):
        ids = [str(i) for i in ids]
        self.poll_failed = False
        if not ids:
            return {}
        if self._qstat_json is not False:
            rc, out, err = run(["qstat", "-x", "-f", "-F", "json"] + ids)
            if out.strip().startswith("{"):
                # qstat exits nonzero when any id is unknown, but still reports
                # the rest.
                self._qstat_json = True
                try:
                    return self._parse_qstat_json(out, ids)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Could not parse qstat -F json output: {e}")
                    return {}
            if _is_unsupported(err):
                self._qstat_json = False
            elif rc != 0:
                self.poll_failed = True
                return {}

        rc, out, err = run(self.status_cmd(ids))
        if rc != 0 and not out.strip():
            self.poll_failed = True
            return {}
        result = self.parse_status(out, ids)
        # The table has no exit status; ask for the finished ones in full.
        finished = [
            i
            for i, s in result.items()
            if s.state[:1].upper() in ("F", "X") and s.exit_code is None
        ]
        if finished:
            rc, out, err = run(["qstat", "-x", "-f"] + finished)
            for job_id, status in self._parse_qstat_full(out, ids).items():
                if job_id in result:
                    result[job_id] = status
        return result

    @staticmethod
    def _parse_qstat_json(out, ids):
        data = json.loads(out)
        result = {}
        for job_id, job in (data.get("Jobs") or {}).items():
            state = job.get("job_state", "")
            exit_status = job.get("Exit_status")
            key = _match_id(job_id, ids)
            result[key] = JobStatus(
                job_id=key,
                state=state,
                category=classify(state, exit_status),
                exit_code=None if exit_status is None else str(exit_status),
                reason=job.get("comment"),
                raw=job,
            )
        return result

    @staticmethod
    def _parse_qstat_full(out, ids):
        """``qstat -f``'s ``Job Id: ...`` blocks of ``key = value`` lines."""
        result = {}
        job_id = None
        fields = {}

        def finish():
            if job_id is None:
                return
            key = _match_id(job_id, ids)
            state = fields.get("job_state", "")
            exit_status = fields.get("Exit_status")
            result[key] = JobStatus(
                job_id=key,
                state=state,
                category=classify(state, exit_status),
                exit_code=exit_status,
                reason=fields.get("comment"),
                raw=dict(fields),
            )

        for line in out.splitlines():
            if line.startswith("Job Id:"):
                finish()
                job_id = line.split(":", 1)[1].strip()
                fields = {}
            elif "=" in line and job_id is not None:
                key, value = line.split("=", 1)
                fields[key.strip()] = value.strip()
        finish()
        return result


_SLURM_ONLY = (
    "ntasks",
    "cpus_per_task",
    "nodes",
    "mem",
    "mem_per_cpu",
    "gpus",
    "constraint",
    "qos",
)


def _sh_quote(text):
    import shlex

    return shlex.quote(str(text))


def _match_id(job_id, ids):
    """The caller's spelling of ``job_id``: PBS may print ``1234.server`` for an
    id given as ``1234``, or truncate a long server name in the table."""
    if job_id in ids:
        return job_id
    sequence = job_id.split(".")[0]
    for i in ids:
        if i.split(".")[0] == sequence:
            return i
    return job_id


def _is_unsupported(stderr):
    stderr = stderr.lower()
    return (
        "invalid option" in stderr
        or "illegal option" in stderr
        or "unrecognized option" in stderr
        or "usage" in stderr
    )
