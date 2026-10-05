# -*- coding: utf-8 -*-

"""The TaskServer: a small queueing system for a machine that has none.

It shares one machine's cores and memory among the jobs of everything on it --
the tasks of several flowcharts' evaluators, and the evaluators themselves when
the JobServer submits them here -- so that two jobs on a laptop no longer
oversubscribe it. It is driven like SLURM or PBS, by commands, locally or over
ssh, through the ``seamm`` scheduler (:mod:`seamm_scheduler.seamm`)::

    python -m seamm_scheduler.taskserver [--root R] submit [--name N] [SCRIPT]
    python -m seamm_scheduler.taskserver [--root R] status [--json] [ID ...]
    python -m seamm_scheduler.taskserver [--root R] cancel ID ...
    python -m seamm_scheduler.taskserver [--root R] find NAME
    python -m seamm_scheduler.taskserver [--root R] count
    python -m seamm_scheduler.taskserver [--root R] queue
    python -m seamm_scheduler.taskserver [--root R] config

A script carries its request in ``#SEAMM`` lines, as a SLURM script does in
``#SBATCH`` lines: ``--cores``, ``--memory`` (bytes, or with a unit), ``--time``
(seconds, or ``HH:MM:SS``), ``--chdir``, ``--output``, ``--name`` and ``--kind``
(``task`` or ``evaluator``). An evaluator -- a flowchart's own process, light and
mostly waiting on its tasks -- is charged no cores and 1 GB, and tasks are
admitted against the tasks running alone, so evaluators can never hold the
cores or memory their tasks wait for; an evaluator itself starts only when
everything still fits. A job named ``seamm-<number>`` (the
JobServer's name for an evaluator) is an evaluator unless it says otherwise.

There is no daemon. The queue is one SQLite file, ``<root>/taskserver/queue.db``;
every ``submit`` and ``status`` call, and every job's runner when its job ends,
makes a scheduling pass that starts the queued jobs that fit, in order. A queued
job is claimed by one guarded ``UPDATE`` in a ``BEGIN IMMEDIATE`` transaction,
so concurrent passes never start a job twice. Each job runs under its own runner
(``run-job``) in a session of its own, so nothing that restarts or is upgraded
touches it. The runner enforces the job's time limit -- measured with
``time.monotonic``, which stops while the machine sleeps -- and its memory: the
job is stopped when its processes use more than 125 % of its request for 30 s,
and the newest job is stopped when the machine itself runs low on memory.

The machine's capacity is in ``<root>/taskserver.ini``::

    [taskserver]
    cores = 8
    memory = 8 GB

by default its physical cores and half its memory (the rest is for the desktop,
the evaluators and everything else).
"""

import argparse
import configparser
import json
import logging
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import time

logger = logging.getLogger("seamm-taskserver")

#: The version of the queue's database layout (2: the job's own pid)
SCHEMA_VERSION = 2

#: Job states. ``starting``: claimed, its runner not yet running the script.
QUEUED = "queued"
STARTING = "starting"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"
TIMEOUT = "timeout"
MEMORY = "memory"
LOST = "lost"
ACTIVE = (STARTING, RUNNING)
TERMINAL = (COMPLETED, FAILED, CANCELLED, TIMEOUT, MEMORY, LOST)

#: Charged to a job that asks for no memory, per core
DEFAULT_MEMORY_PER_CORE = 2 * 1024**3
#: Charged to an evaluator
EVALUATOR_MEMORY = 1024**3
#: The oldest queued job reserves room after waiting this long (seconds)
RESERVE_AFTER = 30 * 60
#: A job using more than this fraction of its memory request ...
MEMORY_LIMIT_FACTOR = 1.25
#: ... for this long (seconds) is stopped
MEMORY_GRACE = 30.0
#: Below this fraction of the machine's memory available, the newest job stops
MACHINE_MEMORY_FLOOR = 0.10
#: Seconds between SIGTERM and SIGKILL
KILL_GRACE = 10.0
#: A claimed job whose runner has not started after this long is lost
START_TIMEOUT = 120.0
#: Terminal jobs older than this are removed from the queue (seconds)
KEEP_FINISHED = 30 * 24 * 3600

EVALUATOR_NAME = re.compile(r"^seamm-\d+$")

#: The variables a job's script starts with (and LC_*); the script sets up the
#: rest itself, as a batch job does.
KEEP_ENVIRONMENT = {
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "TMPDIR",
    "TZ",
}


# ----------------------------------------------------------------------------
# Sizes and times
# ----------------------------------------------------------------------------


def parse_memory(value):
    """Bytes from ``"8 GB"``, ``"512M"``, ``"2gb"``, or a plain number of bytes."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().upper().replace(" ", "")
    if text == "":
        return None
    match = re.fullmatch(r"([0-9.]+)([KMGT]?)(I?B?)", text)
    if match is None:
        raise ValueError(f"Cannot read the memory '{value}'")
    number, unit, _ = match.groups()
    factor = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}[unit]
    return int(float(number) * factor)


def parse_time(value):
    """Seconds from ``3600``, ``"1:00:00"``, ``"1-00:00:00"`` or ``"90s"``."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    if text.endswith("s"):
        text = text[:-1]
    days = 0
    if "-" in text:
        d, text = text.split("-", 1)
        days = int(d)
    parts = [float(p) for p in text.split(":")]
    seconds = 0.0
    for p in parts:
        seconds = seconds * 60 + p
    return days * 86400 + seconds


def format_memory(nbytes):
    if nbytes is None:
        return "-"
    for unit, size in (("T", 1024**4), ("G", 1024**3), ("M", 1024**2)):
        if nbytes >= size:
            return f"{nbytes / size:.1f}{unit}"
    return f"{nbytes}B"


# ----------------------------------------------------------------------------
# The queue
# ----------------------------------------------------------------------------


def default_root():
    return Path(os.environ.get("SEAMM_ROOT", "~/SEAMM")).expanduser()


def settings(root):
    """The runner's limits from ``<root>/taskserver.ini``, with the defaults."""
    values = {
        "memory_factor": MEMORY_LIMIT_FACTOR,
        "memory_grace": MEMORY_GRACE,
        "memory_floor": MACHINE_MEMORY_FLOOR,
        "kill_grace": KILL_GRACE,
    }
    path = Path(root) / "taskserver.ini"
    if path.exists():
        config = configparser.ConfigParser()
        config.read(path)
        if "taskserver" in config:
            for key in values:
                if config["taskserver"].get(key, "").strip():
                    values[key] = float(config["taskserver"][key])
    return values


def capacity(root):
    """The machine's ``{"cores": int, "memory": bytes}`` for the queue."""
    import psutil

    cores = psutil.cpu_count(logical=False) or os.cpu_count() or 1
    memory = psutil.virtual_memory().total // 2
    path = Path(root) / "taskserver.ini"
    if path.exists():
        config = configparser.ConfigParser()
        config.read(path)
        if "taskserver" in config:
            section = config["taskserver"]
            if section.get("cores", "").strip():
                cores = int(section["cores"])
            if section.get("memory", "").strip():
                memory = parse_memory(section["memory"])
    return {"cores": int(cores), "memory": int(memory)}


class Queue:
    """The queue in ``<root>/taskserver/queue.db``."""

    def __init__(self, root=None):
        self.root = Path(root).expanduser() if root is not None else default_root()
        self.directory = self.root / "taskserver"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "queue.db"
        self.db = sqlite3.connect(str(self.path), timeout=60.0, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=60000")
        self._migrate()

    def close(self):
        self.db.close()

    # -- schema -------------------------------------------------------------
    def _migrate(self):
        db = self.db
        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute(
                "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)"
            )
            row = db.execute("SELECT value FROM meta WHERE key = 'schema'").fetchone()
            version = int(row[0]) if row else 0
            if version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"The queue {self.path} has layout {version}, newer than this "
                    f"version of seamm_scheduler understands ({SCHEMA_VERSION})."
                )
            if version < 1:
                db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        kind TEXT NOT NULL DEFAULT 'task',
                        script TEXT NOT NULL,
                        chdir TEXT,
                        output TEXT,
                        cores INTEGER NOT NULL,
                        memory INTEGER NOT NULL,
                        time_limit REAL,
                        state TEXT NOT NULL,
                        submitted REAL NOT NULL,
                        started REAL,
                        ended REAL,
                        pid INTEGER,
                        pid_start REAL,
                        exit_code INTEGER,
                        reason TEXT,
                        cancel INTEGER NOT NULL DEFAULT 0
                    )""")
                db.execute("CREATE INDEX IF NOT EXISTS jobs_state ON jobs (state)")
                db.execute("CREATE INDEX IF NOT EXISTS jobs_name ON jobs (name)")
            if version < 2:
                # The job's own process (the runner's child), so a job whose
                # runner vanished can be stopped, not left running
                columns = {r[1] for r in db.execute("PRAGMA table_info(jobs)")}
                if "job_pid" not in columns:
                    db.execute("ALTER TABLE jobs ADD COLUMN job_pid INTEGER")
                if "job_start" not in columns:
                    db.execute("ALTER TABLE jobs ADD COLUMN job_start REAL")
            db.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?)",
                (str(SCHEMA_VERSION),),
            )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

    # -- jobs ---------------------------------------------------------------
    def charge(self, row):
        """What a job takes from the machine while it runs: (cores, memory).

        An evaluator: no cores, and 1 GB (a quarter of the memory of a queue with
        less than 4 GB, so evaluators can still run there).
        """
        if row["kind"] == "evaluator":
            return 0, min(EVALUATOR_MEMORY, capacity(self.root)["memory"] // 4)
        return row["cores"], row["memory"]

    def submit(
        self,
        script,
        name=None,
        cores=None,
        memory=None,
        time_limit=None,
        chdir=None,
        output=None,
        kind=None,
    ):
        """Add a job; return its id. The request is checked against the machine."""
        request = parse_directives(script)
        name = name or request.get("name")
        cores = int(cores or request.get("cores") or 1)
        memory = parse_memory(memory or request.get("memory"))
        time_limit = parse_time(time_limit or request.get("time"))
        chdir = chdir or request.get("chdir")
        output = output or request.get("output")
        kind = kind or request.get("kind")
        if kind is None:
            kind = "evaluator" if name and EVALUATOR_NAME.match(name) else "task"
        if kind not in ("task", "evaluator"):
            raise ValueError(f"Unknown kind of job '{kind}' (task or evaluator)")
        if memory is None:
            memory = DEFAULT_MEMORY_PER_CORE * cores
        limits = capacity(self.root)
        if kind == "task":
            if cores > limits["cores"]:
                raise ValueError(
                    f"The job asks for {cores} cores; this machine's queue has "
                    f"{limits['cores']} (see {self.root / 'taskserver.ini'})."
                )
            if memory > limits["memory"]:
                raise ValueError(
                    f"The job asks for {format_memory(memory)} of memory; this "
                    f"machine's queue has {format_memory(limits['memory'])} (see "
                    f"{self.root / 'taskserver.ini'})."
                )
        cursor = self.db.execute(
            "INSERT INTO jobs (name, kind, script, chdir, output, cores, memory,"
            " time_limit, state, submitted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                name,
                kind,
                script,
                chdir,
                output,
                cores,
                memory,
                time_limit,
                QUEUED,
                time.time(),
            ),
        )
        return cursor.lastrowid

    def get(self, job_id):
        return self.db.execute(
            "SELECT * FROM jobs WHERE id = ?", (int(job_id),)
        ).fetchone()

    def jobs(self, ids=None):
        if ids:
            marks = ",".join("?" * len(ids))
            return self.db.execute(
                f"SELECT * FROM jobs WHERE id IN ({marks}) ORDER BY id",
                [int(i) for i in ids],
            ).fetchall()
        return self.db.execute("SELECT * FROM jobs ORDER BY id").fetchall()

    def finish(self, job_id, state, exit_code=None, reason=None):
        """Record a job's end, unless it has already ended."""
        self.db.execute(
            "UPDATE jobs SET state = ?, exit_code = ?, reason = ?, ended = ?"
            f" WHERE id = ? AND state NOT IN ({','.join('?' * len(TERMINAL))})",
            (state, exit_code, reason, time.time(), int(job_id), *TERMINAL),
        )

    def cancel(self, job_id):
        """Cancel a job: a queued one at once, a running one through its runner."""
        db = self.db
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute(
                "SELECT * FROM jobs WHERE id = ?", (int(job_id),)
            ).fetchone()
            if row is None:
                db.execute("COMMIT")
                return False
            if row["state"] == QUEUED:
                db.execute(
                    "UPDATE jobs SET state = ?, ended = ?, reason = 'cancelled'"
                    " WHERE id = ? AND state = ?",
                    (CANCELLED, time.time(), row["id"], QUEUED),
                )
            elif row["state"] in ACTIVE:
                db.execute("UPDATE jobs SET cancel = 1 WHERE id = ?", (row["id"],))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        if row["state"] in ACTIVE and row["pid"]:
            # The runner stops the job; if the runner has gone, stop it here.
            if not _alive(row["pid"], row["pid_start"]):
                self._stop_orphan(row)
                self.finish(row["id"], CANCELLED, reason="cancelled")
        return True

    def _stop_orphan(self, row):
        """Stop a job whose runner has gone, if it still runs."""
        if row["job_pid"] and _alive(row["job_pid"], row["job_start"]):
            _kill_session(row["job_pid"])

    # -- checking and scheduling ---------------------------------------------
    def check(self):
        """Mark jobs whose runner has vanished as lost (a sleeping one is not)."""
        now = time.time()
        for row in self.db.execute(
            f"SELECT * FROM jobs WHERE state IN ({','.join('?' * len(ACTIVE))})", ACTIVE
        ).fetchall():
            if row["state"] == STARTING and row["pid"] is None:
                if now - (row["started"] or now) > START_TIMEOUT:
                    # Only if its runner has still not claimed it: a slow runner
                    # may be starting the script right now.
                    self.db.execute(
                        "UPDATE jobs SET state = ?, ended = ?, reason = ?"
                        " WHERE id = ? AND state = ? AND pid IS NULL",
                        (LOST, now, "its runner never started", row["id"], STARTING),
                    )
                continue
            if row["pid"] is not None and not _alive(row["pid"], row["pid_start"]):
                # Its job runs in a session of its own: stop it, or a rerun of
                # the task would run beside it.
                self._stop_orphan(row)
                self.finish(row["id"], LOST, reason="its runner is no longer running")
        # Forget long-finished jobs
        self.db.execute(
            f"DELETE FROM jobs WHERE state IN ({','.join('?' * len(TERMINAL))})"
            " AND ended < ?",
            (*TERMINAL, now - KEEP_FINISHED),
        )

    def schedule(self, start=True):
        """Claim the queued jobs that fit, in order, and start their runners.

        Returns the ids started.
        """
        limits = capacity(self.root)
        db = self.db
        claimed = []
        db.execute("BEGIN IMMEDIATE")
        try:
            # Tasks are admitted against the tasks running; evaluators only when
            # everything still fits. So evaluators never hold what their tasks
            # wait for, and cannot pile up beyond the machine either.
            task_cores = task_memory = evaluator_memory = 0
            for row in db.execute(
                f"SELECT * FROM jobs WHERE state IN ({','.join('?' * len(ACTIVE))})",
                ACTIVE,
            ).fetchall():
                c, m = self.charge(row)
                if row["kind"] == "evaluator":
                    evaluator_memory += m
                else:
                    task_cores += c
                    task_memory += m
            now = time.time()
            queued = db.execute(
                "SELECT * FROM jobs WHERE state = ? ORDER BY id", (QUEUED,)
            ).fetchall()
            # The oldest task that has waited long enough reserves the task pool:
            # no later task jumps it. Evaluators never reserve -- one waiting for
            # memory the running evaluators' tasks will free would otherwise stop
            # those tasks, and itself, for good.
            reserved = False
            for row in queued:
                c, m = self.charge(row)
                if row["kind"] == "task" and (
                    c > limits["cores"] or m > limits["memory"]
                ):
                    # Larger than the machine's queue now (its capacity lowered)
                    db.execute(
                        "UPDATE jobs SET state = ?, ended = ?, reason = ?"
                        " WHERE id = ? AND state = ?",
                        (
                            FAILED,
                            now,
                            "larger than this machine's queue: "
                            f"{limits['cores']} cores, "
                            f"{format_memory(limits['memory'])}",
                            row["id"],
                            QUEUED,
                        ),
                    )
                    continue
                if row["kind"] == "evaluator":
                    fits = task_memory + evaluator_memory + m <= limits["memory"]
                else:
                    if reserved:
                        continue
                    fits = (
                        task_cores + c <= limits["cores"]
                        and task_memory + m <= limits["memory"]
                    )
                if not fits:
                    if row["kind"] == "task" and now - row["submitted"] > RESERVE_AFTER:
                        reserved = True
                    continue
                cursor = db.execute(
                    "UPDATE jobs SET state = ?, started = ? WHERE id = ? AND state = ?",
                    (STARTING, now, row["id"], QUEUED),
                )
                if cursor.rowcount == 1:
                    claimed.append(row["id"])
                    if row["kind"] == "evaluator":
                        evaluator_memory += m
                    else:
                        task_cores += c
                        task_memory += m
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        if start:
            for job_id in claimed:
                self._start_runner(job_id)
        return claimed

    def _start_runner(self, job_id):
        directory = self.directory / "jobs" / str(job_id)
        directory.mkdir(parents=True, exist_ok=True)
        log = open(directory / "runner.log", "ab")
        try:
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "seamm_scheduler.taskserver",
                    "--root",
                    str(self.root),
                    "run-job",
                    str(job_id),
                ],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        except Exception as e:
            self.finish(job_id, FAILED, reason=f"could not start its runner: {e}")
        finally:
            log.close()


# ----------------------------------------------------------------------------
# The runner of one job
# ----------------------------------------------------------------------------


def run_job(root, job_id, poll=2.0):
    """Run job ``job_id`` to its end: time and memory limits, cancellation."""
    import psutil

    queue = Queue(root)
    job_id = int(job_id)
    me = psutil.Process()
    cursor = queue.db.execute(
        "UPDATE jobs SET state = ?, pid = ?, pid_start = ? WHERE id = ? AND state = ?",
        (RUNNING, os.getpid(), me.create_time(), job_id, STARTING),
    )
    if cursor.rowcount != 1:
        return  # cancelled or claimed twice: not ours to run
    row = queue.get(job_id)
    directory = queue.directory / "jobs" / str(job_id)
    directory.mkdir(parents=True, exist_ok=True)
    script = directory / "script.sh"
    script.write_text(row["script"])
    script.chmod(0o700)
    chdir = row["chdir"] or str(Path.home())
    output = row["output"] or str(directory / "output.txt")
    # A minimal environment, as SLURM's export=NONE: the runner was started by
    # whoever made the scheduling pass (any evaluator), whose own variables
    # (another job's share, thread counts, ids) must not leak into this job.
    env = {
        k: v
        for k, v in os.environ.items()
        if k in KEEP_ENVIRONMENT or k.startswith("LC_")
    }
    env.update(
        {
            "SEAMM_TASKSERVER_JOB_ID": str(job_id),
            "SEAMM_TASKSERVER_NTASKS": str(row["cores"]),
            "SEAMM_TASKSERVER_MEMORY": str(row["memory"]),
            "SEAMM_TASKSERVER_ROOT": str(queue.root),
        }
    )
    state, exit_code, reason = FAILED, None, None
    try:
        with open(output, "ab") as out:
            job = subprocess.Popen(
                ["/bin/bash", str(script)],
                cwd=chdir,
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
    except Exception as e:
        queue.finish(job_id, FAILED, reason=f"could not start the script: {e}")
        queue.schedule()
        return
    try:
        job_start = psutil.Process(job.pid).create_time()
    except psutil.Error:
        job_start = None
    queue.db.execute(
        "UPDATE jobs SET job_pid = ?, job_start = ? WHERE id = ?",
        (job.pid, job_start, job_id),
    )
    t0 = time.monotonic()  # stops while the machine sleeps
    over_since = None
    limits = settings(queue.root)
    limit = row["memory"] * limits["memory_factor"]
    last_memory_check = 0.0
    try:
        while True:
            try:
                exit_code = job.wait(timeout=poll)
                state = COMPLETED if exit_code == 0 else FAILED
                if exit_code != 0:
                    reason = f"exit code {exit_code}"
                break
            except subprocess.TimeoutExpired:
                pass
            current = queue.get(job_id)
            if current is not None and current["cancel"]:
                _stop(job, limits["kill_grace"])
                state, reason = CANCELLED, "cancelled"
                break
            if row["time_limit"] and time.monotonic() - t0 > row["time_limit"]:
                _stop(job, limits["kill_grace"])
                state, reason = TIMEOUT, f"time limit of {row['time_limit']:.0f} s"
                break
            now = time.monotonic()
            interval = min(5.0, max(1.0, limits["memory_grace"] / 3))
            if now - last_memory_check < interval:
                continue
            last_memory_check = now
            used = _job_memory(job.pid)
            if row["kind"] == "task" and used > limit:
                over_since = over_since if over_since is not None else now
                if now - over_since > limits["memory_grace"]:
                    _stop(job, limits["kill_grace"])
                    state = MEMORY
                    reason = (
                        f"used {format_memory(used)}, more than its request of "
                        f"{format_memory(row['memory'])}"
                    )
                    break
            else:
                over_since = None
            if _machine_low(queue, job_id, limits):
                _stop(job, limits["kill_grace"])
                state = MEMORY
                reason = (
                    "the machine was low on memory for "
                    f"{limits['memory_grace']:.0f} s; job {job_id}, the newest, "
                    "was stopped"
                )
                break
    except Exception as e:
        # Never leave the job running with its row 'running' (a database busy
        # past its timeout, say): stop it and record why.
        _stop(job, limits["kill_grace"])
        state, reason = FAILED, f"its runner failed: {e}"
    finally:
        if exit_code is None:
            exit_code = job.returncode
        try:
            queue.finish(job_id, state, exit_code=exit_code, reason=reason)
            queue.schedule()
        except Exception as e:
            logger.error(f"Could not record the end of job {job_id}: {e}")


def _machine_low(queue, job_id, limits):
    """Whether this job should go because the machine is low on memory.

    The machine must have been below the floor for the grace period, this must
    be the newest task, and only one job is stopped in a low-memory episode (it
    ends when the machine has memory again), so a passing dip or a process
    outside the queue does not walk through the queue stopping task after task.
    """
    import psutil

    vm = psutil.virtual_memory()
    low = vm.available < limits["memory_floor"] * vm.total
    db = queue.db
    now = time.time()
    if not low:
        # The common case, read-only unless an episode has just ended
        episode = db.execute(
            "SELECT count(*) FROM meta WHERE key IN ('low_since', 'low_killed')"
        ).fetchone()[0]
        if episode:
            db.execute("DELETE FROM meta WHERE key IN ('low_since', 'low_killed')")
        return False
    db.execute("BEGIN IMMEDIATE")
    try:
        meta = dict(
            db.execute(
                "SELECT key, value FROM meta WHERE key IN ('low_since', 'low_killed')"
            ).fetchall()
        )
        if "low_since" not in meta:
            db.execute("INSERT INTO meta VALUES ('low_since', ?)", (str(now),))
            db.execute("COMMIT")
            return False
        stop = (
            "low_killed" not in meta
            and now - float(meta["low_since"]) > limits["memory_grace"]
            and _newest(queue, job_id)
        )
        if stop:
            db.execute(
                "INSERT OR REPLACE INTO meta VALUES ('low_killed', ?)", (str(job_id),)
            )
        db.execute("COMMIT")
        return stop
    except BaseException:
        db.execute("ROLLBACK")
        raise


def _newest(queue, job_id):
    row = queue.db.execute(
        "SELECT id FROM jobs WHERE state = ? AND kind = 'task'"
        " ORDER BY started DESC, id DESC LIMIT 1",
        (RUNNING,),
    ).fetchone()
    return row is not None and row["id"] == job_id


def _job_processes(pid):
    """The job's processes: its session or group, and every descendant -- also
    those that started sessions of their own (a pool's codes)."""
    import psutil

    found = {}
    try:
        root = psutil.Process(pid)
        found[root.pid] = root
        for child in root.children(recursive=True):
            found[child.pid] = child
    except psutil.Error:
        pass
    try:
        sid = os.getsid(pid)
    except OSError:
        sid = None
    if sid is not None:
        for process in psutil.process_iter(["pid"]):
            try:
                if os.getsid(process.pid) == sid:
                    found.setdefault(process.pid, process)
            except (psutil.Error, OSError):
                continue
    return list(found.values())


def _job_memory(pid):
    """The memory used by the job's processes, in bytes.

    The unique set size where psutil offers it (on macOS RSS misses compressed
    and swapped pages), else the resident size.
    """
    import psutil

    total = 0
    for process in _job_processes(pid):
        try:
            try:
                total += process.memory_full_info().uss
            except (psutil.AccessDenied, AttributeError):
                total += process.memory_info().rss
        except psutil.Error:
            continue
    return total


def _signal_all(processes, sig):
    for process in processes:
        try:
            process.send_signal(sig)
        except Exception:
            pass


def _stop(job, grace=KILL_GRACE):
    """SIGTERM the job and all its processes, then SIGKILL those still there."""
    import psutil

    processes = _job_processes(job.pid)
    try:
        os.killpg(job.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    _signal_all(processes, signal.SIGTERM)
    try:
        job.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        pass
    _, alive = psutil.wait_procs(processes, timeout=1.0)
    try:
        os.killpg(job.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    _signal_all(alive, signal.SIGKILL)
    try:
        job.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        pass


def _kill_session(pid, grace=KILL_GRACE):
    """Stop a job that is not our child (its runner has gone): all its
    processes, SIGTERM then SIGKILL."""
    import psutil

    processes = _job_processes(pid)
    _signal_all(processes, signal.SIGTERM)
    _, alive = psutil.wait_procs(processes, timeout=grace)
    _signal_all(alive, signal.SIGKILL)


def _alive(pid, start):
    """Whether process ``pid`` is the one that started at ``start``."""
    import psutil

    try:
        process = psutil.Process(int(pid))
        if start is not None and abs(process.create_time() - float(start)) > 1.0:
            return False
        return process.status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
        return False


# ----------------------------------------------------------------------------
# Scripts
# ----------------------------------------------------------------------------

_DIRECTIVE = re.compile(r"^#SEAMM\s+--([a-z-]+)(?:[=\s]+(.*?))?\s*$")


def parse_directives(script):
    """The request in a script's ``#SEAMM --key value`` lines."""
    request = {}
    for line in script.splitlines():
        match = _DIRECTIVE.match(line.strip())
        if match is not None:
            key, value = match.groups()
            request[key.replace("-", "_")] = (value or "").strip().strip("'\"")
    return request


# ----------------------------------------------------------------------------
# The command line
# ----------------------------------------------------------------------------


def _status_record(row):
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "kind": row["kind"],
        "state": row["state"],
        "exit_code": row["exit_code"],
        "reason": row["reason"],
        "cores": row["cores"],
        "memory": row["memory"],
        "submitted": row["submitted"],
        "started": row["started"],
        "ended": row["ended"],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="seamm-taskserver",
        description=(
            "A queue for this machine's cores and memory "
            "(see seamm_scheduler.taskserver)."
        ),
    )
    parser.add_argument("--root", default=None, help="The SEAMM root (default ~/SEAMM)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("submit", help="Queue a script (from a file or standard input)")
    p.add_argument("script", nargs="?")
    p.add_argument("--name")
    p.add_argument("--cores", type=int)
    p.add_argument("--memory")
    p.add_argument("--time")
    p.add_argument("--chdir")
    p.add_argument("--output")
    p.add_argument("--kind", choices=("task", "evaluator"))

    p = sub.add_parser("status", help="The state of jobs")
    p.add_argument("ids", nargs="*")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("cancel", help="Cancel jobs")
    p.add_argument("ids", nargs="+")

    p = sub.add_parser("find", help="The ids of the jobs with a name")
    p.add_argument("name")

    sub.add_parser("count", help="The ids of the jobs not yet finished")
    sub.add_parser("queue", help="What is queued and running")
    sub.add_parser("config", help="The machine's capacity for the queue")

    p = sub.add_parser("run-job", help=argparse.SUPPRESS)
    p.add_argument("id")

    options = parser.parse_args(argv)
    root = Path(options.root).expanduser() if options.root else default_root()

    if options.command == "run-job":
        run_job(root, options.id)
        return 0

    queue = Queue(root)
    try:
        if options.command == "submit":
            if options.script in (None, "-"):
                script = sys.stdin.read()
            else:
                script = Path(options.script).read_text()
            try:
                job_id = queue.submit(
                    script,
                    name=options.name,
                    cores=options.cores,
                    memory=options.memory,
                    time_limit=options.time,
                    chdir=options.chdir,
                    output=options.output,
                    kind=options.kind,
                )
            except ValueError as e:
                print(f"seamm-taskserver: {e}", file=sys.stderr)
                return 1
            queue.schedule()
            print(job_id)
        elif options.command == "status":
            queue.check()
            queue.schedule()
            rows = queue.jobs(options.ids)
            if options.json:
                print(json.dumps([_status_record(r) for r in rows]))
            else:
                for r in rows:
                    print(f"{r['id']} {r['state']} {r['exit_code']} {r['name'] or ''}")
        elif options.command == "cancel":
            for job_id in options.ids:
                queue.cancel(job_id)
            queue.schedule()
        elif options.command == "find":
            for (job_id,) in queue.db.execute(
                "SELECT id FROM jobs WHERE name = ? ORDER BY id", (options.name,)
            ):
                print(job_id)
        elif options.command == "count":
            queue.check()
            for (job_id,) in queue.db.execute(
                f"SELECT id FROM jobs WHERE state IN ({','.join('?' * 3)}) ORDER BY id",
                (QUEUED, STARTING, RUNNING),
            ):
                print(job_id)
        elif options.command == "queue":
            queue.check()
            limits = capacity(root)
            print(
                f"Capacity: {limits['cores']} cores, {format_memory(limits['memory'])}"
            )
            # What each job takes from the machine (an evaluator: no cores)
            print(f"{'id':>6} {'state':9} {'kind':9} {'cores':>5} {'memory':>8}  name")
            for r in queue.db.execute(
                "SELECT * FROM jobs WHERE state IN (?, ?, ?) ORDER BY id",
                (QUEUED, STARTING, RUNNING),
            ):
                cores, memory = queue.charge(r)
                print(
                    f"{r['id']:>6} {r['state']:9} {r['kind']:9} {cores:>5} "
                    f"{format_memory(memory):>8}  {r['name'] or ''}"
                )
        elif options.command == "config":
            limits = capacity(root)
            print(json.dumps({**limits, "root": str(root)}))
    finally:
        queue.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
