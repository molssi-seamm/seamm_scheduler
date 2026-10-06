# -*- coding: utf-8 -*-

"""The TaskServer: a queue for one machine's cores and memory."""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time

import pytest

from seamm_scheduler import taskserver as ts
from seamm_scheduler.seamm import Seamm, classify


def cli(root, *args, input_text=None):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "seamm_scheduler.taskserver",
            "--root",
            str(root),
            *args,
        ],
        input=input_text,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result


def setup_root(tmp_path, cores=2, memory="2 GB", **extra):
    root = tmp_path / "root"
    root.mkdir()
    lines = ["[taskserver]", f"cores = {cores}", f"memory = {memory}"]
    lines += [f"{k} = {v}" for k, v in extra.items()]
    (root / "taskserver.ini").write_text("\n".join(lines) + "\n")
    return root


def wait_for(root, ids, timeout=60):
    """Poll (as an evaluator does) until the jobs have ended; their records."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = cli(root, "status", "--json", *[str(i) for i in ids]).stdout
        records = {r["id"]: r for r in json.loads(out)}
        if all(records[str(i)]["state"] in ts.TERMINAL for i in ids):
            return records
        time.sleep(0.5)
    raise AssertionError(f"jobs {ids} did not end: {records}")


def wait_until_running(root, job, timeout=30):
    """Poll until the job is running (it may already have ended)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = cli(root, "status", "--json", str(job)).stdout
        (record,) = json.loads(out)
        if record["state"] == "running" or record["state"] in ts.TERMINAL:
            return record
        time.sleep(0.2)
    raise AssertionError(f"job {job} did not start: {record}")


def submit(root, script, *args):
    result = cli(root, "submit", *args, input_text=script)
    assert result.returncode == 0, result.stderr
    return int(result.stdout.strip())


def test_parsing():
    assert ts.parse_memory("8 GB") == 8 * 1024**3
    assert ts.parse_memory("512M") == 512 * 1024**2
    assert ts.parse_memory(1000) == 1000
    assert ts.parse_time("1:00:00") == 3600
    assert ts.parse_time("1-00:00:30") == 86430
    assert ts.parse_time("90s") == 90
    script = (
        "#!/bin/bash\n#SEAMM --cores 4\n#SEAMM --memory '1 GB'\n#SEAMM --name x\necho\n"
    )
    assert ts.parse_directives(script) == {"cores": "4", "memory": "1 GB", "name": "x"}


def test_a_job_runs(tmp_path):
    root = setup_root(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    job = submit(
        root,
        f"#!/bin/bash\n#SEAMM --chdir {work}\n#SEAMM --output {work}/out.txt\n"
        'echo "ran $SEAMM_TASKSERVER_NTASKS"\nexit 0\n',
    )
    record = wait_for(root, [job])[str(job)]
    assert record["state"] == "completed" and record["exit_code"] == 0
    assert (work / "out.txt").read_text().strip() == "ran 1"


def test_a_failing_job(tmp_path):
    root = setup_root(tmp_path)
    job = submit(root, "#!/bin/bash\nexit 3\n")
    record = wait_for(root, [job])[str(job)]
    assert record["state"] == "failed" and record["exit_code"] == 3


STAMP = f"{sys.executable} -c 'import time; print(time.time())'"


def test_cores_are_shared(tmp_path):
    """Three 2-core jobs on 2 cores run one at a time."""
    root = setup_root(tmp_path, cores=2)
    work = tmp_path / "work"
    work.mkdir()
    ids = [
        submit(
            root,
            f"#!/bin/bash\n#SEAMM --cores 2\n#SEAMM --memory 100M\n"
            f"{STAMP} > {work}/{k}.start\nsleep 1\n{STAMP} > {work}/{k}.end\n",
        )
        for k in range(3)
    ]
    wait_for(root, ids)
    spans = []
    for k in range(3):
        start = float((work / f"{k}.start").read_text())
        end = float((work / f"{k}.end").read_text())
        spans.append((start, end))
    spans.sort()
    for (_, end), (start, _) in zip(spans, spans[1:]):
        assert start >= end - 0.05, spans


def test_evaluators_cannot_deadlock_their_tasks(tmp_path):
    """Three evaluators on 2 cores, each waiting for its own 2-core task."""
    root = setup_root(tmp_path, cores=2)
    evaluator = textwrap.dedent(f"""\
        #!/bin/bash
        #SEAMM --cores 1
        {sys.executable} - <<'EOF'
        import json, subprocess, sys, time
        q = [{sys.executable!r}, "-m", "seamm_scheduler.taskserver",
             "--root", {str(root)!r}]
        out = subprocess.run(q + ["submit", "--cores", "2", "--memory", "100M"],
                             input="#!/bin/bash\\nsleep 1\\n", capture_output=True,
                             text=True).stdout
        job = out.strip()
        while True:
            s = json.loads(subprocess.run(q + ["status", "--json", job],
                           capture_output=True, text=True).stdout)[0]["state"]
            if s not in ("queued", "starting", "running"):
                sys.exit(0 if s == "completed" else 1)
            time.sleep(0.5)
        EOF
        """)
    ids = [submit(root, evaluator, "--name", f"seamm-{k}") for k in range(3)]
    records = wait_for(root, ids, timeout=90)
    assert all(records[str(i)]["state"] == "completed" for i in ids), records
    assert all(records[str(i)]["kind"] == "evaluator" for i in ids)


def test_too_large_is_refused(tmp_path):
    root = setup_root(tmp_path, cores=2, memory="1 GB")
    result = cli(root, "submit", "--cores", "4", input_text="#!/bin/bash\n")
    assert result.returncode == 1 and "4 cores" in result.stderr
    result = cli(root, "submit", "--memory", "2 GB", input_text="#!/bin/bash\n")
    assert result.returncode == 1 and "memory" in result.stderr


def test_cancel_queued_and_running(tmp_path):
    root = setup_root(tmp_path, cores=1, kill_grace=1)
    running = submit(root, "#!/bin/bash\nsleep 60\n")
    queued = submit(root, "#!/bin/bash\nsleep 60\n")
    deadline = time.time() + 30
    while time.time() < deadline:
        state = json.loads(cli(root, "status", "--json", str(running)).stdout)[0]
        if state["state"] == "running":
            break
        time.sleep(0.3)
    cli(root, "cancel", str(queued), str(running))
    records = wait_for(root, [running, queued])
    assert records[str(queued)]["state"] == "cancelled"
    assert records[str(running)]["state"] == "cancelled"


def test_time_limit(tmp_path):
    root = setup_root(tmp_path, kill_grace=1)
    job = submit(root, "#!/bin/bash\n#SEAMM --time 2\nsleep 60\n")
    record = wait_for(root, [job], timeout=30)[str(job)]
    assert record["state"] == "timeout"


def test_memory_limit(tmp_path):
    """A job using far more than its request is stopped."""
    root = setup_root(tmp_path, memory_grace=2, kill_grace=1)
    script = textwrap.dedent(f"""\
        #!/bin/bash
        #SEAMM --memory 50M
        {sys.executable} -c "
        import time
        block = bytearray(300 * 1024 * 1024)
        for i in range(0, len(block), 4096):
            block[i] = 1
        time.sleep(60)
        "
        """)
    job = submit(root, script)
    record = wait_for(root, [job], timeout=60)[str(job)]
    assert record["state"] == "memory", record


def test_a_vanished_runner_is_lost(tmp_path):
    root = setup_root(tmp_path)
    job = submit(root, "#!/bin/bash\nsleep 60\n")
    deadline = time.time() + 30
    pid = None
    while time.time() < deadline:
        q = ts.Queue(root)
        row = q.get(job)
        q.close()
        if row["state"] == "running" and row["pid"]:
            pid = row["pid"]
            break
        time.sleep(0.3)
    assert pid is not None
    q = ts.Queue(root)
    job_pid = q.get(job)["job_pid"]
    q.close()
    assert job_pid
    os.kill(pid, signal.SIGKILL)  # the runner, not the job
    record = wait_for(root, [job], timeout=30)[str(job)]
    assert record["state"] == "lost"
    # Its job, in a session of its own, was stopped too: a rerun of the task
    # must not run beside it
    deadline = time.time() + 10
    while time.time() < deadline and ts._alive(job_pid, None):
        time.sleep(0.2)
    assert not ts._alive(job_pid, None)


def test_a_version_1_queue_is_migrated(tmp_path):
    root = setup_root(tmp_path)
    directory = root / "taskserver"
    directory.mkdir()
    db = sqlite3.connect(str(directory / "queue.db"))
    db.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    db.execute("INSERT INTO meta VALUES ('schema', '1')")
    db.execute(
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT,"
        " kind TEXT NOT NULL DEFAULT 'task', script TEXT NOT NULL, chdir TEXT,"
        " output TEXT, cores INTEGER NOT NULL, memory INTEGER NOT NULL,"
        " time_limit REAL, state TEXT NOT NULL, submitted REAL NOT NULL,"
        " started REAL, ended REAL, pid INTEGER, pid_start REAL, exit_code INTEGER,"
        " reason TEXT, cancel INTEGER NOT NULL DEFAULT 0)"
    )
    db.execute(
        "INSERT INTO jobs (name, script, cores, memory, state, submitted)"
        " VALUES ('old', 'exit 0', 1, 1, 'completed', 0)"
    )
    db.commit()
    db.close()
    q = ts.Queue(root)
    assert q.get(1)["name"] == "old" and q.get(1)["job_pid"] is None
    version = q.db.execute("SELECT value FROM meta WHERE key = 'schema'").fetchone()
    assert int(version[0]) == ts.SCHEMA_VERSION
    q.close()


def test_concurrent_passes_start_each_job_once(tmp_path):
    root = setup_root(tmp_path, cores=4)
    q = ts.Queue(root)
    ids = [
        q.submit("#!/bin/bash\nexit 0\n", cores=1, memory=10 * 1024**2)
        for _ in range(12)
    ]
    q.close()
    # Many concurrent scheduling passes that only claim (no runners started)
    code = (
        "import sys; from seamm_scheduler import taskserver as ts;"
        f"q = ts.Queue({str(root)!r});"
        "print(' '.join(map(str, q.schedule(start=False))))"
    )
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", code], stdout=subprocess.PIPE, text=True
        )
        for _ in range(8)
    ]
    claimed = []
    for p in procs:
        out, _ = p.communicate(timeout=60)
        claimed += [int(x) for x in out.split()]
    assert len(claimed) == len(set(claimed)) == 4  # four cores, each job once
    assert set(claimed) <= set(ids)


def test_concurrent_submissions(tmp_path):
    root = setup_root(tmp_path, cores=1)
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "seamm_scheduler.taskserver",
                "--root",
                str(root),
                "submit",
                "--name",
                f"s{k}",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        for k in range(10)
    ]
    ids = []
    for p in procs:
        out, _ = p.communicate("#!/bin/bash\nexit 0\n", timeout=60)
        ids.append(int(out.strip()))
    assert len(set(ids)) == 10
    records = wait_for(root, ids, timeout=120)
    assert all(r["state"] == "completed" for r in records.values())


def test_newer_schema_is_refused(tmp_path):
    root = setup_root(tmp_path)
    ts.Queue(root).close()
    db = sqlite3.connect(str(root / "taskserver" / "queue.db"))
    db.execute("UPDATE meta SET value = '99' WHERE key = 'schema'")
    db.commit()
    db.close()
    with pytest.raises(RuntimeError, match="newer"):
        ts.Queue(root)


# --------------------------------------------------------------------------
# The seamm scheduler
# --------------------------------------------------------------------------


def test_scheduler_directives_and_commands():
    s = Seamm(python="/opt/py/bin/python", root="/home/u/SEAMM")
    d = s.directives(
        {"ntasks": 2, "cpus_per_task": 2, "mem_per_cpu": 1024**3, "walltime": 600},
        {"partition": "normal", "job_name": "seamm-bundle", "chdir": "/w"},
    )
    assert d == {
        "chdir": "/w",
        "name": "seamm-bundle",
        "cores": 4,
        "memory": 4 * 1024**3,
        "time": 600,
    }
    assert "#SEAMM --cores 4" in s.directive_lines(d)
    assert s.submit_cmd(job_name="x") == [
        "/opt/py/bin/python",
        "-m",
        "seamm_scheduler.taskserver",
        "--root",
        "/home/u/SEAMM",
        "submit",
        "--name",
        "x",
    ]
    out = json.dumps(
        [
            {"id": "3", "state": "timeout", "exit_code": -15, "reason": "time"},
            {"id": "4", "state": "lost", "exit_code": None, "reason": None},
        ]
    )
    status = s.parse_status(out, ["3", "4"])
    assert status["3"].timed_out and status["3"].category == "failed"
    assert status["4"].task_state == "lost"
    assert classify("starting") == "pending"


def test_scheduler_drives_the_queue(tmp_path):
    """The QueueBackend submits and polls through the seamm scheduler."""
    from seamm_scheduler.backend import QueueBackend
    from seamm_scheduler.local import LocalTransport
    from seamm_scheduler.script import build_script

    root = setup_root(tmp_path)
    s = Seamm(root=str(root))
    backend = QueueBackend(s, LocalTransport())
    directives = s.directives({"ntasks": 1}, {"chdir": str(tmp_path)})
    script = build_script(directives, "echo hello > hello.txt", scheduler=s)
    job = backend.submit(script, job_name="seamm-bundle_0000")
    deadline = time.time() + 30
    while time.time() < deadline:
        status = backend.poll_many([job])[job]
        if status.is_terminal:
            break
        time.sleep(0.5)
    assert status.category == "completed"
    assert (tmp_path / "hello.txt").read_text().strip() == "hello"


# ---- review fixes (design session, 2026-10-05) --------------------------------


def _running_row(q, kind, name, memory):
    """A row as if its runner were running (this process stands in for it)."""
    import psutil

    me = psutil.Process()
    cursor = q.db.execute(
        "INSERT INTO jobs (name, kind, script, cores, memory, state, submitted,"
        " started, pid, pid_start) VALUES (?, ?, 'x', 1, ?, 'running', ?, ?, ?, ?)",
        (name, kind, memory, time.time(), time.time(), os.getpid(), me.create_time()),
    )
    return cursor.lastrowid


def test_an_old_evaluator_never_blocks_tasks(tmp_path):
    """Four evaluators fill a 2 GB queue (512 MB each); a fifth has waited an
    hour for memory; the running evaluators' task must still start (it would
    deadlock otherwise)."""
    root = setup_root(tmp_path, cores=2, memory="2 GB")
    q = ts.Queue(root)
    for k in range(1, 5):
        _running_row(q, "evaluator", f"seamm-{k}", 1)
    waiting = q.submit("#!/bin/bash\n", name="seamm-5")
    q.db.execute(
        "UPDATE jobs SET submitted = ? WHERE id = ?", (time.time() - 3600, waiting)
    )
    task = q.submit("#!/bin/bash\n", cores=1, memory=100 * 2**20)
    assert q.schedule(start=False) == [task]
    q.close()


def test_an_old_task_reserves_only_against_tasks(tmp_path):
    root = setup_root(tmp_path, cores=2, memory="4 GB")
    q = ts.Queue(root)
    _running_row(q, "task", "busy", 100 * 2**20)  # one core in use
    big = q.submit("#!/bin/bash\n", cores=2, memory=100 * 2**20)
    q.db.execute(
        "UPDATE jobs SET submitted = ? WHERE id = ?", (time.time() - 3600, big)
    )
    small = q.submit("#!/bin/bash\n", cores=1, memory=100 * 2**20)
    evaluator = q.submit("#!/bin/bash\n", name="seamm-9")
    claimed = q.schedule(start=False)
    assert small not in claimed  # does not jump the reserving task
    assert evaluator in claimed  # evaluators are not held up
    q.close()


def test_a_task_larger_than_the_queue_now_fails(tmp_path):
    root = setup_root(tmp_path, cores=4)
    q = ts.Queue(root)
    job = q.submit("#!/bin/bash\n", cores=4, memory=100 * 2**20)
    (root / "taskserver.ini").write_text("[taskserver]\ncores = 2\nmemory = 2 GB\n")
    assert q.schedule(start=False) == []
    row = q.get(job)
    assert row["state"] == "failed" and "larger" in row["reason"]
    q.close()


def test_a_job_starts_with_a_minimal_environment(tmp_path):
    root = setup_root(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    env = dict(os.environ, SEAMM_CE='{"NTASKS": 9}', OMP_NUM_THREADS="99")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "seamm_scheduler.taskserver",
            "--root",
            str(root),
            "submit",
        ],
        input=f"#!/bin/bash\n#SEAMM --output {work}/env.txt\nenv\n",
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )
    job = int(result.stdout.strip())
    wait_for(root, [job])
    text = (work / "env.txt").read_text()
    assert "SEAMM_TASKSERVER_JOB_ID=" in text
    assert "SEAMM_CE" not in text and "OMP_NUM_THREADS" not in text


def test_a_detached_child_is_stopped_with_its_job(tmp_path):
    root = setup_root(tmp_path, kill_grace=1)
    pidfile = tmp_path / "child.pid"
    script = textwrap.dedent(f"""\
        #!/bin/bash
        #SEAMM --time 3
        {sys.executable} - <<'EOF'
        import subprocess, sys
        child = subprocess.Popen(
            [{sys.executable!r}, "-c", "import time; time.sleep(120)"],
            start_new_session=True,
        )
        open({str(pidfile)!r}, "w").write(str(child.pid))
        child.wait()
        EOF
        """)
    job = submit(root, script)
    record = wait_for(root, [job], timeout=60)[str(job)]
    assert record["state"] == "timeout"
    child = int(pidfile.read_text())
    deadline = time.time() + 10
    while time.time() < deadline and ts._alive(child, None):
        time.sleep(0.2)
    assert not ts._alive(child, None)


def test_the_floor_stops_one_job_after_its_grace(tmp_path):
    """A machine 'always' low (floor 100 %): after the grace the newest task is
    stopped -- one per episode, so the other runs to its end."""
    root = setup_root(tmp_path, cores=2, memory_floor=1.0, memory_grace=2, kill_grace=1)
    first = submit(root, "#!/bin/bash\n#SEAMM --memory 10M\nsleep 12\n")
    # The first must be running, so that the second is the newest (a fixed
    # sleep raced a slow runner)
    assert wait_until_running(root, first)["state"] == "running"
    second = submit(root, "#!/bin/bash\n#SEAMM --memory 10M\nsleep 12\n")
    records = wait_for(root, [first, second], timeout=60)
    assert records[str(second)]["state"] == "memory"
    assert "newest" in records[str(second)]["reason"]
    assert records[str(first)]["state"] == "completed"
