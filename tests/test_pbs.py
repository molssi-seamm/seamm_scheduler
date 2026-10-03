# -*- coding: utf-8 -*-

"""Tests for seamm_scheduler.pbs against mocked qsub/qstat/qdel.

There is no PBS site to validate on yet; the output shapes follow PBS
Professional 19's documented ``qstat -x``, ``qstat -x -f`` and
``qstat -x -f -F json`` formats.
"""

import json

from seamm_scheduler import QueueBackend, build_script
from seamm_scheduler.pbs import Pbs, classify


class FakeTransport:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def run(self, argv, input_text=None):
        self.calls.append(list(argv))
        return self.handler(argv, input_text)


class Res:
    def __init__(self, **kwargs):
        self.ntasks = None
        self.cpus_per_task = 1
        self.mem_per_cpu = None
        self.ngpus = 0
        self.walltime = None
        self.partition = None
        self.account = None
        self.qos = None
        self.nodes = None
        self.__dict__.update(kwargs)


QSTAT_JSON = {
    "timestamp": 1759420000,
    "pbs_version": "19.1.3",
    "pbs_server": "pbs01",
    "Jobs": {
        "101.pbs01": {"Job_Name": "a", "job_state": "Q"},
        "102.pbs01": {"Job_Name": "b", "job_state": "R"},
        "103.pbs01": {"Job_Name": "c", "job_state": "F", "Exit_status": 0},
        "104.pbs01": {"Job_Name": "d", "job_state": "F", "Exit_status": 1},
        "105.pbs01": {"Job_Name": "e", "job_state": "F", "Exit_status": 271},
    },
}

QSTAT_TABLE = """\
Job id            Name             User              Time Use S Queue
----------------  ---------------- ----------------  -------- - -----
101.pbs01         a                psaxe             00:00:00 Q workq
102.pbs01         b                psaxe             00:00:05 R workq
103.pbs01         c                psaxe             00:01:00 F workq
"""

QSTAT_FULL = """\
Job Id: 103.pbs01
    Job_Name = c
    job_state = F
    Exit_status = 0

Job Id: 104.pbs01
    Job_Name = d
    job_state = F
    Exit_status = 2
"""


def test_classify():
    assert classify("Q") == "pending"
    assert classify("H") == "pending"
    assert classify("R") == "running"
    assert classify("E") == "running"
    assert classify("F", 0) == "completed"
    assert classify("F", "1") == "failed"
    assert classify("F", -1) == "failed"
    assert classify("F", 271) == "cancelled"
    assert classify("F") == "cancelled"  # deleted before it ran
    assert classify("C") == "completed"  # Torque
    assert classify("") == "unknown"


def test_directives_select_statement():
    d = Pbs().directives(
        Res(
            ntasks=8,
            cpus_per_task=2,
            nodes=2,
            mem_per_cpu=1024**3,
            ngpus=2,
            walltime=7200,
            partition="workq",
            account="proj",
            qos="ignored",
        )
    )
    assert d == {
        "queue": "workq",
        "account": "proj",
        "walltime": "02:00:00",
        "select": "2:ncpus=8:mpiprocs=4:ompthreads=2:mem=8192mb:ngpus=1",
    }
    assert Pbs().directive_lines(d) == [
        "#PBS -q workq",
        "#PBS -A proj",
        "#PBS -l select=2:ncpus=8:mpiprocs=4:ompthreads=2:mem=8192mb:ngpus=1",
        "#PBS -l walltime=02:00:00",
    ]


def test_directives_portable_section_keys():
    d = Pbs().directives(Res(ntasks=1), extra={"partition": "q", "time": "1:00:00"})
    assert d["queue"] == "q"
    assert d["walltime"] == "01:00:00"
    assert d["select"] == "1:ncpus=1:mpiprocs=1"


def test_build_script_pbs():
    text = build_script(
        {"job_name": "t", "select": "1:ncpus=4"}, "echo", scheduler="pbs"
    )
    assert text == "#!/bin/bash\n#PBS -N t\n#PBS -l select=1:ncpus=4\n\necho\n"


def test_submit_reads_the_job_id():
    t = FakeTransport(lambda argv, text: (0, "1234.pbs01\n", ""))
    backend = QueueBackend(Pbs(), t)
    assert backend.submit("#!/bin/bash\n", job_name="x") == "1234.pbs01"
    assert t.calls == [["qsub", "-N", "x"]]


def test_poll_json():
    def handler(argv, text):
        assert argv[:5] == ["qstat", "-x", "-f", "-F", "json"]
        # qstat exits 35 (unknown job) for 106 but reports the rest
        return 35, json.dumps(QSTAT_JSON), "qstat: Unknown Job Id 106.pbs01"

    backend = QueueBackend(Pbs(), FakeTransport(handler))
    ids = ["101.pbs01", "102.pbs01", "103.pbs01", "104.pbs01", "105", "106.pbs01"]
    result = backend.poll_many(ids)
    assert {k: v.category for k, v in result.items()} == {
        "101.pbs01": "pending",
        "102.pbs01": "running",
        "103.pbs01": "completed",
        "104.pbs01": "failed",
        "105": "cancelled",  # matched by sequence number
    }
    assert result["104.pbs01"].exit_code == "1"
    assert result["105"].task_state == "lost"


def test_poll_falls_back_to_text_and_asks_for_exit_status():
    calls = []

    def handler(argv, text):
        calls.append(argv)
        if "-F" in argv:
            return 2, "", "qstat: invalid option -- 'F'\nusage: qstat ..."
        if argv == ["qstat", "-x", "101.pbs01", "102.pbs01", "103.pbs01"]:
            return 0, QSTAT_TABLE, ""
        if argv == ["qstat", "-x", "-f", "103.pbs01"]:
            return 0, QSTAT_FULL, ""
        return 1, "", "unexpected"

    pbs = Pbs()
    backend = QueueBackend(pbs, FakeTransport(handler))
    result = backend.poll_many(["101.pbs01", "102.pbs01", "103.pbs01"])
    assert {k: v.category for k, v in result.items()} == {
        "101.pbs01": "pending",
        "102.pbs01": "running",
        "103.pbs01": "completed",
    }
    # The JSON probe is remembered.
    assert pbs._qstat_json is False
    backend.poll_many(["101.pbs01"])
    assert sum(1 for c in calls if "-F" in c) == 1


def test_cancel():
    t = FakeTransport(lambda argv, text: (0, "", ""))
    QueueBackend(Pbs(), t).cancel_many(["1.pbs01", "2.pbs01"])
    assert t.calls == [["qdel", "1.pbs01", "2.pbs01"]]


def test_env_names():
    assert Pbs.env_names["job_id"] == "PBS_JOBID"
    assert Pbs.env_names["nodefile"] == "PBS_NODEFILE"


def test_slurm_spellings_become_resources_and_find_count():
    """SLURM spellings in a section become the select chunk; the explicit
    resources win where both give a value."""
    d = Pbs().directives(
        Res(ntasks=2), extra={"ntasks": "4", "mem": "4G", "queue": "q"}
    )
    assert "ntasks" not in d and "mem" not in d
    assert d["select"] == "1:ncpus=2:mpiprocs=2:mem=4096mb"
    assert Pbs().count_cmd()[0] == "sh"
    assert "-N seamm-x" in Pbs().find_cmd("seamm-x")[2]


def test_dependency_is_a_W_attribute():
    """qsub takes a dependency with -W, not as a resource (real OpenPBS refuses
    -l depend). SLURM's spelling 'dependency' is accepted."""
    pbs = Pbs()
    lines = pbs.directive_lines(pbs.directives({}, {"dependency": "afterany:11.x"}))
    assert "#PBS -W depend=afterany:11.x" in lines
    assert not any("-l depend" in line for line in lines)


def test_chdir_becomes_a_cd():
    """A PBS job starts in the home directory: chdir becomes a cd."""
    pbs = Pbs()
    directives = pbs.directives({}, {"chdir": "/data/job 7"})
    assert not any("chdir" in line for line in pbs.directive_lines(directives))
    script = build_script(directives, "run_flowchart", scheduler=pbs)
    lines = script.splitlines()
    assert "cd '/data/job 7' || exit 1" in lines
    assert lines.index("cd '/data/job 7' || exit 1") < lines.index("run_flowchart")


def test_export():
    pbs = Pbs()
    assert "#PBS -V" in pbs.directive_lines(pbs.directives({}, {"export": "ALL"}))
    assert "#PBS -V" not in pbs.directive_lines(pbs.directives({}, {"export": "NONE"}))


def test_find_includes_finished_jobs():
    assert "qselect -x" in Pbs().find_cmd("bundle-1")[-1]


def test_section_select_memory_is_kept():
    """A bundle's resources set the cores; the section's memory stays."""
    d = Pbs().directives(Res(ntasks=1), extra={"select": "1:ncpus=1:mem=4gb"})
    assert d["select"] == "1:ncpus=1:mem=4gb:mpiprocs=1"


def test_jobserver_overrides_in_slurm_spellings():
    """A job's ntasks/mem overrides (SLURM spellings) reach the select."""
    d = Pbs().directives(
        {}, extra={"queue": "workq", "ntasks": "6", "mem": "8G", "time": "1-00:00:00"}
    )
    assert d["select"] == "1:ncpus=6:mpiprocs=6:mem=8192mb"
    assert d["walltime"] == "24:00:00"


def test_walltime_limits_parse():
    from seamm_scheduler.config import _parse_slurm_value

    assert _parse_slurm_value("walltime", "02:00:00") == 7200


def test_garbled_json_is_a_failed_poll():
    """A truncated qstat reply must not look like 'every job is gone'."""
    for reply in ('{"Jobs": {"1.x": {"job_state"', '{"Jobs": []}'):
        backend = QueueBackend(Pbs(), FakeTransport(lambda a, t, r=reply: (0, r, "")))
        assert backend.poll_many(["1.x"]) == {}
        assert backend.scheduler.poll_failed is True


def test_text_fallback_unknown_ids_are_gone():
    """qstat -x (text) with only unknown ids: the jobs are gone, not a failure."""
    pbs = Pbs()
    pbs._qstat_json = False
    backend = QueueBackend(
        pbs, FakeTransport(lambda a, t: (153, "", "qstat: Unknown Job Id 9.x"))
    )
    assert backend.poll_many(["9.x"]) == {}
    assert pbs.poll_failed is False


def test_cd_failure_stops_the_job():
    pbs = Pbs()
    assert pbs.prologue_lines({"chdir": "/a b"}) == ["cd '/a b' || exit 1"]
