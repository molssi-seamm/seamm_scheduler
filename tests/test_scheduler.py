# -*- coding: utf-8 -*-

"""Tests for the Scheduler interface, the SLURM resource translation, the
generic QueueBackend, and SLURM 25.11's JSON (captured on TinkerCliffs,
2026-10-02)."""

import json

import pytest

from seamm_scheduler import (
    JobStatus,
    QueueBackend,
    TASK_STATES,
    build_script,
    format_memory_mb,
    format_walltime,
    get_scheduler,
)
from seamm_scheduler.pbs import Pbs
from seamm_scheduler.scheduler import SubmitError
from seamm_scheduler.slurm import Slurm


class FakeTransport:
    def __init__(self, responses):
        # [(match_prefix, (rc, out, err))]
        self.responses = list(responses)
        self.calls = []

    def run(self, argv, input_text=None):
        self.calls.append((list(argv), input_text))
        for prefix, response in self.responses:
            if argv[: len(prefix)] == prefix:
                return response
        return 1, "", "no response scripted"


class Res:
    """A stand-in for seamm_exec.Resources."""

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


# ---- helpers ------------------------------------------------------------


def test_get_scheduler():
    assert isinstance(get_scheduler("slurm"), Slurm)
    assert isinstance(get_scheduler("PBS"), Pbs)
    with pytest.raises(ValueError, match="Unknown scheduler 'lsf'"):
        get_scheduler("lsf")


def test_format_walltime_and_memory():
    assert format_walltime(3600 * 26 + 61) == "26:01:01"
    assert format_walltime(59.6) == "00:01:00"
    assert format_memory_mb(2 * 1024**3) == "2048M"
    assert format_memory_mb(1) == "1M"


def test_task_states_cover_every_category():
    assert TASK_STATES == {
        "pending": "queued",
        "running": "running",
        "completed": "finished",
        "failed": "failed",
        "cancelled": "lost",
        "unknown": "running",
    }
    assert JobStatus("1", "PENDING", "pending").task_state == "queued"


# ---- SLURM directives from resources ------------------------------------


def test_slurm_directives_from_resources():
    s = Slurm()
    d = s.directives(
        Res(
            ntasks=4,
            mem_per_cpu=2 * 1024**3,
            walltime=4 * 3600,
            partition="normal_q",
            account="seamm",
            ngpus=1,
        ),
        extra={"qos": "tc_normal_base", "mem": "20G", "export": "NONE"},
    )
    assert d == {
        "qos": "tc_normal_base",
        "export": "NONE",
        "partition": "normal_q",
        "account": "seamm",
        "ntasks": 4,
        "cpus_per_task": 1,
        "mem_per_cpu": "2048M",
        "time": "04:00:00",
        "gpus": 1,
    }
    lines = s.directive_lines(d)
    assert "#SBATCH --mem-per-cpu=2048M" in lines
    assert "#SBATCH --export=NONE" in lines
    assert not any("--mem=" in line for line in lines)


def test_slurm_directives_accept_a_dict_and_leave_out_none():
    d = Slurm().directives({"ntasks": 2}, extra={"time": "01:00:00"})
    assert d == {"time": "01:00:00", "ntasks": 2}


def test_build_script_defaults_to_slurm_and_takes_a_scheduler():
    text = build_script({"ntasks": 2}, "echo hi")
    assert text == "#!/bin/bash\n#SBATCH --ntasks=2\n\necho hi\n"
    text = build_script({"queue": "workq"}, "echo hi", scheduler="pbs")
    assert "#PBS -q workq" in text


def test_slurm_count_cmd_counts_array_elements():
    assert Slurm().count_cmd() == [
        "squeue",
        "--noheader",
        "--me",
        "-r",
        "--format=%i",
    ]


# ---- SLURM 25.11 JSON (TinkerCliffs) --------------------------------------

SACCT_25 = {
    "jobs": [
        {
            "job_id": 7826618,
            "state": {"current": ["COMPLETED"], "reason": "BeginTime"},
            "exit_code": {
                "status": ["SUCCESS"],
                "return_code": {"set": True, "infinite": False, "number": 0},
                "signal": {
                    "id": {"set": False, "infinite": False, "number": 0},
                    "name": "",
                },
            },
        },
        {
            "job_id": 7823204,
            "state": {"current": ["TIMEOUT"], "reason": "None"},
            "exit_code": {
                "status": ["SUCCESS"],
                "return_code": {"set": True, "infinite": False, "number": 0},
            },
        },
    ]
}


def test_sacct_json_slurm_25_nested_return_code():
    result = Slurm._parse_sacct_json(json.dumps(SACCT_25))
    assert result["7826618"].category == "completed"
    assert result["7826618"].exit_code == 0
    assert result["7823204"].category == "failed"
    assert result["7823204"].task_state == "failed"


def test_squeue_json_slurm_25_state_list():
    out = json.dumps(
        {"jobs": [{"job_id": 7823204, "job_state": ["TIMEOUT", "COMPLETING"]}]}
    )
    result = Slurm._parse_squeue_json(out)
    assert result["7823204"].state == "TIMEOUT"
    assert result["7823204"].is_terminal


# ---- the generic QueueBackend --------------------------------------------


def test_queue_backend_submit_poll_cancel_count_with_slurm():
    transport = FakeTransport(
        [
            (["sbatch"], (0, "42\n", "")),
            (["squeue", "--json"], (0, json.dumps({"jobs": []}), "")),
            (["sacct", "--json"], (0, json.dumps(SACCT_25), "")),
            (["scancel"], (0, "", "")),
            (["squeue", "--noheader", "--me"], (0, "1\n2\n3\n", "")),
        ]
    )
    backend = QueueBackend(Slurm(), transport)
    assert backend.submit("#!/bin/bash\n", job_name="t") == "42"
    assert transport.calls[0] == (
        ["sbatch", "--parsable", "--job-name", "t"],
        "#!/bin/bash\n",
    )
    states = backend.poll_many(["7826618"])
    assert states["7826618"].category == "completed"
    backend.cancel_many(["1", "2"])
    assert ["scancel", "1", "2"] in [c[0] for c in transport.calls]
    assert backend.count_jobs() == 3


def test_queue_backend_submit_error_type():
    backend = QueueBackend(Slurm(), FakeTransport([(["sbatch"], (1, "", "bad"))]))
    with pytest.raises(SubmitError, match="sbatch failed"):
        backend.submit("x")


def test_queue_backend_count_unknown_without_count_cmd():
    assert QueueBackend(Pbs(), FakeTransport([])).count_jobs() is None


def test_queue_backend_without_transport_says_so():
    with pytest.raises(NotImplementedError, match="no transport"):
        QueueBackend(Slurm()).poll_many(["1"])


def test_slurm_poll_failed_when_sacct_cannot_answer():
    s = Slurm()

    def ssh_down(argv, input_text=None):
        return 255, "", "ssh: connect to host tc port 22: Operation timed out"

    assert s.poll(ssh_down, ["1"]) == {}
    assert s.poll_failed

    def gone(argv, input_text=None):
        if argv[0] == "sacct":
            return 0, json.dumps({"jobs": []}), ""
        return 1, "", "slurm_load_jobs error: Invalid job id specified"

    assert s.poll(gone, ["1"]) == {}
    assert not s.poll_failed


def test_slurm_unparseable_output_is_a_failed_poll():
    s = Slurm()

    def garbled(argv, input_text=None):
        return 0, "Welcome to the cluster!\n{not json", ""

    assert s.poll(garbled, ["1"]) == {}
    assert s.poll_failed


def test_slurm_without_accounting_trusts_squeue():
    s = Slurm()

    def no_accounting(argv, input_text=None):
        if argv[0] == "sacct":
            return 1, "", "sacct: error: Slurm accounting storage is disabled"
        if "--json" in argv:
            return 0, json.dumps({"jobs": []}), ""
        return 0, "", ""

    assert s.poll(no_accounting, ["1"]) == {}
    assert not s.poll_failed
    # Remembered: sacct is not asked again
    calls = []

    def recording(argv, input_text=None):
        calls.append(argv[0])
        return no_accounting(argv, input_text)

    s.poll(recording, ["1"])
    assert "sacct" not in calls and not s.poll_failed


def test_find_jobs_by_name():
    transport = FakeTransport([(["squeue", "--noheader", "--me"], (0, "77\n", ""))])
    backend = QueueBackend(Slurm(), transport)
    assert backend.find_jobs("seamm-b.1-abc") == ["77"]
    assert transport.calls[0][0][3] == "--name=seamm-b.1-abc"
    down = QueueBackend(Slurm(), FakeTransport([]))
    assert down.find_jobs("x") is None
    # A finished job is found in accounting
    finished = FakeTransport(
        [
            (["squeue", "--noheader", "--me"], (0, "", "")),
            (["sacct"], (0, "78\n", "")),
        ]
    )
    assert QueueBackend(Slurm(), finished).find_jobs("seamm-b.1-abc") == ["78"]
    nowhere = FakeTransport(
        [(["squeue", "--noheader", "--me"], (0, "", "")), (["sacct"], (0, "", ""))]
    )
    assert QueueBackend(Slurm(), nowhere).find_jobs("x") == []


def test_local_transport_can_drop_the_allocation(monkeypatch):
    from unittest.mock import MagicMock, patch

    from seamm_scheduler import LocalTransport

    monkeypatch.setenv("SLURM_MEM_PER_CPU", "1000")
    monkeypatch.setenv("SLURM_CONF", "/etc/slurm/slurm.conf")
    monkeypatch.setenv("SBATCH_ACCOUNT", "seamm")
    fake = MagicMock(returncode=0, stdout="", stderr="")
    with patch("seamm_scheduler.local.subprocess.run", return_value=fake) as run:
        LocalTransport(drop_env_prefixes=("SLURM_",)).run(["sbatch"])
    env = run.call_args.kwargs["env"]
    assert "SLURM_MEM_PER_CPU" not in env and env["SBATCH_ACCOUNT"] == "seamm"
    assert env["SLURM_CONF"] == "/etc/slurm/slurm.conf"


def test_squeue_json_transient_failure_without_accounting():
    s = Slurm()
    s._no_accounting = True

    def run(argv, input_text=None):
        if argv[0] == "squeue":
            return 255, "", "ssh: tc: timed out after 300 s"
        return 1, "", "sacct: error: Slurm accounting storage is disabled"

    assert s.poll(run, ["1"]) == {}
    assert s.poll_failed


def test_squeue_json_transient_failure_with_accounting():
    s = Slurm()

    def run(argv, input_text=None):
        if argv[0] == "squeue":
            return 1, "", "slurm_load_jobs error: Unable to contact slurm controller"
        # sacct has not recorded the new job yet
        return 0, json.dumps({"jobs": []}), ""

    assert s.poll(run, ["1"]) == {}
    assert s.poll_failed

    def gone(argv, input_text=None):
        if argv[0] == "squeue":
            return 1, "", "slurm_load_jobs error: Invalid job id specified"
        return 0, json.dumps({"jobs": []}), ""

    assert s.poll(gone, ["1"]) == {}
    assert not s.poll_failed

    def answered_by_sacct(argv, input_text=None):
        if argv[0] == "squeue":
            return 1, "", "slurm_load_jobs error: Unable to contact slurm controller"
        return 0, json.dumps(SACCT_25), ""

    s.poll(answered_by_sacct, ["7826618"])
    assert not s.poll_failed


def test_find_narrows_a_date_range_the_site_refuses():
    calls = []

    def run(argv, input_text=None):
        calls.append(argv)
        if argv[0] == "squeue":
            return 0, "", ""
        if "--starttime=now-14days" in argv:
            return 1, "", "sacct: error: Too wide of a date range in query"
        return 0, "79\n", ""

    assert Slurm().find(run, "x") == ["79"]
    assert sum(1 for c in calls if c[0] == "sacct") == 2
