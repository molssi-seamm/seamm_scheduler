#!/usr/bin/env python
"""Replay command output recorded from real sites (devtools/capture_fixtures.py).

Each fixture is the lifecycle of four jobs -- one that succeeds, one that fails
(exit 3), one held behind another, and a long one that is cancelled -- polled
while queued/running and again after they finish. The back end is driven with a
transport that answers each command with the recorded output.
"""

import json
from pathlib import Path

import pytest

from seamm_scheduler.backend import QueueBackend
from seamm_scheduler.scheduler import get_scheduler

fixtures = sorted((Path(__file__).parent / "fixtures").glob("*/lifecycle.json"))


class ReplayTransport:
    """Answers each command with the next recorded output for the same argv.

    A fresh back end asks what the recorded one had already learned (e.g. that
    ``squeue --json`` is not supported), so a command not recorded in this phase
    is answered from the same kind of command earlier in the recording (same
    program and options, any job ids).
    """

    def __init__(self, calls, everything):
        self.calls = list(calls)
        self.everything = everything

    def run(self, argv, input_text=None):
        argv = [str(a) for a in argv]
        for i, call in enumerate(self.calls):
            if call["argv"] == argv:
                del self.calls[i]
                return call["rc"], call["stdout"], call["stderr"]
        for call in self.everything:
            if call["argv"][:-1] == argv[:-1] and call["rc"] != 0:
                return call["rc"], call["stdout"], call["stderr"]
        raise AssertionError(f"No recorded output for {argv}")


def _replay(path, label):
    data = json.loads(path.read_text())
    calls = [c for c in data["calls"] if c["label"] == label]
    transport = ReplayTransport(calls, data["calls"])
    backend = QueueBackend(get_scheduler(data["scheduler"]), transport)
    return data, backend


@pytest.mark.parametrize("path", fixtures, ids=[p.parent.name for p in fixtures])
def test_submit_ids(path):
    data = json.loads(path.read_text())
    scheduler = get_scheduler(data["scheduler"])
    submits = [c for c in data["calls"] if c["label"].startswith("submit ")]
    assert len(submits) == 4
    for call in submits:
        name = call["label"].split()[1]
        assert scheduler.parse_submit(call["stdout"]) == data["ids"][name]


@pytest.mark.parametrize("path", fixtures, ids=[p.parent.name for p in fixtures])
def test_poll_while_running(path):
    data, backend = _replay(path, "poll running/pending")
    ids = data["ids"]
    result = backend.poll_many(list(ids.values()))
    assert result[ids["held"]].category == "pending"
    for name in ("ok", "fail"):
        assert result[ids[name]].category == "running"
    # The long job may still be waiting for a free core.
    assert result[ids["long"]].category in ("running", "pending")


@pytest.mark.parametrize("path", fixtures, ids=[p.parent.name for p in fixtures])
def test_poll_finished(path):
    data, backend = _replay(path, "poll finished")
    ids = data["ids"]
    result = backend.poll_many(list(ids.values()))
    assert result[ids["ok"]].category == "completed"
    assert result[ids["fail"]].category == "failed"
    assert result[ids["long"]].category == "cancelled"
    # The held job ran once the long one was cancelled (afterany).
    assert result[ids["held"]].category == "completed"


@pytest.mark.parametrize("path", fixtures, ids=[p.parent.name for p in fixtures])
def test_find(path):
    data, backend = _replay(path, "find long")
    assert backend.find_jobs(_name(data, "long")) == [data["ids"]["long"]]
    data, backend = _replay(path, "find finished ok")
    assert backend.find_jobs(_name(data, "ok")) == [data["ids"]["ok"]]


@pytest.mark.parametrize("path", fixtures, ids=[p.parent.name for p in fixtures])
def test_unknown_id(path):
    data, backend = _replay(path, "poll unknown id")
    assert backend.poll_many(["999999"]) == {}


def _name(data, which):
    """The job name the capture used, from the recorded find command."""
    for call in data["calls"]:
        if call["label"].startswith("find"):
            for arg in call["argv"]:
                for part in arg.replace("'", " ").replace('"', " ").split():
                    if part.endswith(f"-{which}") and "seammfx" in part:
                        return part.split("=")[-1]
    raise AssertionError(f"no job name for {which}")
