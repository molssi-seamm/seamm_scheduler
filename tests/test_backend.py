# -*- coding: utf-8 -*-

"""Tests for seamm_slurm.backend.SlurmBackend.

Uses a FakeBackend whose ``_run`` is driven by a caller-supplied function, so
these tests never touch a real SLURM installation -- they exercise the
submit/poll/cancel logic and the JSON-vs-text fallback against canned CLI
output (some of it captured verbatim from a real SLURM 21.08.5 cluster
during Phase 0 groundwork).
"""

import json

import pytest

from seamm_slurm.backend import SlurmBackend, SlurmError, SlurmSubmitError


class FakeBackend(SlurmBackend):
    """A SlurmBackend whose `_run` is a caller-supplied function, and which
    records every call made to it."""

    def __init__(self, run_fn):
        super().__init__()
        self._run_fn = run_fn
        self.calls = []

    def _run(self, argv, input_text=None):
        self.calls.append((list(argv), input_text))
        return self._run_fn(argv, input_text)


# ---- submit -----------------------------------------------------------


def test_submit_returns_job_id():
    def run_fn(argv, input_text):
        assert argv[:2] == ["sbatch", "--parsable"]
        assert input_text == "#!/bin/bash\necho hi\n"
        return 0, "42\n", ""

    backend = FakeBackend(run_fn)
    job_id = backend.submit("#!/bin/bash\necho hi\n")
    assert job_id == "42"


def test_submit_parses_parsable_cluster_suffix():
    def run_fn(argv, input_text):
        return 0, "42;chemai\n", ""

    backend = FakeBackend(run_fn)
    assert backend.submit("script") == "42"


def test_submit_passes_job_name():
    def run_fn(argv, input_text):
        assert "--job-name" in argv
        assert argv[argv.index("--job-name") + 1] == "demo"
        return 0, "1\n", ""

    backend = FakeBackend(run_fn)
    backend.submit("script", job_name="demo")


def test_submit_raises_on_sbatch_failure():
    def run_fn(argv, input_text):
        return 1, "", "sbatch: error: invalid partition"

    backend = FakeBackend(run_fn)
    with pytest.raises(SlurmSubmitError, match="invalid partition"):
        backend.submit("script")


def test_submit_raises_on_empty_output():
    def run_fn(argv, input_text):
        return 0, "", ""

    backend = FakeBackend(run_fn)
    with pytest.raises(SlurmSubmitError):
        backend.submit("script")


# ---- cancel -------------------------------------------------------------


def test_cancel_success():
    def run_fn(argv, input_text):
        assert argv == ["scancel", "42"]
        return 0, "", ""

    backend = FakeBackend(run_fn)
    backend.cancel("42")  # should not raise


def test_cancel_raises_on_failure():
    def run_fn(argv, input_text):
        return 1, "", "scancel: error: Invalid job id specified"

    backend = FakeBackend(run_fn)
    with pytest.raises(SlurmError, match="Invalid job id"):
        backend.cancel("42")


# ---- poll_many: empty / no-op -------------------------------------------


def test_poll_many_empty_input_makes_no_calls():
    backend = FakeBackend(lambda argv, input_text: (0, "", ""))
    assert backend.poll_many([]) == {}
    assert backend.calls == []


# ---- poll_many: squeue JSON path ----------------------------------------

# Captured (trimmed) from a real `squeue --json` on SLURM 21.08.5/ChemAI.
SQUEUE_JSON_RUNNING = json.dumps(
    {
        "jobs": [
            {"job_id": 5, "job_state": "RUNNING", "partition": "batch"},
        ]
    }
)

SQUEUE_JSON_EMPTY = json.dumps({"jobs": []})


def test_poll_many_squeue_json_running():
    def run_fn(argv, input_text):
        assert argv[:2] == ["squeue", "--json"]
        return 0, SQUEUE_JSON_RUNNING, ""

    backend = FakeBackend(run_fn)
    result = backend.poll_many(["5"])
    assert result["5"].state == "RUNNING"
    assert result["5"].category == "running"
    assert result["5"].is_terminal is False


def test_poll_many_squeue_json_flags_list_form():
    # Some SLURM/OpenAPI versions report job_state as a list of flags.
    data = json.dumps({"jobs": [{"job_id": 5, "job_state": ["COMPLETED"]}]})

    def run_fn(argv, input_text):
        if argv[0] == "squeue":
            return 0, data, ""
        return 0, "", ""

    backend = FakeBackend(run_fn)
    result = backend.poll_many(["5"])
    assert result["5"].category == "completed"


# ---- poll_many: squeue falls back to text when --json unsupported -------


def test_poll_many_squeue_falls_back_to_text_on_unrecognized_option():
    calls = []

    def run_fn(argv, input_text):
        calls.append(argv)
        if argv[:2] == ["squeue", "--json"]:
            return 1, "", "squeue: unrecognized option '--json'"
        if argv[0] == "squeue":
            assert "--format=%i|%T|%R" in argv
            return 0, "6|PENDING|(Resources)\n", ""
        return 0, "", ""

    backend = FakeBackend(run_fn)
    result = backend.poll_many(["6"])
    assert result["6"].state == "PENDING"
    assert result["6"].category == "pending"
    assert result["6"].reason == "(Resources)"
    # Capability should be remembered -- a second poll shouldn't re-probe JSON.
    backend.poll_many(["6"])
    json_attempts = [c for c in calls if c[:2] == ["squeue", "--json"]]
    assert len(json_attempts) == 1


# ---- poll_many: sacct fills in what squeue no longer lists --------------

# Captured (trimmed) from a real `sacct --json` on SLURM 21.08.5/ChemAI --
# note state/exit_code are nested objects here, unlike squeue's flat string.
SACCT_JSON_COMPLETED = json.dumps(
    {
        "jobs": [
            {
                "job_id": 3,
                "state": {"current": "COMPLETED", "reason": "Prolog"},
                "exit_code": {"status": "SUCCESS", "return_code": 0},
            }
        ]
    }
)


def test_poll_many_uses_sacct_for_jobs_squeue_no_longer_lists():
    def run_fn(argv, input_text):
        if argv[0] == "squeue":
            return 0, SQUEUE_JSON_EMPTY, ""
        if argv[0] == "sacct":
            assert argv[:2] == ["sacct", "--json"]
            return 0, SACCT_JSON_COMPLETED, ""
        raise AssertionError(f"unexpected command: {argv}")

    backend = FakeBackend(run_fn)
    result = backend.poll_many(["3"])
    assert result["3"].state == "COMPLETED"
    assert result["3"].category == "completed"
    assert result["3"].exit_code == 0
    assert result["3"].is_terminal is True


def test_poll_many_does_not_call_sacct_when_squeue_has_everything():
    def run_fn(argv, input_text):
        if argv[0] == "squeue":
            return 0, SQUEUE_JSON_RUNNING, ""
        raise AssertionError("sacct should not have been called")

    backend = FakeBackend(run_fn)
    backend.poll_many(["5"])  # would raise if sacct were called


def test_poll_many_sacct_text_fallback_skips_substep_rows():
    def run_fn(argv, input_text):
        if argv[0] == "squeue":
            if argv[:2] == ["squeue", "--json"]:
                return 1, "", "squeue: unrecognized option '--json'"
            return 0, "", ""
        if argv[0] == "sacct":
            if argv[:2] == ["sacct", "--json"]:
                return 1, "", "sacct: unrecognized option '--json'"
            # Real (trimmed) shape from `sacct --parsable2 --noheader`.
            return (
                0,
                "3|COMPLETED|0:0\n3.batch|COMPLETED|0:0\n3.extern|COMPLETED|0:0\n",
                "",
            )
        raise AssertionError(f"unexpected command: {argv}")

    backend = FakeBackend(run_fn)
    result = backend.poll_many(["3"])
    assert list(result.keys()) == ["3"]
    assert result["3"].state == "COMPLETED"
    assert result["3"].exit_code == "0:0"


def test_poll_many_missing_job_id_is_absent_from_result():
    def run_fn(argv, input_text):
        if argv[0] == "squeue":
            return 0, SQUEUE_JSON_EMPTY, ""
        if argv[0] == "sacct":
            return 0, json.dumps({"jobs": []}), ""
        raise AssertionError(f"unexpected command: {argv}")

    backend = FakeBackend(run_fn)
    result = backend.poll_many(["999"])
    assert result == {}
