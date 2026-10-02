# -*- coding: utf-8 -*-

"""Tests for seamm_slurm.script.build_script."""

from seamm_slurm.script import build_script


def test_build_script_basic_directives():
    script = build_script(
        {
            "job_name": "demo",
            "partition": "batch",
            "nodes": 1,
            "ntasks": 4,
            "time": "01:00:00",
        },
        payload="echo hello\n",
    )
    lines = script.splitlines()
    assert lines[0] == "#!/bin/bash"
    assert "#SBATCH --job-name=demo" in lines
    assert "#SBATCH --partition=batch" in lines
    assert "#SBATCH --nodes=1" in lines
    assert "#SBATCH --ntasks=4" in lines
    assert "#SBATCH --time=01:00:00" in lines
    assert "echo hello" in lines


def test_build_script_skips_blank_and_none_directives():
    script = build_script(
        {"partition": "batch", "account": "", "qos": None, "nodes": 1},
        payload="echo hi\n",
    )
    assert "#SBATCH --partition=batch" in script
    assert "--account" not in script
    assert "--qos" not in script
    assert "#SBATCH --nodes=1" in script


def test_build_script_directive_order_is_reproducible():
    directives = {"time": "01:00:00", "job_name": "x", "partition": "batch"}
    script = build_script(directives, payload="echo hi\n")
    order = [line for line in script.splitlines() if line.startswith("#SBATCH")]
    assert order == [
        "#SBATCH --job-name=x",
        "#SBATCH --partition=batch",
        "#SBATCH --time=01:00:00",
    ]


def test_build_script_passes_through_unknown_directives():
    script = build_script({"gres": "gpu:1"}, payload="echo hi\n")
    assert "#SBATCH --gres=gpu:1" in script


def test_build_script_custom_shell():
    script = build_script({}, payload="echo hi\n", shell="/bin/sh")
    assert script.splitlines()[0] == "#!/bin/sh"


def test_build_script_payload_is_verbatim_after_directives():
    script = build_script(
        {"partition": "batch"},
        payload="line one\nline two\n",
    )
    body = script.split("\n\n", 1)[1]
    assert body.strip("\n") == "line one\nline two"
