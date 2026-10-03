# -*- coding: utf-8 -*-

"""Tests for the task-layer keys of a target section (2026-10-02)."""

import json

import pytest

from seamm_scheduler import QueueBackend, SlurmSection, TargetSection
from seamm_scheduler.config import _parse_time, list_sections, load_target
from seamm_scheduler.pbs import Pbs
from seamm_scheduler.slurm import LocalSlurm, SshSlurm
from seamm_scheduler.stage import LocalStager, RsyncStager

INI = """\
[DEFAULT]
default = local

[local]
type = local
tasks = pool
max_concurrent_jobs = 4

[chemai]
type = local
tasks = queue
scheduler = slurm
transport = local
partition = normal
bundle_tasks = 50

[arc]
type = local
tasks = queue
scheduler = slurm
transport = ssh
host = tinkercliffs
remote_root = /projects/seamm/psaxe/tasks
account = seamm
partition = normal_q
max_queued_tasks = 800
bundle_walltime = 04:00:00
inline_below = 30
poll_interval = 20
remote_python = /projects/seamm/psaxe/phase2/venv/bin/python
remote_seamm_root = /projects/seamm/SEAMM
setup = module load ORCA/6.1.1

[arc-all]
type = slurm
transport = ssh
host = tinkercliffs
tasks = queue
scheduler = slurm
shared_filesystem = yes

[pbs-site]
type = local
tasks = queue
scheduler = pbs
transport = ssh
host = pbs01

[old]
transport = local
partition = batch
"""


@pytest.fixture
def sections(tmp_path):
    (tmp_path / "host.ini").write_text(INI)
    return list_sections(tmp_path, "host")


def test_new_keys_are_not_directives(sections):
    arc = sections["arc"]
    assert arc.directives == {"account": "seamm", "partition": "normal_q"}
    assert arc.tasks == "queue"
    assert arc.max_queued_tasks == 800
    assert arc.bundle_walltime == 4 * 3600
    assert arc.inline_below == 30.0
    assert arc.poll_interval == 20.0
    assert arc.remote_python.endswith("/venv/bin/python")
    assert sections["chemai"].directives == {"partition": "normal"}
    assert sections["chemai"].bundle_tasks == 50


def test_old_sections_keep_their_meaning(sections):
    old = sections["old"]
    assert old.tasks is None
    assert old.type == "slurm"
    assert old.directives == {"partition": "batch"}
    assert isinstance(old.build_backend(), LocalSlurm)


def test_task_transport_and_staging(sections):
    # Evaluator on this host, tasks over ssh: staged with rsync.
    arc = sections["arc"]
    assert arc.task_transport == "ssh"
    assert not arc.tasks_share_filesystem
    assert isinstance(arc.build_task_backend(), SshSlurm)
    assert isinstance(arc.build_task_stager(), RsyncStager)
    # Evaluator inside the cluster: local commands, no staging, whatever
    # transport the JobServer uses to reach the cluster.
    arc_all = sections["arc-all"]
    assert arc_all.task_transport == "local"
    assert isinstance(arc_all.build_task_backend(), LocalSlurm)
    assert isinstance(arc_all.build_task_stager(), LocalStager)
    # The JobServer still reaches it over ssh.
    assert isinstance(arc_all.build_backend(), SshSlurm)
    chemai = sections["chemai"]
    assert chemai.tasks_share_filesystem


def test_shared_filesystem_overrides_the_default():
    s = TargetSection(
        name="x",
        transport="ssh",
        host="h",
        type="local",
        tasks="queue",
        shared_filesystem=True,
    )
    assert isinstance(s.build_task_stager(), LocalStager)


def test_pbs_task_backend(sections):
    backend = sections["pbs-site"].build_task_backend()
    assert type(backend) is QueueBackend
    assert isinstance(backend.scheduler, Pbs)
    assert backend.host == "pbs01"


def test_only_tasks_queue_builds_a_task_backend(sections):
    with pytest.raises(RuntimeError, match="tasks=pool"):
        sections["local"].build_task_backend()


def test_settings_round_trip(sections, tmp_path):
    arc = sections["arc"]
    data = json.loads(json.dumps(arc.task_settings()))
    again = TargetSection.from_settings(data)
    assert again == arc
    # Unknown keys from a newer writer are ignored.
    data["something_new"] = 1
    assert TargetSection.from_settings(data) == arc


def test_settings_round_trip_with_limits(tmp_path):
    (tmp_path / "h.ini").write_text(
        "[q]\ntransport = local\n\n[q.limits]\noverridable = ntasks\nntasks.max = 8\n"
    )
    q = load_target(tmp_path, "h")
    assert SlurmSection.from_settings(json.loads(json.dumps(q.task_settings()))) == q


@pytest.mark.parametrize(
    "text, message",
    [
        ("tasks = cloud", "unknown tasks 'cloud'"),
        ("tasks = queue\nscheduler = lsf", "Unknown scheduler 'lsf'"),
        ("max_queued_tasks = many", "is not an integer"),
        ("shared_filesystem = maybe", "is not yes or no"),
        ("bundle_walltime = soon", "is not a time"),
    ],
)
def test_bad_values_are_reported(tmp_path, text, message):
    (tmp_path / "h.ini").write_text(f"[q]\ntype = local\n{text}\n")
    with pytest.raises((RuntimeError, ValueError), match=message):
        load_target(tmp_path, "h")


@pytest.mark.parametrize(
    "text, seconds",
    [
        ("30", 30 * 60),  # SLURM: a bare number is minutes
        ("10:30", 10 * 60 + 30),
        ("04:00:00", 4 * 3600),
        ("1-00:00:00", 86400),
        ("2-12", 2 * 86400 + 12 * 3600),
        ("1-02:30", 86400 + 2 * 3600 + 30 * 60),
    ],
)
def test_parse_time_slurm_forms(text, seconds):
    assert _parse_time(text) == seconds


def test_limits_bare_time_is_minutes():
    """A bare number in a .limits time bound is minutes, as SLURM reads it."""
    from seamm_scheduler.config import FieldLimits

    s = SlurmSection(
        name="q",
        transport="local",
        host=None,
        directives={"time": "30"},
        limits={"time": FieldLimits(maximum="60")},  # 60 minutes
    )
    assert s.merge_overrides({"time": "45"})["time"] == "45"
    assert s.merge_overrides({"time": "00:59:00"})["time"] == "00:59:00"
    with pytest.raises(ValueError, match="exceeds the maximum"):
        s.merge_overrides({"time": "01:01:00"})
