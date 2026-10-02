# -*- coding: utf-8 -*-

"""Tests for seamm_slurm.status."""

import pytest

from seamm_slurm.status import JobStatus, classify


@pytest.mark.parametrize(
    "raw_state,expected",
    [
        ("PENDING", "pending"),
        ("CONFIGURING", "pending"),
        ("RUNNING", "running"),
        ("COMPLETING", "running"),
        ("SUSPENDED", "running"),
        ("COMPLETED", "completed"),
        ("CANCELLED", "cancelled"),
        ("CANCELLED by 1234", "cancelled"),
        ("FAILED", "failed"),
        ("TIMEOUT", "failed"),
        ("NODE_FAIL", "failed"),
        ("OUT_OF_MEMORY", "failed"),
        ("SOME_FUTURE_STATE", "unknown"),
        ("", "unknown"),
    ],
)
def test_classify(raw_state, expected):
    assert classify(raw_state) == expected


@pytest.mark.parametrize(
    "category,expected_terminal",
    [
        ("pending", False),
        ("running", False),
        ("completed", True),
        ("cancelled", True),
        ("failed", True),
        ("unknown", False),
    ],
)
def test_job_status_is_terminal(category, expected_terminal):
    status = JobStatus(job_id="1", state="X", category=category)
    assert status.is_terminal is expected_terminal
