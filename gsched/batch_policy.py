"""Explicit batch failure isolation; no task DAG or execution authority."""
from __future__ import annotations

FAILURE_POLICIES = ("freeze", "continue_independent")


def validate_failure_policy(value):
    if not isinstance(value, str) or value not in FAILURE_POLICIES:
        raise ValueError("failure_policy 必须是 freeze 或 continue_independent")
    return value


def failure_policy(batch):
    # Old read-only schemas remain readable without migration or reopening.
    value = batch["failure_policy"] if "failure_policy" in batch.keys() else "freeze"
    return validate_failure_policy(value)
