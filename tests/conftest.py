"""Synthetic bindings used by public protocol tests; never loaded by production."""
import hashlib
from pathlib import Path

import pytest

from gsched.native_deployment import parse_deployment


@pytest.fixture
def deployment():
    payload = (Path(__file__).resolve().parents[1]
               / "examples/native-deployment.example.json").read_bytes()
    return parse_deployment(payload, expected_sha256=hashlib.sha256(payload).hexdigest())
