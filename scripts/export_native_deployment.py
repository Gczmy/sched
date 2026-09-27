"""Export private bindings from an independently pinned Step5D contract.

Does not execute imported project code, rewrite contracts, or launch anything.
Run from a source checkout; store output outside tracked files (e.g. .local/).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gsched import native_deployment as bindings
from gsched import native_step5d_protocol as protocol


def export_payload(raw: bytes, *, expected_sha256: str) -> bytes:
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("reviewed contract file digest mismatch")
    contract = json.loads(raw)
    request = contract["request_body_schema"]
    if (request["schema"] != protocol.REQUEST_SCHEMA
            or contract["parent_protocol"]["sha256"] != protocol.PARENT_PROTOCOL_SHA256):
        raise ValueError("unsupported frozen contract")
    rules = request["nested_schemas"]["scheduler_identity_prefix_v1"]["rules"]

    def claim(key, label):
        text = rules[key]
        if not isinstance(text, str) or not text.startswith(label):
            raise ValueError("unsupported frozen identity rule")
        return text[len(label):].replace("{target_phase_profile}", "{phase}")

    value = dict(schema=bindings.SCHEMA,
        project=claim("SCHED_PROJECT", "string exactly "),
        batch_name_template=claim("SCHED_BATCH_ID", "exact Step 5D batch claim "),
        task_id_template=claim("SCHED_TASK_ID", "exact Step 5D task claim "),
        logical_argv_profiles=request["submitted_logical_argv_profiles"])
    root_rule = request["nested_schemas"]["project_root_expectation_v1"]["rules"]["project"]
    if root_rule != "string exactly " + value["project"]:
        raise ValueError("frozen project bindings disagree")
    payload = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    bindings.parse_deployment(payload, expected_sha256=hashlib.sha256(payload).hexdigest())
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--contract-sha256", required=True, help="independently reviewed file digest")
    parser.add_argument("--output", type=Path, required=True, help="new private file; never overwritten")
    args = parser.parse_args()
    try:
        with args.contract.open("rb") as stream:
            raw = stream.read(1048577)
        if len(raw) > 1048576:
            raise ValueError("contract file too large")
        payload = export_payload(raw, expected_sha256=args.contract_sha256)
        # Parent must already exist. O_EXCL also rejects an existing symlink.
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
        parser.exit(1, f"deployment export failed: {type(exc).__name__}\n")
    print(hashlib.sha256(payload).hexdigest())


if __name__ == "__main__":
    main()
