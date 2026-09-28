"""Deployment substitution must not weaken the frozen protocol checks."""
import copy
from dataclasses import FrozenInstanceError
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

from gsched import native_deployment as bindings
from gsched import native_step5d_protocol as d
from gsched import native_step5e_protocol as e
from gsched import native_step5f_protocol as f


EXAMPLE = Path(__file__).resolve().parents[1] / "examples/native-deployment.example.json"


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def parse(value):
    payload = encode(value)
    return bindings.parse_deployment(payload, expected_sha256=hashlib.sha256(payload).hexdigest())


def request(deployment, phase="preparation"):
    return d.build_request_body(deployment=deployment, target_phase_profile=phase,
        scheduler_identity_prefix=d.scheduler_prefix(deployment=deployment,
            target_phase_profile=phase, run_id="example-run", launch_marker="example-launch"),
        launch_nonce="a" * 64,
        control_peer_expectation=dict(peer_role=d.PEER_ROLE,
            credentials=dict(pid=12002, uid=1000, gid=1000),
            process_identity=dict(boot_id_sha256="b" * 64, pid=12002, start_ticks=1234,
                parent_pid=12001, process_group_id=12001, cgroup_identity_sha256="c" * 64)),
        project_root_expectation=dict(project=deployment.project,
            canonical_absolute_path="/srv/projects/example", st_dev=1, st_ino=2,
            st_mode=stat.S_IFDIR | 0o755, st_uid=1000, st_gid=1000))


def anchor(body):
    subject = body["control_peer_expectation"]["process_identity"]
    issuer = dict(subject, pid=subject["parent_pid"], parent_pid=12000)
    return dict(schema=e.ANCHOR_SCHEMA, scope=e.SCOPE, protocol_sha256=e.CONTRACT_SHA256,
        session_id="example-session", anchor_nonce="d" * 64,
        phase=body["target_phase_profile"], scheduler_identity_prefix=body["scheduler_identity_prefix"],
        issuer_process_identity=issuer, subject_process_identity=subject,
        subject_credentials=body["control_peer_expectation"]["credentials"],
        root_identity=body["project_root_expectation"], authorization=None,
        code_manifest=[dict(relative_path="scripts/example.py", size_bytes=10,
                            sha256="e" * 64, role="reviewed_main_source")])


def f_request(deployment, phase="preparation", nonce="f" * 64):
    return dict(schema=f.SCHEMA, main_revision="1" * 40, scheduler_revision="2" * 40,
        nonce=nonce, session_id="example-session", phase=phase, phase_sha256="3" * 64,
        runtime_manifest_sha256="4" * 64,
        prefix=f.prefix(phase, "example-run", "example-launch", deployment=deployment),
        poison_environment=f.poison_environment())


def test_file_pin_is_required_and_checked_before_parsing(tmp_path):
    path = tmp_path / "deployment.json"
    payload = EXAMPLE.read_bytes()
    path.write_bytes(payload)
    pin = hashlib.sha256(payload).hexdigest()
    loaded = bindings.load_deployment(path, expected_sha256=pin)
    path.write_bytes(payload.replace(b"example", b"changed"))
    with pytest.raises(bindings.NativeDeploymentError, match="digest_mismatch"):
        bindings.load_deployment(path, expected_sha256=pin)
    assert loaded.project == "example"  # The active owner keeps its immutable snapshot.
    with pytest.raises(bindings.NativeDeploymentError, match="digest_required"):
        bindings.parse_deployment(payload, expected_sha256="")


@pytest.mark.parametrize("mutation", ["schema", "unknown", "missing", "project", "batch",
    "task", "phase", "argv", "relative", "nul", "shell", "surrogate", "size", "duplicate"])
def test_invalid_bindings_rejected(mutation):
    value = json.loads(EXAMPLE.read_bytes())
    if mutation == "schema": value["schema"] = "sched_native_deployment/v2"
    if mutation == "unknown": value["extra"] = None
    if mutation == "missing": del value["project"]
    if mutation == "project": value["project"] = True
    if mutation == "batch": value["batch_name_template"] = "batch-{other}"
    if mutation == "task": value["task_id_template"] = "task"
    if mutation == "phase": del value["logical_argv_profiles"]["aggregation"]
    if mutation == "argv": value["logical_argv_profiles"]["preparation"] = "python"
    if mutation == "relative": value["logical_argv_profiles"]["preparation"][0] = "python"
    if mutation == "nul": value["project"] = "bad\x00name"
    if mutation == "shell": value["logical_argv_profiles"]["preparation"][1] = "-c"
    payload = encode(value)
    if mutation == "surrogate": payload = payload.replace(b'"example"', b'"\\ud800"')
    if mutation == "size": payload = b" " * (bindings.MAX_BYTES + 1)
    if mutation == "duplicate": payload = payload[:-1] + b',"project":"example"}'
    with pytest.raises(bindings.NativeDeploymentError):
        bindings.parse_deployment(payload, expected_sha256=hashlib.sha256(payload).hexdigest())


def test_bindings_are_immutable_and_never_implicitly_loaded(deployment, monkeypatch):
    with pytest.raises(FrozenInstanceError):
        deployment.project = "changed"
    with pytest.raises(TypeError):
        deployment.argv("preparation")[0] = "/bin/other"
    monkeypatch.setenv("M2B_NATIVE_DEPLOYMENT_FILE", str(EXAMPLE))
    with pytest.raises(TypeError):
        d.validate_request_body(request(deployment))
    with pytest.raises(bindings.NativeDeploymentError, match="required"):
        d.validate_request_body(request(deployment), deployment=None)


@pytest.mark.parametrize("phase", bindings.PHASES)
def test_synthetic_wire_vectors(deployment, phase):
    expected = json.loads((Path(__file__).parent / "fixtures/native-deployment-v1-vectors.json").read_bytes())[phase]
    body = request(deployment, phase)
    frame = d.encode_request_frame(body, deployment=deployment)
    digests = d.request_digests(frame, deployment=deployment)
    assert digests.request_body_sha256 == expected["step5d_body_sha256"]
    assert digests.request_frame_sha256 == expected["step5d_frame_sha256"]
    external = e.parse_anchor(e.canonical(anchor(json.loads(body))), deployment=deployment)
    envelope_frame = e.encode_frame(e.request_envelope("example-session", "d" * 64, frame, deployment=deployment))
    envelope, embedded, parsed = e.parse_request(envelope_frame, deployment=deployment)
    e.match_external(external, envelope, parsed)
    assert embedded == frame
    assert e.digest(envelope_frame) == expected["step5e_request_sha256"]
    assert e.digest(e.canonical(external)) == expected["step5e_anchor_sha256"]
    raw = f.canonical_request(f_request(deployment, phase), deployment=deployment)
    assert f.parse_request(raw, deployment=deployment)["prefix"] == parsed["scheduler_identity_prefix"]
    assert hashlib.sha256(raw).hexdigest() == expected["step5f_request_sha256"]


@pytest.mark.parametrize("field", ["project", "batch_name_template", "task_id_template", "logical_argv_profiles"])
def test_request_cannot_substitute_administrator_expectations(deployment, field):
    value = json.loads(EXAMPLE.read_bytes())
    if field == "logical_argv_profiles":
        value[field]["preparation"][0] = "/opt/other/bin/python"
    else:
        value[field] = "other-" + value[field]
    other = parse(value)
    foreign_body = request(other)
    with pytest.raises(d.Step5DProtocolViolation):
        d.validate_request_body(foreign_body, deployment=deployment)
    with pytest.raises(e.Step5EProtocolViolation):
        foreign = d.encode_request_frame(foreign_body, deployment=other)
        e.parse_embedded(foreign, deployment=deployment)
    if field != "logical_argv_profiles":
        with pytest.raises(e.Step5EProtocolViolation):
            e.parse_anchor(e.canonical(anchor(json.loads(foreign_body))), deployment=deployment)
        with pytest.raises(f.Step5FProtocolViolation):
            f.validate_request(f_request(other), deployment=deployment)


def test_digest_drift_and_extra_fields_still_rejected(deployment):
    body = json.loads(request(deployment))
    body["submitted_logical_argv_sha256"] = "0" * 64
    with pytest.raises(d.Step5DProtocolViolation, match="request_digest_mismatch"):
        d.validate_request_body(encode(body), deployment=deployment)
    body["deployment"] = json.loads(EXAMPLE.read_bytes())
    with pytest.raises(d.Step5DProtocolViolation, match="request_schema_mismatch"):
        d.validate_request_body(encode(body), deployment=deployment)


def test_step5e_init_uses_cold_bindings(deployment):
    body = json.loads(request(deployment))
    init = e.startup_message("INIT", "example-session", deployment=deployment,
        anchor_nonce="d" * 64, phase=body["target_phase_profile"],
        scheduler_identity_prefix=body["scheduler_identity_prefix"], root_identity=body["project_root_expectation"])
    with pytest.raises((bindings.NativeDeploymentError, e.Step5EProtocolViolation)):
        e.validate_startup(init, "INIT", expected_session="example-session")
    bad = copy.deepcopy(init)
    bad["root_identity"]["project"] = "changed"
    with pytest.raises(e.Step5EProtocolViolation):
        e.validate_startup(bad, "INIT", expected_session="example-session", deployment=deployment)


def test_step5f_owner_preserves_nonce_and_single_use_guards(deployment):
    value = f_request(deployment, nonce=hashlib.sha256(b"synthetic-replay-test").hexdigest())
    owner = f.RequestOwner(deployment=deployment)
    raw = owner.encode_once(value)
    assert f.parse_request(raw, deployment=deployment) == value
    with pytest.raises(f.Step5FProtocolViolation, match="request_already_consumed"):
        owner.encode_once(value)
    with pytest.raises(f.Step5FProtocolViolation, match="nonce_already_consumed"):
        f.RequestOwner(deployment=deployment).encode_once(value)
    with pytest.raises(TypeError):
        f.RequestOwner()


def test_step5d_owner_retains_cold_bindings_without_launch(deployment, tmp_path):
    from gsched.native_step5d_control import NativeStep5DRequestOwner
    root = tmp_path.resolve()
    launcher = root / "launcher"
    launcher.write_bytes(b"synthetic-launcher-not-executed")
    launcher.chmod(0o755)
    (root / "logs").mkdir()
    log = root / "logs/native.log"
    before = set(os.listdir("/proc/self/fd"))
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    launcher_fd = os.open(launcher, os.O_RDONLY)
    log_fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    prepared = None
    try:
        owner = NativeStep5DRequestOwner(deployment=deployment)
        prepared = owner.prepare(target_phase_profile="preparation", run_id="example-run",
            launch_marker="example-launch", project_root_path=str(root), project_root_fd=root_fd,
            launcher_fd=launcher_fd, launcher_sha256=hashlib.sha256(launcher.read_bytes()).hexdigest(),
            log_relative_path="logs/native.log", log_fd=log_fd)
        body = d.validate_request_body(prepared.request_body, deployment=deployment)
        assert body["project_root_expectation"]["project"] == deployment.project
        from gsched._native_step5d_linux import _project_root_expectation
        assert _project_root_expectation(root_fd, root, deployment=deployment) == body["project_root_expectation"]
        assert prepared.plan.logical_submitted_argv == deployment.argv("preparation")
        prepared._revalidate()
    finally:
        if prepared is not None:
            prepared.close()
        for fd in (root_fd, launcher_fd, log_fd):
            os.close(fd)
    assert set(os.listdir("/proc/self/fd")) == before


@pytest.fixture
def step5d_local_log_inputs(deployment, tmp_path):
    root = tmp_path.resolve()
    launcher = root / "launcher"
    launcher.write_bytes(b"synthetic-launcher-not-executed")
    launcher.chmod(0o755)
    (root / "logs").mkdir()
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    launcher_fd = os.open(launcher, os.O_RDONLY)
    try:
        yield root, dict(
            target_phase_profile="preparation", run_id="example-run",
            launch_marker="example-launch", project_root_path=str(root),
            project_root_fd=root_fd, launcher_fd=launcher_fd,
            launcher_sha256=hashlib.sha256(launcher.read_bytes()).hexdigest(),
            log_relative_path="logs/native.log",
        )
    finally:
        os.close(launcher_fd)
        os.close(root_fd)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="retained FD launch requires Linux")
def test_step5d_owner_creates_and_retains_local_log(deployment, step5d_local_log_inputs, monkeypatch):
    from gsched import native_step5d_control as control

    root, inputs = step5d_local_log_inputs
    log = root / "logs/native.log"
    assert not log.exists()
    before = set(os.listdir("/proc/self/fd"))
    source_fds = []
    factory = control._create_step5d_no_data_launch_owner

    def capturing_factory(**kwargs):
        source_fds.append(kwargs["log_fd"])
        return factory(**kwargs)

    monkeypatch.setattr(control, "_create_step5d_no_data_launch_owner", capturing_factory)
    prepared = control.NativeStep5DRequestOwner(deployment=deployment).prepare(**inputs)
    try:
        assert len(source_fds) == 1
        with pytest.raises(OSError):
            os.fstat(source_fds[0])
        assert prepared.plan.log_fd != source_fds[0]
        prepared.plan.validate_live_fds()
        assert log.read_bytes() == b""
        assert stat.S_IMODE(log.stat().st_mode) == 0o600
        assert os.fstat(prepared.plan.log_fd).st_ino == log.stat().st_ino
    finally:
        prepared.close()
    assert set(os.listdir("/proc/self/fd")) == before


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="retained FD launch requires Linux")
def test_step5d_owner_rejects_preexisting_local_log(deployment, step5d_local_log_inputs):
    from gsched.native_launch import NativeLaunchPlanError
    from gsched.native_step5d_control import NativeStep5DRequestOwner

    root, inputs = step5d_local_log_inputs
    log = root / "logs/native.log"
    log.write_bytes(b"existing log must remain")
    before = set(os.listdir("/proc/self/fd"))
    with pytest.raises(NativeLaunchPlanError, match="fresh"):
        NativeStep5DRequestOwner(deployment=deployment).prepare(**inputs)
    assert log.read_bytes() == b"existing log must remain"
    assert set(os.listdir("/proc/self/fd")) == before


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="retained FD launch requires Linux")
def test_step5d_owner_factory_failure_keeps_empty_log_without_fd_leak(
    deployment, step5d_local_log_inputs, monkeypatch,
):
    from gsched import native_step5d_control as control

    root, inputs = step5d_local_log_inputs
    log = root / "logs/native.log"
    before = set(os.listdir("/proc/self/fd"))
    source_fds = []

    def failing_factory(**kwargs):
        source_fds.append(kwargs["log_fd"])
        assert os.fstat(kwargs["log_fd"]).st_size == 0
        raise RuntimeError("injected factory failure")

    monkeypatch.setattr(control, "_create_step5d_no_data_launch_owner", failing_factory)
    with pytest.raises(RuntimeError, match="injected factory failure"):
        control.NativeStep5DRequestOwner(deployment=deployment).prepare(**inputs)
    assert len(source_fds) == 1
    with pytest.raises(OSError):
        os.fstat(source_fds[0])
    assert log.read_bytes() == b""
    assert stat.S_IMODE(log.stat().st_mode) == 0o600
    assert set(os.listdir("/proc/self/fd")) == before


def test_exporter_requires_reviewed_contract_and_preserves_existing_file(deployment, tmp_path):
    from scripts.export_native_deployment import export_payload
    projection = d.alignment_projection(deployment=deployment)["request"]
    contract = dict(parent_protocol=dict(sha256=d.PARENT_PROTOCOL_SHA256),
        request_body_schema=dict(schema=d.REQUEST_SCHEMA, nested_schemas=projection["nested_schemas"],
            submitted_logical_argv_profiles=projection["logical_argv_profiles"]))
    raw = encode(contract)
    pin = hashlib.sha256(raw).hexdigest()
    exported = export_payload(raw, expected_sha256=pin)
    assert json.loads(exported) == json.loads(EXAMPLE.read_bytes())
    with pytest.raises(ValueError, match="digest mismatch"):
        export_payload(raw + b" ", expected_sha256=pin)
    source, output = tmp_path / "contract.json", tmp_path / "private.json"
    source.write_bytes(raw)
    command = [sys.executable, str(EXAMPLE.parent.parent / "scripts/export_native_deployment.py"),
               "--contract", str(source), "--contract-sha256", pin, "--output", str(output)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == hashlib.sha256(exported).hexdigest()
    assert output.read_bytes() == exported
    assert output.stat().st_mode & 0o777 == 0o600
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert result.returncode == 1
    assert output.read_bytes() == exported
