"""Read-only privacy, Markdown-reference and diff checks (Python >= 3.10).

Kept identical in sched and dsh-node-sched. --all reads tracked working files;
--staged reads changed index blobs and checks Markdown against the full index.
Diagnostics contain locations and rule names only, never matching source text.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit


VERSION = 1
MAX_FILE_BYTES = 8 * 1024 * 1024
POLICY_PATH = ".repository-check.json"
EXAMPLE_USERS = {"user", "example", "tester", "test", "runner"}
HOME_PATH = re.compile(r"(?:/(?:home|Users)/|[A-Za-z]:/Users/)([A-Za-z0-9_.@-]+)", re.I)
PATTERNS = {
    "private-key": re.compile(r"-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----"),
    "github-token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})\b"),
    "aws-access-key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "service-token": re.compile(r"\b(?:sk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{32,}|xox[baprs]-[A-Za-z0-9-]{24,})\b"),
    "credential-url": re.compile(r"[a-z][a-z0-9+.-]*://[^\s/:<>\"']+:[^\s/@<>\"']{8,}@", re.I),
    "screen-session": re.compile(r"\bscreen\s+(?:-d\s+)?-r\s+[0-9]{4,}\.[A-Za-z0-9_-]+"),
}
CONTENT_RULES = frozenset(PATTERNS) | {"personal-path"}
PRIVATE_DIRS = {".local", ".ssh", ".sched", ".dsh", "logs", "tmp"}
PRIVATE_NAMES = {"config.local.json", "state.db", "id_rsa", "id_ed25519", "id_ecdsa"}
PRIVATE_SUFFIX = re.compile(r"\.(?:pem|key|log|db|sqlite|sqlite3)(?:-wal|-shm)?$", re.I)
INLINE_LINK = re.compile(r"!?\[[^\]\n]*\]\(\s*(<[^>\n]+>|[^\s)]+)(?:\s+[\"'][^\n]*?[\"'])?\s*\)")
REFERENCE_LINK = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*(<[^>]+>|\S+)")
DOC_REFERENCE = re.compile(r"`(docs/[A-Za-z0-9_./-]+)`")


class CheckError(RuntimeError):
    pass


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    line: int
    rule: str


def git(root: Path, *args: str, data: bytes | None = None) -> bytes:
    result = subprocess.run(["git", "-C", str(root), *args], input=data,
                            capture_output=True, check=False)
    if result.returncode:
        raise CheckError("Git query failed; check repository access and merge conflicts")
    return result.stdout


def index_entries(root: Path) -> dict[str, tuple[str, str]]:
    entries = {}
    for record in git(root, "ls-files", "--stage", "-z").split(b"\0"):
        if not record:
            continue
        meta, raw_path = record.split(b"\t", 1)
        mode, oid, stage = meta.decode("ascii").split()
        if stage != "0":
            raise CheckError("Resolve unmerged index entries before checking")
        entries[os.fsdecode(raw_path)] = (mode, oid)
    return entries


def index_blobs(root: Path, entries: dict, paths: set[str]) -> dict[str, bytes]:
    # Bound each object before reading it. IDs come only from ls-files.
    selected = sorted(p for p in paths if entries[p][0] != "160000")
    if not selected:
        return {}
    query = ("\n".join(entries[p][1] for p in selected) + "\n").encode()
    headers = git(root, "cat-file", "--batch-check", data=query).splitlines()
    if len(headers) != len(selected):
        raise CheckError("Incomplete Git index snapshot")
    blobs, bounded = {}, []
    oversized = None
    for path, header in zip(selected, headers):
        oid, kind, length = header.split()
        if kind != b"blob" or oid.decode() != entries[path][1]:
            raise CheckError("Unexpected Git object in index")
        if int(length) > MAX_FILE_BYTES:
            if oversized is None:
                oversized = b"\0" * (MAX_FILE_BYTES + 1)
            blobs[path] = oversized
        else:
            bounded.append(path)
    if not bounded:
        return blobs
    raw = git(root, "cat-file", "--batch", data=("\n".join(entries[p][1] for p in bounded) + "\n").encode())
    offset = 0
    for path in bounded:
        end = raw.index(b"\n", offset)
        oid, kind, length = raw[offset:end].split()
        size = int(length)
        if kind != b"blob" or oid.decode() != entries[path][1]:
            raise CheckError("Unexpected Git object in index")
        offset = end + 1
        blobs[path] = raw[offset:offset + size]
        offset += size + 1
    if offset != len(raw):
        raise CheckError("Incomplete Git index snapshot")
    return blobs


def working_blobs(root: Path, entries: dict, paths: set[str]) -> dict[str, bytes]:
    result = {}
    for path in sorted(paths):
        if entries[path][0] == "160000":
            continue
        local = root / path
        # Never follow a tracked symlink into private files outside the repository.
        if local.is_symlink():
            result[path] = os.fsencode(os.readlink(local))
        else:
            if not local.resolve().is_relative_to(root):
                raise CheckError("Tracked path resolves outside the repository")
            with local.open("rb") as stream:
                result[path] = stream.read(MAX_FILE_BYTES + 1)
    return result


def private_filename(path: str) -> bool:
    parts = PurePosixPath(path).parts
    name = parts[-1].lower()
    return (any(p.lower() in PRIVATE_DIRS for p in parts[:-1])
            or path.startswith(("docs/operations/", "results/diagnostics/"))
            or name in PRIVATE_NAMES or bool(PRIVATE_SUFFIX.search(name))
            or (name.startswith(".env") and name not in (".env.example", ".env.template")))


def content_findings(path: str, raw: bytes) -> list[tuple[Finding, str]]:
    findings = []
    # Decode with replacement so non-UTF8 files cannot hide ASCII key headers.
    for number, line in enumerate(raw.decode("utf-8", errors="replace").splitlines(), 1):
        normalized = line.replace("\\", "/")
        normalized = re.sub(r"/{2,}", "/", normalized)
        rules = {rule for rule, pattern in PATTERNS.items() if pattern.search(line)}
        if any(m[1].lower() not in EXAMPLE_USERS for m in HOME_PATH.finditer(normalized)):
            rules.add("personal-path")
        fingerprint = hashlib.sha256(line.encode("utf-8")).hexdigest()
        findings.extend((Finding(path, number, rule), fingerprint) for rule in sorted(rules))
    return findings


def load_exceptions(raw: bytes | None, entries: dict) -> set[tuple[str, str, str]]:
    if raw is None:
        return set()
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise CheckError("Duplicate check-policy key")
            result[key] = value
        return result
    try:
        value = json.loads(raw, object_pairs_hook=pairs)
        if type(value) is not dict or set(value) != {"schema_version", "exceptions"}:
            raise ValueError
        if type(value["schema_version"]) is not int or value["schema_version"] != VERSION:
            raise ValueError
        if type(value["exceptions"]) is not list:
            raise ValueError
        allowed = set()
        for item in value["exceptions"]:
            if type(item) is not dict or set(item) != {"path", "rule", "line_sha256", "reason"}:
                raise ValueError
            if (not all(type(v) is str for v in item.values()) or item["path"] not in entries
                    or item["rule"] not in CONTENT_RULES
                    or not re.fullmatch(r"[0-9a-f]{64}", item["line_sha256"])
                    or len(item["reason"].strip()) < 12):
                raise ValueError
            key = (item["path"], item["rule"], item["line_sha256"])
            if key in allowed:
                raise ValueError
            allowed.add(key)
        return allowed
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise CheckError("Invalid .repository-check.json; use exact path/rule/line digest and a reason") from exc


def markdown_findings(path: str, raw: bytes, available: set[str]) -> list[Finding]:
    findings, fence = [], None
    for number, line in enumerate(raw.decode("utf-8", errors="replace").splitlines(), 1):
        marker = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if marker:
            if fence is None:
                fence = marker[1]
            elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                fence = None
            continue
        if fence is not None:
            continue
        targets = [m[1] for m in INLINE_LINK.finditer(line)]
        reference = REFERENCE_LINK.match(line)
        if reference:
            targets.append(reference[1])
        if path == "AGENTS.md":
            targets.extend(DOC_REFERENCE.findall(line))
        for target in targets:
            target = target.strip("<>")
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            decoded = unquote(parsed.path)
            candidate = posixpath.normpath(posixpath.join(posixpath.dirname(path), decoded))
            if (candidate not in available and not any(p.startswith(candidate.rstrip("/") + "/") for p in available)
                    and candidate != "."):
                findings.append(Finding(path, number, "broken-doc-link"))
    return findings


def check(root: Path, *, staged: bool, base: str | None = None) -> tuple[list[Finding], int]:
    entries = index_entries(root)
    selected = set(entries)
    if staged:
        selected &= {os.fsdecode(p) for p in git(root, "diff", "--cached", "--name-only", "--diff-filter=ACMRT", "-z").split(b"\0") if p}
    documents = {p for p in entries if p.lower().endswith(".md") and entries[p][0] != "160000"}
    required = selected | documents | ({POLICY_PATH} if POLICY_PATH in entries else set())
    blobs = (index_blobs if staged else working_blobs)(root, entries, required)
    exceptions = load_exceptions(blobs.get(POLICY_PATH), entries)
    available = set(entries) if staged else {p for p in entries if (root / p).exists() or (root / p).is_symlink()}
    findings = []
    for path in sorted(selected):
        if private_filename(path):
            findings.append(Finding(path, 0, "private-runtime-file"))
        if entries[path][0] == "160000":
            findings.append(Finding(path, 0, "unscanned-submodule"))
            continue
        raw = blobs[path]
        if len(raw) > MAX_FILE_BYTES:
            findings.append(Finding(path, 0, "oversize-file"))
            continue
        for finding, digest in content_findings(path, raw):
            if (path, finding.rule, digest) not in exceptions:
                findings.append(finding)
    for path in sorted(documents):
        if len(blobs[path]) > MAX_FILE_BYTES:
            findings.append(Finding(path, 0, "oversize-file"))
        else:
            findings.extend(markdown_findings(path, blobs[path], available))
    diff = ["git", "-C", str(root), "diff", "--check"]
    if staged:
        diff.append("--cached")
    elif base:
        if not re.fullmatch(r"[0-9a-fA-F]{40,64}", base):
            raise CheckError("--base must be a full Git commit ID")
        git(root, "cat-file", "-e", base + "^{commit}")
        diff.extend([base, "HEAD"])
    result = subprocess.run(diff, capture_output=True)
    if result.returncode:
        if result.returncode not in (1, 2):
            raise CheckError("Git diff check failed")
        findings.append(Finding(".", 0, "diff-whitespace"))
    return sorted(set(findings)), len(selected)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--all", action="store_true", help="scan all tracked working files")
    mode.add_argument("--staged", action="store_true", help="scan changed Git index blobs")
    parser.add_argument("--base", help="also check committed diff from this full SHA to HEAD")
    args = parser.parse_args()
    if args.staged and args.base:
        parser.error("--base is only supported with --all")
    try:
        root = Path(os.fsdecode(git(Path.cwd(), "rev-parse", "--show-toplevel")).strip()).resolve()
        findings, count = check(root, staged=args.staged, base=args.base)
    except (CheckError, OSError, ValueError) as exc:
        # OSError can contain a private absolute path: do not print its message.
        print(str(exc) if isinstance(exc, CheckError) else "Repository input could not be read", file=sys.stderr)
        return 2
    for finding in findings:
        location = json.dumps(finding.path, ensure_ascii=True)
        print(f"{location}:{finding.line}: {finding.rule}")
    print(f"Repository check: {count} files, {len(findings)} findings")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
