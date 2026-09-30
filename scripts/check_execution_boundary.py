"""Check sched's source and required-test independence without importing runtime code.

Comments and historical Markdown do not define runtime behavior. Python is checked
as AST: imports, executed constants, namespace writes and external fixture paths.
The default scans working source, including new files, without requiring staging.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
import json
from pathlib import Path
import posixpath
import re
import subprocess
import sys


PROJECT_PROTOCOL = re.compile(r"mpc[_-]?otsf|m2b|step[ _-]?5[defg]", re.I)
CONTRACT_ROOT = re.compile(r"(?:^|_)contract_root(?:$|_)", re.I)
RESEARCH_PHASES = {"preparation", "raw_collection", "aggregation"}
SOURCE_SUFFIXES = {".py", ".c", ".h", ".sh", ".yml", ".yaml", ".toml"}


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    line: int
    rule: str


def _name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return _name(node.value) + "." + node.attr
    return ""


def _text(node: ast.AST) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _external_path(node: ast.AST) -> bool:
    """A fixed external source/fixture cannot become a required local input."""
    for item in ast.walk(node):
        value = _text(item)
        if value is not None:
            normalized = value.replace("\\", "/")
            if PROJECT_PROTOCOL.search(value) or CONTRACT_ROOT.search(value) or normalized.startswith("../"):
                return True
        if isinstance(item, ast.Subscript) and _name(item.value).endswith(".parents"):
            if isinstance(item.slice, ast.Constant) and type(item.slice.value) is int and item.slice.value >= 2:
                return True
    return False


def python_findings(path: str, source: str) -> list[Finding]:
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        return [Finding(path, exc.lineno or 1, "source-syntax")]
    runtime = path.startswith("gsched/")
    findings: set[Finding] = set()
    aliases = {"gsched": "gsched", "sys": "sys", "importlib": "importlib"}
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for value in node.names:
                aliases[value.asname or value.name.split(".")[0]] = value.name
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            for value in node.names:
                aliases[value.asname or value.name] = node.module + "." + value.name
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.body and isinstance(node.body[0], ast.Expr) and _text(node.body[0].value) is not None:
                docstrings.add(id(node.body[0].value))

    def resolved(node: ast.AST) -> str:
        name = _name(node)
        first, dot, rest = name.partition(".")
        return aliases.get(first, first) + (dot + rest if dot else "")

    def add(node: ast.AST, rule: str) -> None:
        findings.add(Finding(path, getattr(node, "lineno", 1), rule))

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            modules = ([value.name for value in node.names] if isinstance(node, ast.Import)
                       else [node.module or "", *[value.name for value in node.names]])
            if any(PROJECT_PROTOCOL.search(value) for value in modules):
                add(node, "project-protocol-import")
            if runtime:
                roots = ([value.name.split(".")[0] for value in node.names]
                         if isinstance(node, ast.Import) else ([] if node.level else [(node.module or "").split(".")[0]]))
                if any(value not in sys.stdlib_module_names and value != "gsched" for value in roots):
                    add(node, "runtime-external-import")
        if isinstance(node, (ast.Name, ast.Attribute)) and PROJECT_PROTOCOL.search(_name(node)):
            add(node, "project-protocol-identifier")
        value = _text(node)
        if value is not None and id(node) not in docstrings:
            if PROJECT_PROTOCOL.search(value):
                add(node, "project-protocol-constant")
            if CONTRACT_ROOT.search(value):
                add(node, "external-contract-root")
        if runtime and isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            if RESEARCH_PHASES <= {_text(item) for item in node.elts}:
                add(node, "project-phase-enum")
        if runtime and value == "raw_collection":
            add(node, "project-phase-constant")
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if resolved(target) in {"gsched.__path__", "gsched.__spec__", "gsched.__loader__"}:
                    add(node, "scheduler-namespace-injection")
                if isinstance(target, ast.Subscript) and resolved(target.value) == "sys.modules":
                    module = _text(target.slice)
                    if module == "gsched" or (module and module.startswith("gsched.")):
                        add(node, "scheduler-namespace-injection")
        if isinstance(node, ast.Call):
            name = resolved(node.func)
            if name in {"gsched.__path__.append", "gsched.__path__.extend", "gsched.__path__.insert"}:
                add(node, "scheduler-namespace-injection")
            if name in {"setattr", "builtins.setattr"} and len(node.args) >= 2:
                if resolved(node.args[0]) == "gsched" and _text(node.args[1]) in {"__path__", "__spec__", "__loader__"}:
                    add(node, "scheduler-namespace-injection")
            if name == "sys.modules.update" and node.args and isinstance(node.args[0], ast.Dict):
                if any((_text(key) or "").startswith("gsched.") or _text(key) == "gsched"
                       for key in node.args[0].keys):
                    add(node, "scheduler-namespace-injection")
            if name in {"__import__", "builtins.__import__", "importlib.import_module"}:
                module = _text(node.args[0]) if node.args else None
                if module and PROJECT_PROTOCOL.search(module):
                    add(node, "project-protocol-import")
                if runtime and module is None:
                    add(node, "runtime-dynamic-import")
                elif runtime and module and module.split(".")[0] not in sys.stdlib_module_names | {"gsched"}:
                    add(node, "runtime-external-import")
            if runtime and name == "importlib.metadata.entry_points":
                add(node, "runtime-plugin-discovery")
            if name in {"sys.path.append", "sys.path.insert", "sys.path.extend"}:
                if any(_external_path(argument) for argument in node.args):
                    add(node, "external-search-path")
            if name.endswith((".read_bytes", ".read_text", ".open")) or name in {"open", "Path"}:
                if _external_path(node):
                    add(node, "external-source-fixture")
            if name.endswith(".Extension") or name == "Extension":
                if _external_path(node):
                    add(node, "external-build-source")
                for argument in node.args[1:]:
                    for item in ast.walk(argument):
                        source_path = _text(item)
                        if source_path and (source_path.startswith("/") or re.match(r"^[A-Za-z]:[/\\]", source_path)):
                            add(item, "external-build-source")
            if name in {"setup", "setuptools.setup"}:
                if any(keyword.arg == "package_dir" and _external_path(keyword.value)
                       for keyword in node.keywords):
                    add(node, "external-package-namespace")
    return sorted(findings)


def text_findings(path: str, source: str) -> list[Finding]:
    findings = []
    # Native sources and shell/CI requirements cannot embed a customer protocol.
    for number, line in enumerate(source.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith(("//", "/*", "*")) or (stripped.startswith("#") and not path.endswith((".c", ".h"))):
            continue
        if PROJECT_PROTOCOL.search(line):
            findings.append(Finding(path, number, "project-protocol-source"))
        if CONTRACT_ROOT.search(line):
            findings.append(Finding(path, number, "external-contract-root"))
        if path.startswith(".github/workflows/") and re.search(r"^\s*repository\s*:\s*\S", line):
            findings.append(Finding(path, number, "required-external-checkout"))
        include = re.match(r'\s*#\s*include\s+"([^"]+)"', line) if path.endswith((".c", ".h")) else None
        if include:
            target = include[1].replace("\\", "/")
            relative = posixpath.normpath(posixpath.join(posixpath.dirname(path), target))
            if target.startswith("/") or re.match(r"^[A-Za-z]:", target) or relative.startswith("../"):
                findings.append(Finding(path, number, "external-native-include"))
    return findings


def check(root: Path) -> list[Finding]:
    result = subprocess.run(["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
                            check=True, capture_output=True)
    findings: set[Finding] = set()
    paths = {value.decode("utf-8") for value in result.stdout.split(b"\0") if value}
    for path in sorted(paths):
        if path in {"scripts/check_execution_boundary.py", "scripts/test_execution_boundary.py"}:
            continue
        if not (path.startswith(("gsched/", "native/", "tests/", "scripts/", ".github/workflows/"))
                or path in {"setup.py", "pyproject.toml"}):
            continue
        local = root / path
        if local.suffix not in SOURCE_SUFFIXES or not local.exists():
            continue
        if local.is_symlink() or not local.resolve().is_relative_to(root.resolve()):
            findings.add(Finding(path, 1, "external-source-path"))
            continue
        source = local.read_text(encoding="utf-8")
        findings.update(python_findings(path, source) if local.suffix == ".py"
                        else text_findings(path, source))
    return sorted(findings)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    try:
        findings = check(root)
    except (OSError, UnicodeError, subprocess.SubprocessError):
        print("Execution boundary input could not be read", file=sys.stderr)
        return 2
    for finding in findings:
        print(f"{json.dumps(finding.path)}:{finding.line}: {finding.rule}")
    print(f"Execution boundary: {len(findings)} findings")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
