"""Boundary checks distinguish harmless history from executable dependencies."""
import unittest

import check_execution_boundary as check


class ExecutionBoundaryTests(unittest.TestCase):
    def rules(self, path, source):
        return {finding.rule for finding in check.python_findings(path, source)}

    def test_generic_lifecycle_and_legacy_storage_keys_are_allowed(self):
        source = 'import os\nfrom .execution import Owner\nTABLE = "native_sessions"\nKEY = "_native_exec_contract_v2"\n'
        self.assertEqual(set(), self.rules("gsched/_legacy_execution.py", source))
        self.assertEqual(set(), self.rules("gsched/execution/core.py", '# historical M2B / MPC_OTSF\nimport hashlib\n'))
        self.assertEqual(set(), self.rules("gsched/_legacy_execution.py", '"""Historical MPC_OTSF import is no longer supported."""\n'))

    def test_static_and_dynamic_customer_imports_are_rejected(self):
        for source in ('from mpc_otsf.adapter import launch\n',
                       'from . import native_step5d_protocol\n',
                       'import importlib\nimportlib.import_module("mpcotsf_execution.adapter")\n'):
            with self.subTest(source=source):
                self.assertIn("project-protocol-import", self.rules("gsched/execution/core.py", source))
        self.assertIn("runtime-dynamic-import", self.rules("gsched/core.py", 'import importlib\nimportlib.import_module(request.module)\n'))
        self.assertIn("runtime-external-import", self.rules("gsched/core.py", 'import importlib\nimportlib.import_module("customer_backend")\n'))
        self.assertIn("runtime-plugin-discovery", self.rules("gsched/core.py", 'from importlib.metadata import entry_points\nentry_points(group="customer.backends")\n'))

    def test_protocol_constants_and_scientific_phase_enum_are_rejected(self):
        self.assertIn("project-protocol-constant", self.rules("gsched/core.py", 'MAGIC = "M2BNLC01"\n'))
        self.assertIn("project-phase-enum", self.rules("gsched/core.py", 'PHASES = ("aggregation", "preparation", "raw_collection")\n'))

    def test_external_contract_and_sibling_fixtures_are_rejected(self):
        source = 'import os\nroot = os.environ["M2B_STEP5E_CONTRACT_ROOT"]\n'
        self.assertIn("external-contract-root", self.rules("tests/test_vector.py", source))
        source = 'from pathlib import Path\nPath(__file__).resolve().parents[2].joinpath("research", "vectors.json").read_bytes()\n'
        self.assertIn("external-source-fixture", self.rules("tests/test_vector.py", source))
        self.assertIn("external-source-fixture", self.rules("setup.py", 'from pathlib import Path\nPath("../customer/native.c").read_text()\n'))
        self.assertEqual(set(), self.rules("tests/test_local.py", 'from pathlib import Path\nPath(__file__).resolve().parents[1].joinpath("tests", "fixture.json").read_bytes()\n'))

    def test_namespace_injection_aliases_are_rejected(self):
        for source in ('import sys as s\ns.modules["gsched.execution.hidden"] = module\n',
                       'import gsched as g\ng.__path__.append("./project")\n',
                       'import gsched\ngsched.__path__ = ["./project"]\n',
                       'import gsched as g\nsetattr(g, "__path__", ["./project"])\n',
                       'from sys import modules as m\nm.update({"gsched.hidden": module})\n'):
            with self.subTest(source=source):
                self.assertIn("scheduler-namespace-injection", self.rules("tests/test_hook.py", source))

    def test_native_and_ci_requirements_are_checked(self):
        findings = check.text_findings("native/worker.c", 'const char *schema = "m2b_request/v1";\n')
        self.assertIn("project-protocol-source", {item.rule for item in findings})
        findings = check.text_findings(".github/workflows/ci.yml", '  repository: owner/customer\n')
        self.assertIn("required-external-checkout", {item.rule for item in findings})

    def test_customer_build_sources_cannot_be_installed_into_scheduler(self):
        source = 'from setuptools import Extension\nExtension("gsched.execution._hidden", ["../customer/native.c"])\n'
        self.assertIn("external-build-source", self.rules("setup.py", source))
        source = 'from setuptools import setup\nsetup(package_dir={"gsched": "../customer/package"})\n'
        self.assertIn("external-package-namespace", self.rules("setup.py", source))
        findings = check.text_findings("native/worker.c", '#include "../../customer/private.h"\n')
        self.assertIn("external-native-include", {item.rule for item in findings})
        self.assertEqual([], check.text_findings("native/worker.c", '#include "../include/common.h"\n'))


if __name__ == "__main__":
    unittest.main()
