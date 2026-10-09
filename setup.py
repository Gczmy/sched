"""Pure Python by default; native compilation is an explicit installation choice."""

import os
import sys

from setuptools import Extension, setup

enabled = os.environ.get("SCHED_BUILD_NATIVE") == "1"
if enabled and sys.platform != "linux":
    raise RuntimeError("SCHED_BUILD_NATIVE=1 requires Linux")

setup(options={"build": {"build_base": "build/native" if enabled else "build/python"}},
      exclude_package_data={"gsched.execution": ["_fdexec*.so", "_fdexec*.pyd"]},
      ext_modules=[Extension(
    "gsched.execution._fdexec", ["native/fdexec.c"], depends=["native/device_policy.h"],
    extra_compile_args=["-std=c11", "-Wall", "-Wextra", "-Werror"],
)] if enabled else [])
