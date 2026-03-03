# -*- coding: utf-8 -*-
"""Runtime/process utility services."""

from __future__ import annotations

import os
import sys
from typing import Dict


def resolve_python_cli_executable() -> str:
    exe = os.path.abspath(str(sys.executable or "").strip() or "python")
    base = os.path.basename(exe).lower()
    if base == "pythonw.exe":
        candidate = os.path.join(os.path.dirname(exe), "python.exe")
        if os.path.exists(candidate):
            return candidate
    return exe


def build_utf8_subprocess_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return env

