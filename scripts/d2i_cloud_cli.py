#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Entry: python scripts/d2i_cloud_cli.py ..."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cloud.cli import main  # HTTP-only client

if __name__ == "__main__":
    raise SystemExit(main())
