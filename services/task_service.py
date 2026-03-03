# -*- coding: utf-8 -*-
"""Task domain service scaffold for future phase migration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class TaskRuntimeState:
    """Minimal runtime state model for task-oriented panels."""

    task_id: str
    status: str
    message: str = ""
    started_at_ts: Optional[float] = None
    finished_at_ts: Optional[float] = None

