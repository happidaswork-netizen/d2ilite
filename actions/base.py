# -*- coding: utf-8 -*-
"""Shared action primitives for single-image operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol


@dataclass
class ImageActionContext:
    image_path: str
    metadata: Any | None = None
    app_config: Dict[str, Any] = field(default_factory=dict)
    ui_parent: Any | None = None


@dataclass
class ImageActionResult:
    ok: bool
    message: str
    output_path: Optional[str] = None
    reveal_path: Optional[str] = None
    metadata_updates: Optional[Dict[str, Any]] = None


class ImageAction(Protocol):
    id: str
    label: str

    def run(self, context: ImageActionContext, **options: Any) -> ImageActionResult:
        ...


_REGISTRY: Dict[str, ImageAction] = {}


def register_image_action(action: ImageAction) -> ImageAction:
    _REGISTRY[str(action.id)] = action
    return action


def get_image_action(action_id: str) -> ImageAction | None:
    return _REGISTRY.get(str(action_id or "").strip())
