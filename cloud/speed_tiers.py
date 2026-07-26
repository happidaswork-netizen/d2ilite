# -*- coding: utf-8 -*-
"""Map product speed tiers onto existing crawl.* numeric fields."""

from __future__ import annotations

from typing import Any, Dict, Optional

SPEED_TIER_SAFE = "safe"
SPEED_TIER_STANDARD = "standard"
SPEED_TIER_TURBO = "turbo"
VALID_SPEED_TIERS = {SPEED_TIER_SAFE, SPEED_TIER_STANDARD, SPEED_TIER_TURBO}

# Product defaults from D2I Cloud contract §3.4
SPEED_TIER_CRAWL: Dict[str, Dict[str, Any]] = {
    SPEED_TIER_SAFE: {
        "concurrent_requests": 1,
        "download_delay": 5,
        "autothrottle_start_delay": 5,
        "autothrottle_max_delay": 15,
        "interval_min_seconds": 5,
        "interval_max_seconds": 15,
        "image_interval_min_seconds": 2,
        "image_interval_max_seconds": 5,
        "blocked_statuses": [403, 429, 412, 521],
        "blocked_backoff_hours": 6,
        "suspect_block_consecutive_failures": 3,
        "retry_times": 3,
        "timeout_seconds": 30,
    },
    SPEED_TIER_STANDARD: {
        "concurrent_requests": 1,
        "download_delay": 3,
        "autothrottle_start_delay": 3,
        "autothrottle_max_delay": 10,
        "interval_min_seconds": 3,
        "interval_max_seconds": 8,
        "image_interval_min_seconds": 2,
        "image_interval_max_seconds": 5,
        "blocked_statuses": [403, 429, 412, 521],
        "blocked_backoff_hours": 6,
        "suspect_block_consecutive_failures": 3,
        "retry_times": 3,
        "timeout_seconds": 30,
    },
    SPEED_TIER_TURBO: {
        "concurrent_requests": 2,
        "download_delay": 1,
        "autothrottle_start_delay": 1,
        "autothrottle_max_delay": 5,
        "interval_min_seconds": 1,
        "interval_max_seconds": 3,
        "image_interval_min_seconds": 1,
        "image_interval_max_seconds": 3,
        "blocked_statuses": [403, 429, 412, 521],
        "blocked_backoff_hours": 6,
        "suspect_block_consecutive_failures": 3,
        "retry_times": 2,
        "timeout_seconds": 25,
    },
}


def normalize_speed_tier(value: Any, *, default: str = SPEED_TIER_SAFE) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "安全": SPEED_TIER_SAFE,
        "标准": SPEED_TIER_STANDARD,
        "极速": SPEED_TIER_TURBO,
        "fast": SPEED_TIER_TURBO,
        "slow": SPEED_TIER_SAFE,
    }
    text = aliases.get(text, text)
    if text in VALID_SPEED_TIERS:
        return text
    return default if default in VALID_SPEED_TIERS else SPEED_TIER_SAFE


def apply_speed_tier_to_crawl(
    crawl: Optional[Dict[str, Any]],
    speed_tier: Any,
    *,
    reason: str = "",
    overwrite_existing: bool = False,
) -> Dict[str, Any]:
    data = dict(crawl) if isinstance(crawl, dict) else {}
    requested = str(speed_tier or "").strip()
    # Empty tier = inherit the template's own crawl.speed_tier; final default stays safe.
    tier = normalize_speed_tier(requested or data.get("speed_tier"), default=SPEED_TIER_SAFE)
    defaults = dict(SPEED_TIER_CRAWL[tier])
    for key, value in defaults.items():
        if overwrite_existing or key not in data:
            data[key] = value
    data["speed_tier"] = tier
    if reason:
        data["speed_tier_reason"] = str(reason).strip()
    elif "speed_tier_reason" not in data:
        data["speed_tier_reason"] = ""
    return data
