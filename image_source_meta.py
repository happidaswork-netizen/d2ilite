# -*- coding: utf-8 -*-
"""Source-image EXIF/audit helpers (no scrapy / cloud imports).

Contract (Hermes original+EXIF P0):
- Never invent shooting time. Empty + exif_present=false when source has none.
- photo_taken_at_source is only ``source_exif`` or ``unknown``.
- Prefer reading original bytes under downloads/images (sha path), not re-encoded named copies.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

PathLike = Union[str, Path]

# Pillow EXIF tags
_TAG_DATETIME_ORIGINAL = 36867
_TAG_DATETIME_DIGITIZED = 36868
_TAG_DATETIME = 306
_TAG_MAKE = 271
_TAG_MODEL = 272
_TAG_SOFTWARE = 305
_TAG_ORIENTATION = 274

_DATE_PATTERNS = (
    re.compile(r"(?P<y>20\d{2}|19\d{2})[:\-/](?P<m>\d{1,2})[:\-/](?P<d>\d{1,2})(?:[ T](?P<H>\d{1,2}):(?P<M>\d{1,2})(?::(?P<S>\d{1,2}))?)?"),
    re.compile(r"(?P<y>20\d{2}|19\d{2})(?P<m>\d{2})(?P<d>\d{2})(?:[ T_]?(?P<H>\d{2})(?P<M>\d{2})(?P<S>\d{2})?)?"),
)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: PathLike) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_date_token(value: Any) -> Tuple[str, Optional[datetime]]:
    text = str(value or "").strip()
    if not text:
        return "", None
    # EXIF classic "YYYY:MM:DD HH:MM:SS"
    text = text.replace("\x00", " ").strip()
    for pat in _DATE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        try:
            y = int(m.group("y"))
            mo = int(m.group("m"))
            d = int(m.group("d"))
            H = int(m.group("H") or 0)
            M = int(m.group("M") or 0)
            S = int(m.group("S") or 0)
            if not (1800 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31):
                continue
            dt = datetime(y, mo, d, H, M, S)
            if H or M or S:
                return dt.strftime("%Y-%m-%d %H:%M:%S"), dt
            return dt.strftime("%Y-%m-%d"), dt
        except Exception:
            continue
    return "", None


def _decode_exif_value(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, bytes):
        for enc in ("utf-8", "utf-16", "latin-1"):
            try:
                return val.decode(enc, errors="ignore").strip("\x00").strip()
            except Exception:
                continue
        return ""
    if isinstance(val, tuple) and len(val) == 1:
        return _decode_exif_value(val[0])
    return str(val).strip()


def inspect_source_image(
    source: PathLike | bytes,
    *,
    expected_sha256: str = "",
) -> Dict[str, Any]:
    """Return audit fields for one original image (path or raw bytes)."""
    result: Dict[str, Any] = {
        "ok": False,
        "path": "",
        "sha256": "",
        "byte_size": 0,
        "width": 0,
        "height": 0,
        "format": "",
        "exif_present": False,
        "source_photo_taken_at": "",
        "photo_taken_at_source": "unknown",
        "source_exif": {},
        "error": "",
    }

    payload: Optional[bytes] = None
    path: Optional[Path] = None
    if isinstance(source, (bytes, bytearray)):
        payload = bytes(source)
        result["byte_size"] = len(payload)
        result["sha256"] = sha256_bytes(payload)
    else:
        path = Path(source)
        result["path"] = str(path)
        if not path.is_file():
            result["error"] = "file_missing"
            return result
        try:
            result["byte_size"] = int(path.stat().st_size)
            result["sha256"] = sha256_file(path)
        except OSError as exc:
            result["error"] = f"stat:{exc}"
            return result

    expected = str(expected_sha256 or "").strip().lower().removeprefix("sha256:")
    if expected and result["sha256"] and result["sha256"].lower() != expected:
        result["error"] = "sha256_mismatch"
        # still try EXIF so callers can see both problems

    try:
        from PIL import Image
        from io import BytesIO

        opener = BytesIO(payload) if payload is not None else path
        with Image.open(opener) as img:  # type: ignore[arg-type]
            result["format"] = str(img.format or "").upper()
            result["width"] = int(img.width or 0)
            result["height"] = int(img.height or 0)
            exif_obj = None
            try:
                exif_obj = img.getexif()
            except Exception:
                exif_obj = None
            source_exif: Dict[str, Any] = {}
            taken = ""
            taken_dt = None
            if exif_obj:
                for tag, key in (
                    (_TAG_DATETIME_ORIGINAL, "DateTimeOriginal"),
                    (_TAG_DATETIME_DIGITIZED, "DateTimeDigitized"),
                    (_TAG_DATETIME, "DateTime"),
                    (_TAG_MAKE, "Make"),
                    (_TAG_MODEL, "Model"),
                    (_TAG_SOFTWARE, "Software"),
                    (_TAG_ORIENTATION, "Orientation"),
                ):
                    raw = exif_obj.get(tag)
                    if raw is None:
                        continue
                    text = _decode_exif_value(raw)
                    if not text:
                        continue
                    source_exif[key] = text
                    if key in {"DateTimeOriginal", "DateTimeDigitized", "DateTime"} and not taken:
                        taken, taken_dt = parse_date_token(text)

                # any non-empty EXIF tag ⇒ present (even without date)
                if source_exif or len(exif_obj.keys()) > 0:
                    result["exif_present"] = True

            info_map = getattr(img, "info", {}) if isinstance(getattr(img, "info", {}), dict) else {}
            if not taken:
                for key in (
                    "date:create",
                    "date:modify",
                    "creation_time",
                    "Creation Time",
                    "DateTimeOriginal",
                    "DateTime",
                ):
                    if key in info_map:
                        taken, taken_dt = parse_date_token(info_map.get(key))
                        if taken:
                            source_exif.setdefault(key, _decode_exif_value(info_map.get(key)))
                            result["exif_present"] = True
                            break
            if not taken:
                for xmp_key in ("xmp", "XML:com.adobe.xmp", "Raw profile type xmp"):
                    if xmp_key not in info_map:
                        continue
                    raw = info_map.get(xmp_key)
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="ignore")
                    taken, taken_dt = parse_date_token(raw)
                    if taken:
                        result["exif_present"] = True
                        source_exif["xmp_date"] = taken
                        break

            result["source_exif"] = source_exif
            if taken:
                result["source_photo_taken_at"] = taken
                result["photo_taken_at_source"] = "source_exif"
                result["exif_present"] = True
            # keep taken_dt unused but available for callers via re-parse
            _ = taken_dt
    except Exception as exc:  # noqa: BLE001
        if not result["error"]:
            result["error"] = f"decode:{type(exc).__name__}:{exc}"
        return result

    if not result["error"]:
        result["ok"] = True
    return result


def enrich_download_manifest_row(row: Dict[str, Any], *, image_path: PathLike = "") -> Dict[str, Any]:
    """Fill EXIF audit fields onto an image_downloads.jsonl row (in place + return)."""
    out = row if isinstance(row, dict) else {}
    path = str(image_path or out.get("saved_path") or out.get("named_path") or "").strip()
    expected = str(out.get("sha256") or out.get("image_sha256") or "").strip()
    if not path:
        out.setdefault("exif_present", False)
        out.setdefault("source_photo_taken_at", "")
        out.setdefault("photo_taken_at_source", "unknown")
        out.setdefault("source_exif", {})
        return out
    info = inspect_source_image(path, expected_sha256=expected)
    out["exif_present"] = bool(info.get("exif_present"))
    out["source_photo_taken_at"] = str(info.get("source_photo_taken_at") or "")
    out["photo_taken_at_source"] = str(info.get("photo_taken_at_source") or "unknown")
    out["source_exif"] = dict(info.get("source_exif") or {})
    if info.get("width"):
        out["width"] = int(info["width"])
    if info.get("height"):
        out["height"] = int(info["height"])
    if info.get("format"):
        out["image_format"] = str(info["format"])
    if info.get("error"):
        out["source_meta_error"] = str(info["error"])
    if info.get("sha256") and not expected:
        out["sha256"] = str(info["sha256"])
    return out
