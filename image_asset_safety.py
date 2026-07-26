from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from PIL import Image


FORMAT_EXTENSIONS = {
    "JPEG": ".jpg",
    "PNG": ".png",
    "WEBP": ".webp",
    "GIF": ".gif",
    "BMP": ".bmp",
    "TIFF": ".tiff",
    "HEIC": ".heic",
    "AVIF": ".avif",
}

FORMAT_MIME_TYPES = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "WEBP": "image/webp",
    "GIF": "image/gif",
    "BMP": "image/bmp",
    "TIFF": "image/tiff",
    "HEIC": "image/heic",
    "AVIF": "image/avif",
}

EMBEDDABLE_TITI_FORMATS = {"JPEG", "PNG", "WEBP"}


@dataclass(frozen=True)
class ImageFormatInfo:
    format: str
    extension: str
    mime_type: str
    width: Optional[int]
    height: Optional[int]
    mode: str
    animated: bool
    frame_count: int
    metadata_embeddable: bool


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def detect_image_format_bytes(payload: bytes) -> str:
    head = bytes(payload[:32])
    if head.startswith(b"\xff\xd8\xff"):
        return "JPEG"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if head.startswith((b"GIF87a", b"GIF89a")):
        return "GIF"
    if head.startswith(b"BM"):
        return "BMP"
    if head.startswith((b"II*\x00", b"MM\x00*")):
        return "TIFF"
    if head.startswith(b"RIFF") and len(head) >= 12 and head[8:12] == b"WEBP":
        return "WEBP"
    if len(head) >= 12 and head[4:8] == b"ftyp":
        brand = head[8:12]
        if brand in {b"avif", b"avis"}:
            return "AVIF"
        if brand in {b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1"}:
            return "HEIC"
    return ""


def image_extension_from_bytes(payload: bytes) -> str:
    detected = detect_image_format_bytes(payload)
    if detected not in FORMAT_EXTENSIONS:
        raise ValueError(f"unsupported image signature: {detected or 'unknown'}")
    return FORMAT_EXTENSIONS[detected]


def _format_from_magic(path: Path) -> str:
    with path.open("rb") as handle:
        return detect_image_format_bytes(handle.read(32))


def inspect_image_format(path: str | Path) -> ImageFormatInfo:
    image_path = Path(path)
    if not image_path.is_file():
        raise FileNotFoundError(image_path)

    detected = _format_from_magic(image_path)
    width: Optional[int] = None
    height: Optional[int] = None
    mode = ""
    animated = False
    frame_count = 1
    try:
        with Image.open(image_path) as image:
            pillow_format = str(image.format or "").upper()
            if pillow_format == "JPG":
                pillow_format = "JPEG"
            if detected and pillow_format and detected != pillow_format:
                raise ValueError(
                    f"image signature/decoder mismatch ({detected} != {pillow_format}): {image_path}"
                )
            if not detected:
                detected = pillow_format
            width, height = int(image.width), int(image.height)
            mode = str(image.mode or "")
            frame_count = max(1, int(getattr(image, "n_frames", 1) or 1))
            animated = bool(getattr(image, "is_animated", False) or frame_count > 1)
            image.verify()
    except Exception:
        # Pillow may not have HEIF/AVIF codecs installed; retain their magic-only
        # evidence path. Formats Pillow normally supports must decode and verify.
        if detected not in {"HEIC", "AVIF"}:
            raise ValueError(f"unsupported or invalid image: {image_path}")

    if detected not in FORMAT_EXTENSIONS:
        raise ValueError(f"unsupported image format {detected or 'unknown'}: {image_path}")
    return ImageFormatInfo(
        format=detected,
        extension=FORMAT_EXTENSIONS[detected],
        mime_type=FORMAT_MIME_TYPES[detected],
        width=width,
        height=height,
        mode=mode,
        animated=animated,
        frame_count=frame_count,
        metadata_embeddable=(detected in EMBEDDABLE_TITI_FORMATS and not animated),
    )


def image_file_evidence(path: str | Path) -> Dict[str, Any]:
    image_path = Path(path)
    info = inspect_image_format(image_path)
    payload = asdict(info)
    payload.update(
        {
            "file_name": image_path.name,
            "byte_size": image_path.stat().st_size,
            "file_sha256": "sha256:" + sha256_file(image_path),
        }
    )
    return payload


def atomic_copy_verified(source: str | Path, destination: str | Path) -> Path:
    src = Path(source).resolve()
    dst = Path(destination).resolve()
    if src == dst:
        return dst
    if not src.is_file():
        raise FileNotFoundError(src)
    dst.parent.mkdir(parents=True, exist_ok=True)
    source_hash = sha256_file(src)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{dst.name}.", suffix=".copy", dir=str(dst.parent))
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        shutil.copy2(src, tmp)
        if sha256_file(tmp) != source_hash:
            raise IOError(f"copy hash mismatch: {src}")
        os.replace(tmp, dst)
        if sha256_file(dst) != source_hash:
            raise IOError(f"destination hash mismatch: {dst}")
        return dst
    finally:
        if tmp.exists():
            tmp.unlink()


def atomic_write_bytes_verified(
    destination: str | Path,
    payload: bytes,
    *,
    expected_sha256: str = "",
) -> Path:
    dst = Path(destination).resolve()
    dst.parent.mkdir(parents=True, exist_ok=True)
    expected = str(expected_sha256 or "").strip().lower()
    actual = hashlib.sha256(payload).hexdigest()
    if expected and actual != expected:
        raise IOError(f"payload hash mismatch: {dst}")

    fd, tmp_name = tempfile.mkstemp(prefix=f".{dst.name}.", suffix=".write", dir=str(dst.parent))
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if sha256_file(tmp) != actual:
            raise IOError(f"temporary file hash mismatch: {dst}")
        os.replace(tmp, dst)
        if sha256_file(dst) != actual:
            raise IOError(f"destination hash mismatch: {dst}")
        return dst
    finally:
        if tmp.exists():
            tmp.unlink()


def atomic_write_json(path: str | Path, payload: Dict[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".json", dir=str(destination.parent))
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, destination)
        return destination
    finally:
        if tmp.exists():
            tmp.unlink()


def titi_sidecar_path(image_path: str | Path) -> Path:
    path = Path(image_path)
    return path.with_name(path.name + ".titi.json")
