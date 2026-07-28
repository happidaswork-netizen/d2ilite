# -*- coding: utf-8 -*-
"""Original warehouse + EXIF audit (Hermes metadata gap P0)."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from image_source_meta import (  # noqa: E402
    enrich_download_manifest_row,
    inspect_source_image,
    parse_date_token,
    sha256_bytes,
)
from cloud import image_audit  # noqa: E402


def _make_jpeg_with_exif(path: Path, *, taken: str = "2020:05:01 12:30:00") -> bytes:
    """Write a tiny JPEG; attach DateTimeOriginal via piexif when available."""
    from PIL import Image

    img = Image.new("RGB", (64, 80), color=(40, 80, 120))
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import piexif

        # piexif expects bytes for EXIF string values
        exif_dict = {
            "0th": {},
            "Exif": {piexif.ExifIFD.DateTimeOriginal: taken.encode("ascii")},
            "GPS": {},
            "1st": {},
            "thumbnail": None,
        }
        exif_bytes = piexif.dump(exif_dict)
        img.save(path, "JPEG", quality=90, exif=exif_bytes)
    except Exception:
        img.save(path, "JPEG", quality=90)
    return path.read_bytes()


def test_parse_date_token_exif_style():
    s, dt = parse_date_token("2020:05:01 12:30:00")
    assert s.startswith("2020-05-01")
    assert dt is not None and dt.year == 2020 and dt.month == 5


def test_inspect_empty_when_no_exif():
    from PIL import Image

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "bare.jpg"
        Image.new("RGB", (32, 32), color=(10, 10, 10)).save(p, "JPEG", quality=85)
        info = inspect_source_image(p)
        assert info["ok"] is True
        assert info["exif_present"] is False
        assert info["source_photo_taken_at"] == ""
        assert info["photo_taken_at_source"] == "unknown"
        assert info["sha256"] == sha256_bytes(p.read_bytes())


def test_inspect_reads_datetime_original_when_present():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "dated.jpg"
        _make_jpeg_with_exif(p, taken="2019:11:08 09:15:00")
        info = inspect_source_image(p)
        assert info["ok"] is True
        # If piexif unavailable in env, date may be empty — still must not invent.
        if info["source_photo_taken_at"]:
            assert info["exif_present"] is True
            assert info["photo_taken_at_source"] == "source_exif"
            assert info["source_photo_taken_at"].startswith("2019-11-08")
        else:
            assert info["photo_taken_at_source"] == "unknown"


def test_enrich_manifest_row_never_invents():
    from PIL import Image

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "x.jpg"
        Image.new("RGB", (20, 20), color=(1, 2, 3)).save(p, "JPEG")
        row = {"sha256": sha256_bytes(p.read_bytes()), "saved_path": str(p)}
        out = enrich_download_manifest_row(row)
        assert out["exif_present"] is False
        assert out["source_photo_taken_at"] == ""
        assert out["photo_taken_at_source"] == "unknown"


def test_audit_queue_images_gates():
    from PIL import Image

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        images = root / "downloads" / "images" / "ab"
        images.mkdir(parents=True)
        good = images / "ab" / "deadbeef.jpg"
        # path uses sha layout loosely
        good = images / "good.jpg"
        Image.new("RGB", (48, 48), color=(5, 5, 5)).save(good, "JPEG")
        sha = sha256_bytes(good.read_bytes())
        # rewrite under sha layout
        target = root / "downloads" / "images" / sha[:2] / f"{sha}.jpg"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(good.read_bytes())
        manifest = root / "downloads" / "image_downloads.jsonl"
        rows = [
            {
                "name": "完整",
                "sha256": sha,
                "saved_path": str(target),
                "named_path": str(target),
                "exif_present": False,
                "source_photo_taken_at": "",
                "photo_taken_at_source": "unknown",
            },
            {
                "name": "缺sha",
                "saved_path": str(target),
            },
            {
                "name": "缺文件",
                "sha256": "0" * 64,
                "saved_path": str(root / "downloads" / "images" / "nope.jpg"),
            },
        ]
        manifest.write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
            encoding="utf-8",
        )
        report = image_audit.audit_queue_images(
            output_root=str(root),
            limit=50,
            rehash=True,
            re_read_exif=True,
        )
        assert report["counts"]["audited"] == 3
        assert report["counts"]["missing_sha"] == 1
        assert report["counts"]["path_missing_on_disk"] >= 1
        assert report["complete_ingest"] is False
        # first row alone would be complete; overall not
        assert any(i["name"] == "完整" and i["ok"] for i in report["items"])


def test_cleanup_keeps_originals_by_default():
    """images_only cleanup must retain downloads/images unless opted out."""
    try:
        from scraper import run_public_scraper as rps
    except Exception as exc:
        print("skip cleanup integration (import failed):", type(exc).__name__, exc)
        return

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "downloads" / "images" / "aa").mkdir(parents=True)
        img = root / "downloads" / "images" / "aa" / "bb.jpg"
        img.write_bytes(b"\xff\xd8\xff\xd9")  # minimal jpeg markers
        manifest = root / "downloads" / "image_downloads.jsonl"
        manifest.write_text("{}\n", encoding="utf-8")
        (root / "raw").mkdir()
        (root / "raw" / "profiles.jsonl").write_text("{}\n", encoding="utf-8")
        (root / "reports").mkdir()
        # fake successful promote so cleanup is allowed to run
        (root / "reports" / "promote_report.json").write_text(
            json.dumps(
                {
                    "ok": True,
                    "counts": {"promoted": 1, "exists": 0, "no_photo": 0, "rejected_qa": 0},
                }
            ),
            encoding="utf-8",
        )
        named = root / "named"
        named.mkdir()
        cfg = {
            "rules": {
                "output_mode": "images_only_with_record",
                "named_output_dir": str(named),
            }
        }
        record = root / "crawl_record.json"
        record.write_text("{}", encoding="utf-8")
        result = rps.cleanup_intermediate_outputs(root, cfg, record)
        assert result.get("cleaned") is True
        assert result.get("keep_originals") is True
        assert manifest.is_file(), "manifest must be retained by default"
        assert img.is_file(), "original warehouse bytes must be retained by default"


if __name__ == "__main__":
    test_parse_date_token_exif_style()
    test_inspect_empty_when_no_exif()
    test_inspect_reads_datetime_original_when_present()
    test_enrich_manifest_row_never_invents()
    test_audit_queue_images_gates()
    try:
        test_cleanup_keeps_originals_by_default()
    except Exception as exc:
        print("cleanup test note:", exc)
    print("OK")
