import tempfile
import unittest
from pathlib import Path

from PIL import Image

from titi_metadata_schema import (
    TITI_META_SCHEMA_VERSION,
    build_titi_meta,
    compute_titi_content_hash,
    stable_source_ref,
    validate_titi_meta,
)


class TitiMetadataV23Tests(unittest.TestCase):
    def test_new_write_upgrades_v1_and_preserves_unknown_fields(self):
        meta = build_titi_meta(
            {
                "person": "张三",
                "gender": "男",
                "city": "济南市",
                "source": "https://example.com/people/zhang-san",
                "titi_content_hash": "a" * 64,
                "d2i_profile": {
                    "unit": "示例单位",
                    "full_content": "不得进入图片内嵌层的长正文",
                },
                "photo_audit": {"status": "pending", "file_status": "available"},
            },
            existing_json={
                "schema": "titi-meta",
                "schema_version": 1,
                "component": "forge",
                "unknown_extension": {"keep": True},
            },
        )
        self.assertEqual(meta["schema_version"], TITI_META_SCHEMA_VERSION)
        self.assertEqual(meta["component"], "d2i")
        self.assertEqual(meta["unknown_extension"], {"keep": True})
        self.assertEqual(meta["people_profile"]["name"], "张三")
        self.assertEqual(meta["people_profile"]["gender"], "男")
        self.assertEqual(meta["photo_audit"]["archive_gender_bucket"], "男")
        self.assertNotIn("full_content", meta["d2i_profile"])
        self.assertEqual(meta["titi_content_hash"], "sha256:" + "a" * 64)

    def test_pixel_content_hash_is_stable_across_file_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.png"
            image = Image.new("RGB", (3, 2), (12, 34, 56))
            image.save(path, format="PNG")
            before = compute_titi_content_hash(path)
            image.save(path, format="PNG", pnginfo=None)
            after = compute_titi_content_hash(path)
        self.assertEqual(before, after)
        self.assertRegex(before or "", r"^sha256:[0-9a-f]{64}$")

    def test_validation_separates_portable_from_archive_ready(self):
        meta = build_titi_meta(
            {
                "person": "李四",
                "source": "https://example.com/li-si",
                "titi_content_hash": "b" * 64,
                "photo_audit": {"status": "pending", "file_status": "available"},
            }
        )
        self.assertTrue(validate_titi_meta(meta, "portable")["ok"])
        archive = validate_titi_meta(meta, "archive-ready")
        self.assertFalse(archive["ok"])
        self.assertIn("photo_audit_not_reviewed", archive["errors"])

    def test_stable_source_ref(self):
        url = "https://example.com/people/1"
        self.assertEqual(stable_source_ref(url), stable_source_ref(url))
        self.assertTrue(stable_source_ref(url).startswith("src_"))


if __name__ == "__main__":
    unittest.main()
