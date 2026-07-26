# -*- coding: utf-8 -*-
"""
Headed-browser fallback for one public official profile page.

Use this when the normal requests-based collector cannot open a public page
because of protocol/proxy instability, while a visible browser can load it.
The script reuses run_official_profile_collector parsing, registry, file
layout, and metadata writing.
"""

from __future__ import annotations

import argparse
import base64
import sys
from pathlib import Path
from typing import Any, Dict, List

from lxml import html
from PIL import Image
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scraper.run_official_profile_collector import (  # noqa: E402
    REGION_CONFIGS,
    OfficialCollector,
    QueueItem,
    body_text,
    domain_of,
    extract_title,
    sha1_text,
    sha256_file,
)


def default_edge_path() -> str:
    candidates = (
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    )
    for item in candidates:
        if Path(item).exists():
            return item
    return candidates[0]


def fetch_image_in_page(page: Any, image_url: str) -> Dict[str, Any]:
    return page.evaluate(
        """async (url) => {
            const res = await fetch(url, { credentials: 'include', cache: 'no-store' });
            if (!res.ok) {
                return { ok: false, status: res.status, contentType: res.headers.get('content-type') || '' };
            }
            const buf = await res.arrayBuffer();
            let binary = '';
            const bytes = new Uint8Array(buf);
            const chunk = 0x8000;
            for (let i = 0; i < bytes.length; i += chunk) {
                binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
            }
            return {
                ok: true,
                status: res.status,
                contentType: res.headers.get('content-type') || '',
                base64: btoa(binary)
            };
        }""",
        image_url,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect one official profile page with a visible browser.")
    parser.add_argument("--region", choices=sorted(REGION_CONFIGS), default="shandong")
    parser.add_argument("--url", required=True)
    parser.add_argument("--province", default="")
    parser.add_argument("--city", default="")
    parser.add_argument("--county", default="")
    parser.add_argument("--administrative-level", default="agency")
    parser.add_argument("--unit-name", required=True)
    parser.add_argument("--output-root", default="")
    parser.add_argument("--edge-path", default=default_edge_path())
    parser.add_argument("--profile-dir", default=str(PROJECT_ROOT / "data" / "browser_profiles" / "official_browser_once"))
    parser.add_argument(
        "--portrait-url-contains",
        action="append",
        default=[],
        help="Only use page image URLs containing this token. Repeatable. If set, images are paired to parsed people by order.",
    )
    parser.add_argument("--max-people-per-page", type=int, default=80)
    parser.add_argument("--max-images-per-person", type=int, default=8)
    parser.add_argument("--min-image-width", type=int, default=80)
    parser.add_argument("--min-image-height", type=int, default=120)
    parser.add_argument("--max-image-aspect-ratio", type=float, default=3.0)
    parser.add_argument("--timeout", type=int, default=45)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    region_config = REGION_CONFIGS[args.region]
    if not args.output_root:
        args.output_root = str(PROJECT_ROOT / "data" / "public_archive" / region_config["project_name"])

    collector_args = argparse.Namespace(
        region=args.region,
        output_root=args.output_root,
        obey_robots=False,
        timeout=args.timeout,
        max_people_per_page=args.max_people_per_page,
        max_images_per_person=args.max_images_per_person,
        min_image_width=args.min_image_width,
        min_image_height=args.min_image_height,
        max_image_aspect_ratio=args.max_image_aspect_ratio,
        delay_min=2.0,
        delay_max=4.0,
        slow_delay_min=15.0,
        slow_delay_max=35.0,
        block_backoff_minutes=90,
        progress_interval=30,
        export_interval=90,
        max_workers=1,
        max_depth=1,
        max_pages=1,
    )
    collector = OfficialCollector(collector_args)

    profile_dir = Path(args.profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)
    province = args.province or str(region_config["province"])

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            executable_path=args.edge_path,
            headless=False,
            viewport={"width": 1280, "height": 900},
            ignore_https_errors=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(args.url, wait_until="commit", timeout=args.timeout * 1000)
        page.wait_for_load_state("domcontentloaded", timeout=args.timeout * 1000)
        page.wait_for_timeout(5000)

        content = page.content()
        final_url = page.url
        doc = html.fromstring(content)
        doc.make_links_absolute(final_url)
        page_title = extract_title(doc)
        full_text = body_text(doc)

        item = QueueItem(
            url=args.url,
            province=province,
            city=args.city,
            county_or_district=args.county,
            administrative_level=args.administrative_level,
            unit_name=args.unit_name,
            source_unit_url=args.url,
            depth=0,
            parent_url="",
            priority=100,
        )
        people, page_images = collector.extract_people(doc, item, page_title, full_text)

        if args.portrait_url_contains:
            ordered_urls: List[str] = []
            for img in doc.xpath("//img[@src]"):
                src = str(img.get("src") or "")
                if all(token in src for token in args.portrait_url_contains):
                    ordered_urls.append(src)
            ordered_urls = list(dict.fromkeys(ordered_urls))[: len(people)]
            for idx, person in enumerate(people):
                person["_image_urls"] = [ordered_urls[idx]] if idx < len(ordered_urls) else []

        print(f"title={page_title} people={len(people)} page_images={len(page_images)}", flush=True)
        images_found = 0

        for person in people:
            image_urls = list(dict.fromkeys(person.get("_image_urls", []) or []))[: args.max_images_per_person]
            image_total = len(image_urls)
            person_id = collector.registry.upsert_person(person)
            ok_count = 0
            for idx, image_url in enumerate(image_urls, start=1):
                try:
                    payload_info = fetch_image_in_page(page, image_url)
                    if not payload_info.get("ok"):
                        print(f"image fetch failed name={person.get('name')} status={payload_info}", flush=True)
                        continue
                    payload = base64.b64decode(str(payload_info["base64"]))
                    content_type = str(payload_info.get("contentType") or "image/jpeg")
                    ext = collector.image_extension(image_url, content_type)
                    out_dir = collector.output_dir_for_person(person)
                    out_path = collector.unique_image_path(out_dir, person["name"], idx, ext)
                    out_path.write_bytes(payload)

                    width = height = 0
                    usable = 1
                    try:
                        with Image.open(out_path) as im:
                            width, height = im.size
                        if width < args.min_image_width or height < args.min_image_height:
                            usable = 0
                        ratio = max(width / max(height, 1), height / max(width, 1))
                        if ratio > args.max_image_aspect_ratio:
                            usable = 0
                    except Exception:
                        usable = 0

                    if not usable:
                        out_path.unlink(missing_ok=True)
                        continue

                    digest = sha256_file(out_path)
                    asset_id = sha1_text(f"{person_id}|{image_url}|official_photo|{idx}|{digest}")
                    out_path = collector.write_image_metadata(out_path, person, person_id, asset_id, image_url, idx, image_total)
                    digest = sha256_file(out_path) if out_path.exists() else digest
                    stored_content_type = "image/jpeg" if out_path.suffix.lower() in {".jpg", ".jpeg"} else content_type
                    collector.registry.upsert_asset(
                        {
                            "asset_id": asset_id,
                            "person_id": person_id,
                            "name": person["name"],
                            "image_type": "official_photo" if idx == 1 else "material_photo",
                            "image_index": idx,
                            "image_total": image_total,
                            "image_path": str(out_path),
                            "image_url": image_url,
                            "source_url": args.url,
                            "source_page_title": person.get("source_page_title", page_title),
                            "source_site_domain": domain_of(args.url),
                            "caption_or_alt": "",
                            "sha256": digest,
                            "content_type": stored_content_type,
                            "width": width,
                            "height": height,
                            "is_usable": 1,
                            "notes": "browser_fetch",
                        }
                    )
                    ok_count += 1
                    images_found += 1
                    print(f"saved name={person['name']} path={out_path}", flush=True)
                except Exception as exc:
                    print(f"image error name={person.get('name')} url={image_url} error={exc!r}", flush=True)

            if ok_count <= 0:
                person["person_id"] = person_id
                person["image_status"] = "failed_image"
                person["has_official_photo"] = 0
                person["primary_image_path"] = ""
                collector.registry.upsert_person(person)

        collector.registry.mark_queue(args.url, "done", "browser_fallback", 200)
        collector.registry.record_visit(args.url, "done", 200, page_title, len(people), images_found, 0, "browser_fallback")
        collector.registry.export_tables()
        ctx.close()

    print(f"downloaded={images_found}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
