#!/usr/bin/env python3
"""
Re-write XMP metadata for ALL Shandong government official photos
following PWI Forge ↔ PWI Titi Metadata Spec v1.0.

Reads existing XMP d2i_profile from each photo, builds complete
A-layer + B-layer metadata, and writes it back.

Usage:
  python3 rewrite_all_metadata.py [--dry-run] [--city 临沂]
"""
import sys, re, json, argparse
from pathlib import Path

B = Path("/sessions/determined-sweet-cannon/mnt/bugemini/d2ilite")
sys.path.insert(0, str(B))

import sqlite3
from metadata_writer import write_xmp_metadata
from metadata_manager import read_image_metadata


def extract_position(name, bio, unit_name):
    """Extract position from biography text."""
    if not bio:
        return ""
    m = re.search(r'现任([^。\n]+)', bio)
    if m:
        pos = m.group(1).strip()
        pos = re.sub(rf'{re.escape(name)}.*$', '', pos).strip()
        pos = re.sub(r'，\s*$', '', pos).strip()
        if pos:
            return pos
    m = re.search(r'主持(.+?)全面工作', bio)
    if m:
        return m.group(1).strip() + "负责人"
    if unit_name and unit_name != "未知":
        return unit_name + "干部"
    return ""


def build_full_metadata(d2i, existing_meta):
    """Build complete metadata dict from d2i_profile + existing XMP."""
    name = d2i.get('name', '')
    unit_name = d2i.get('unit_name', '')
    bio = d2i.get('full_content', '')
    source_url = d2i.get('source_url', '')
    source_site_domain = d2i.get('source_site_domain', '')
    gender = d2i.get('gender', '男')
    city = d2i.get('city', '')
    position = extract_position(name, bio, unit_name)

    title = f"{name} - {position}" if position else name

    keywords = [gender, unit_name, "official_photo"]
    if source_site_domain:
        keywords.append(source_site_domain)
    dept = d2i.get('department', '')
    if dept:
        keywords.append(dept)
    seen = set()
    keywords_unique = []
    for k in keywords:
        if k and k != 'None' and k not in seen:
            seen.add(k)
            keywords_unique.append(k)

    metadata = {
        'title': title,
        'description': bio,
        'keywords': keywords_unique,
        'source': source_url,
        'city': city,
        'person': name,
        'position': position,
        'd2i_profile': d2i,
    }
    if existing_meta.titi_asset_id:
        metadata['titi_asset_id'] = existing_meta.titi_asset_id
    if existing_meta.titi_world_id:
        metadata['titi_world_id'] = existing_meta.titi_world_id
    return metadata, title, position


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='Scan only, no writes')
    parser.add_argument('--city', help='Limit to specific city')
    args = parser.parse_args()

    base = Path("/sessions/determined-sweet-cannon/mnt/bugemini/d2ilite/data/public_archive/山东公开官员/山东省")
    photo_ext = {'.jpg', '.jpeg', '.png'}

    # Load all DB positions
    db_paths = [
        Path("/tmp/dezhou_final_backup.sqlite"),
        B / "data/public_archive/山东公开官员/registry/people.sqlite",
    ]
    db_positions = {}
    for dp in db_paths:
        if dp and dp.exists():
            try:
                conn = sqlite3.connect(str(dp))
                conn.row_factory = sqlite3.Row
                for r in conn.execute("SELECT person_id, position FROM people").fetchall():
                    pid = r['person_id']
                    pos = r['position']
                    if pos and pos != 'None':
                        db_positions[pid] = pos
                conn.close()
                print(f"Loaded {len(db_positions)} positions from {dp.name}")
                break
            except Exception as e:
                print(f"  DB error: {e}")

    # Find photos
    photos = []
    for f in sorted(base.rglob('*')):
        if f.suffix.lower() in photo_ext and f.stat().st_size >= 1000:
            if args.city and args.city not in str(f):
                continue
            photos.append(f)

    print(f"Found {len(photos)} photos" + (f" (city={args.city})" if args.city else ""))

    stats = {'ok': 0, 'skip': 0, 'fail': 0, 'no_d2i': 0}

    for i, photo_path in enumerate(photos, 1):
        rel = photo_path.relative_to(base)
        try:
            existing = read_image_metadata(str(photo_path))
            if not existing.titi_json or 'd2i_profile' not in existing.titi_json:
                stats['no_d2i'] += 1
                if i <= 5:
                    print(f"  [NO-D2I] {rel}")
                continue

            d2i = existing.titi_json['d2i_profile']

            # Supplement position from DB
            pid = d2i.get('person_id', '')
            if pid and pid in db_positions:
                d2i['position'] = db_positions[pid]

            metadata, title, position = build_full_metadata(d2i, existing)

            if args.dry_run:
                if i <= 3:
                    print(f"  [DRY] {rel} -> title={title}")
                stats['ok'] += 1
                continue

            write_xmp_metadata(str(photo_path), metadata)

            if i % 20 == 0 or i == 1:
                ver = read_image_metadata(str(photo_path))
                print(f"  [{i}/{len(photos)}] {rel}")
                print(f"     title={ver.title} pos={ver.position}")

            stats['ok'] += 1

        except Exception as e:
            stats['fail'] += 1
            print(f"  [FAIL] {rel}: {e}")

    print(f"\n{'='*50}")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == '__main__':
    main()
