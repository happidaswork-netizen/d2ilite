#!/usr/bin/env python3
"""东营市政府领导专采"""
import argparse, hashlib, json, re, sqlite3, sys, time, csv
from datetime import datetime
from pathlib import Path
import requests

PROJ = Path("/sessions/determined-sweet-cannon/mnt/bugemini/d2ilite")
PUB = PROJ / "data" / "public_archive" / "山东公开官员"
DB = Path("/tmp/linyi_work/registry/people.sqlite")

sys.path.insert(0, str(PROJ))
from metadata_writer import build_titi_json, generate_titi_asset_id, write_xmp_metadata

BASE = "http://www.dongying.gov.cn"
PROV, CITY = "山东省", "东营市"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

LEADERS = [
    ("市委常委、副市长", "杨同贤", "/col/col253814/index.html", "/picture/0/e6017a38b7dc403eac99047d43af923d.jpg"),
    ("副市长", "梁润生", "/col/col253815/index.html", "/picture/0/1c8bc16d71cd48d3b094f9c70d9573da.jpg"),
    ("副市长", "杨勇", "/col/col347446/index.html", "/picture/0/a5628d172fae44f491e59a3c7b5bfc82.jpg"),
    ("副市长", "王秀凤", "/col/col188158/index.html", "/picture/0/9d3d195137e84ce697d717e9be994f4e.jpg"),
    ("副市长", "边志伟", "/col/col311073/index.html", "/picture/0/5b1ed22041fd488fb6b763ab6edc88f1.jpg"),
    ("副市长", "赵海", "/col/col323833/index.html", "/picture/0/88c5092cb6414d56ad07f9e586eb14f8.jpg"),
    ("副市长", "师辉", "/col/col333075/index.html", "/picture/0/594f59138610406f99a8eea28dba2882.jpg"),
    ("副市长", "张莉", "/col/col364842/index.html", "/picture/0/2603021034304179649.png"),
    ("秘书长", "王学春", "/col/col71756/index.html", "/picture/0/s_2502282040004905780.jpg"),
]

def fetch(url):
    r = requests.get(url, headers=HDR, timeout=30); r.encoding = "utf-8"
    return r.status_code, r.text

def extract_bio(htm):
    clean = re.sub(r'<script[^>]*>.*?</script>', '', htm, flags=re.DOTALL)
    clean = re.sub(r'<style[^>]*>.*?</style>', '', clean, flags=re.DOTALL)
    clean = re.sub(r'<[^>]+>', '\n', clean)
    clean = re.sub(r'\n\s*\n', '\n', clean).strip()
    lines = [l.strip() for l in clean.split('\n') if l.strip()]
    bio = ""
    for l in lines:
        if re.search(r'[，,]?\s*(男|女)[，,]?\s*', l) and len(l) > 10:
            bio = l; break
    fg = ""
    for i, l in enumerate(lines):
        if '分工' in l and i+1 < len(lines):
            fg = lines[i+1]; break
    return bio, fg

def gender_from_bio(bio):
    if re.search(r'[，,]\s*男[，,]', bio) or bio.startswith('男'): return "男"
    if re.search(r'[，,]\s*女[，,]', bio) or bio.startswith('女'): return "女"
    return "未知"

def dl(url, path):
    if path.exists() and path.stat().st_size > 1000: return True
    path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, headers=HDR, timeout=30, stream=True)
    if r.status_code != 200 or "image" not in r.headers.get("Content-Type",""):
        return False
    with open(path, "wb") as f: f.write(r.content)
    print("    [OK]", len(r.content), "b ->", path.name); return True

def write_meta(path, pid, aid, name, g, pos, bio, fg, src_url, img_url):
    md = {
        "title": f"{pos} {name}", "description": bio,
        "keywords": [name, pos, CITY, "东营市人民政府"],
        "source": src_url, "city": CITY, "person": name, "titi_asset_id": aid,
        "d2i_profile": {
            "person_id": pid, "asset_id": aid, "name": name, "gender": g,
            "province": PROV, "city": CITY,
            "unit": "东营市人民政府", "unit_name": "东营市人民政府", "position": pos,
            "source_url": src_url, "source_page_title": f"市政府领导 - {name}",
            "source_site_domain": "www.dongying.gov.cn",
            "image_url": img_url, "image_type": "official_photo",
            "full_content": bio, "division_of_work": fg,
            "crawled_at": datetime.now().isoformat(),
        },
    }
    titi = build_titi_json(md)
    write_xmp_metadata(str(path), titi)

def pid(name): return "p_dy_" + hashlib.sha256(("dongying-"+name).encode()).hexdigest()[:16]

def export_tables(conn):
    root = Path("/tmp/linyi_work")
    for table in ["people", "image_assets", "source_units"]:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        cols = [d[0] for d in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        with open(root / "registry" / f"{table}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(cols)
            for r in rows: w.writerow([r[c] for c in cols])
        with open(root / "registry" / f"{table}.jsonl", "w", encoding="utf-8") as f:
            for r in rows: f.write(json.dumps(dict(r), ensure_ascii=False) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--start", type=int, default=0)
    args = ap.parse_args()
    print("=" * 60)
    print("东营领导专采 | limit:", args.limit, "dry:", args.dry_run)
    print("=" * 60)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    img_base = PUB / "山东省" / "东营市" / "市级" / "东营市人民政府"
    res = {"ok": 0, "fail": 0}
    batch = LEADERS[args.start:]
    if args.limit: batch = batch[:args.limit]

    for pos, name, dpath, ph_path in batch:
        url = BASE + dpath
        ph_url = BASE + ph_path
        print(f"\n> {pos} {name}")
        st, htm = fetch(url)
        bio, fg = "", ""
        if st == 200: bio, fg = extract_bio(htm)
        g = gender_from_bio(bio)
        print(f"  照: {ph_path.split('/')[-1][:30]} | 性: {g}")
        if bio: print(f"  简: {bio[:80]}...")
        if args.dry_run: res["ok"] += 1; continue

        out = img_base / ("男" if g=="男" else ("女" if g=="女" else "未知")) / (name + ".jpg")
        if not dl(ph_url, out):
            out2 = out.with_suffix('.png')
            if not dl(ph_url, out2):
                res["fail"] += 1; continue
            out = out2

        pid_ = pid(name); aid_ = generate_titi_asset_id()
        write_meta(out, pid_, aid_, name, g, pos, bio, fg, url, ph_url)
        exist = conn.execute("SELECT person_id FROM people WHERE name=? AND city=?", (name, CITY)).fetchone()
        raw = json.dumps({"biography": bio, "division_of_work": fg}, ensure_ascii=False)
        if exist:
            conn.execute("UPDATE people SET position=?,division_of_work=?,biography=?," +
                "source_url=?,source_page_title=?,has_official_photo=1," +
                "image_status='official_photo',primary_image_path=?,gender=? " +
                "WHERE person_id=?",
                (pos, fg, bio, url, f"市政府领导 - {name}", str(out), g, exist["person_id"]))
            pid_ = exist["person_id"]; print(f"  [UPD] {pid_}")
        else:
            conn.execute("INSERT INTO people(person_id,name,gender,province,city," +
                "county_or_district,administrative_level,unit_name,department,position," +
                "division_of_work,biography,full_public_text,source_url,source_page_title," +
                "source_site_domain,source_unit_url,has_official_photo,has_news_photo," +
                "image_status,primary_image_path,raw_fields_json,crawled_at,notes) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,'official_photo',?,?," +
                "datetime('now'),?)",
                (pid_, name, g, PROV, CITY, "", "city", "东营市人民政府", "", pos, fg,
                 bio, bio, url, f"市政府领导 - {name}",
                 "www.dongying.gov.cn", BASE + "/",
                 str(out), raw, "manual dongying leadership batch"))
            print(f"  [INS] {pid_}")
        conn.execute("DELETE FROM image_assets WHERE person_id=?", (pid_,))
        conn.execute("INSERT INTO image_assets(asset_id,person_id,name,image_type," +
            "image_index,image_total,image_path,image_url,source_url,source_page_title," +
            "source_site_domain,sha256,width,height,is_usable,crawled_at) " +
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,datetime('now'))",
            (aid_, pid_, name, "official_photo", 1, 1, str(out), ph_url, url,
             f"市政府领导 - {name}", "www.dongying.gov.cn", "", 0, 0))
        conn.commit(); res["ok"] += 1; time.sleep(2)

    if not args.dry_run:
        export_tables(conn)
        c = conn.cursor()
        print(f"\n完成! OK: {res['ok']} FAIL: {res['fail']}")
        print(f"东营有图: {c.execute('SELECT count(*) FROM people WHERE city=? AND has_official_photo=1', (CITY,)).fetchone()[0]}")
        print(f"总usable资产: {c.execute('SELECT count(*) FROM image_assets WHERE is_usable=1').fetchone()[0]}")
    conn.close()

if __name__ == "__main__":
    main()
