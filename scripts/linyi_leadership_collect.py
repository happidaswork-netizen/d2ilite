#!/usr/bin/env python3
"""临沂市政府领导专采"""
import argparse, hashlib, json, re, sqlite3, sys, time, csv
from datetime import datetime
from pathlib import Path
import requests
from lxml import html as lxml_html

PROJ = Path("/sessions/determined-sweet-cannon/mnt/bugemini/d2ilite")
PUB = PROJ / "data" / "public_archive" / "山东公开官员"
DB = Path("/tmp/linyi_work/registry/people.sqlite")

sys.path.insert(0, str(PROJ))
from metadata_writer import build_titi_json, generate_titi_asset_id, write_xmp_metadata

BASE = "https://www.linyi.gov.cn"
PROV, CITY = "山东省", "临沂市"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

LEADERS = [
    ("市长","于胜涛","/zf/szfld/sz4/yst.htm"),("副市长","隽新阳","/zf/szfld/fsz/jxy.htm"),
    ("副市长","苟宏水","/zf/szfld/fsz/ghs.htm"),("副市长","王映海","/zf/szfld/fsz/wyh.htm"),
    ("副市长","张秀丽","/zf/szfld/fsz/zxl.htm"),("副市长","靖涛","/zf/szfld/fsz/jt1.htm"),
    ("副市长","孙德士","/zf/szfld/fsz/sds.htm"),("副市长","马焕军","/zf/szfld/fsz/mhj.htm"),
    ("副市长","李无旡","/zf/szfld/fsz/lwj1.htm"),
]

def fetch(url):
    r = requests.get(url, headers=HDR, timeout=30); r.encoding = "utf-8"
    return r.status_code, r.text

def norm(src, base):
    if not src or src.startswith(("data:", "javascript:")): return ""
    from urllib.parse import urljoin; return urljoin(base, src)

def extract(htm, url):
    doc = lxml_html.fromstring(htm)
    info = {"photo": "", "bio": "", "fg": ""}
    for img in doc.xpath("//img[@src]"):
        s = img.get("src","")
        if "virtual_attach_file" in s: info["photo"] = norm(s, url); break
    bio = "".join(doc.xpath('//div[contains(@class,"ldjj")]//text()')).strip() or ""
    if bio: info["bio"] = bio
    for h1 in doc.xpath('//h1[contains(text(),"分工")]'):
        p = h1.getparent()
        sib = p.getnext() if p is not None else None
        if sib is not None:
            t = "".join(sib.xpath(".//text()")).strip()
            if t: info["fg"] = t; break
    return info

def gender(bio):
    if re.search(r"性别[：]?\s*男|[,，]\s*男[,，]", bio): return "男"
    if re.search(r"性别[：]?\s*女|[,，]\s*女[,，]", bio): return "女"
    return "未知"

def dl(url, path):
    if path.exists() and path.stat().st_size > 1000: return True
    path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, headers={**HDR, "Referer": BASE+"/"}, timeout=30, stream=True)
    if r.status_code != 200 or "image" not in r.headers.get("Content-Type",""): return False
    with open(path, "wb") as f: f.write(r.content)
    print("    [OK]", len(r.content), "b ->", path.name); return True

def write_meta(path, pid, aid, name, g, pos, bio, fg, src_url, page_title, img_url):
    md = {
        "title": f"{pos} {name}",
        "description": bio,
        "keywords": [name, pos, CITY, "临沂市人民政府"],
        "source": src_url,
        "city": CITY,
        "person": name,
        "titi_asset_id": aid,
        "d2i_profile": {
            "person_id": pid, "asset_id": aid, "name": name, "gender": g,
            "province": PROV, "city": CITY,
            "unit": "临沂市人民政府", "unit_name": "临沂市人民政府", "position": pos,
            "source_url": src_url, "source_page_title": page_title,
            "source_site_domain": "www.linyi.gov.cn",
            "image_url": img_url, "image_type": "official_photo",
            "full_content": bio,
            "division_of_work": fg,
            "crawled_at": datetime.now().isoformat(),
        },
    }
    titi = build_titi_json(md)
    write_xmp_metadata(str(path), titi)

def pid(name): return "p_" + hashlib.sha256(("linyi-"+name).encode()).hexdigest()[:16]

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
    print("临沂领导专采 | limit:", args.limit, "dry:", args.dry_run)
    print("=" * 60)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    img_base = PUB / "山东省" / "临沂市" / "市级" / "临沂市人民政府"
    res = {"ok": 0, "fail": 0}
    batch = LEADERS[args.start:]
    if args.limit: batch = batch[:args.limit]

    for pos, name, dpath in batch:
        url = BASE + dpath
        print("\n>", pos, name)
        st, htm = fetch(url)
        if st != 200: print("  [SKIP] HTTP", st); res["fail"]+=1; continue

        info = extract(htm, url)
        ph, bio, fg = info["photo"], info["bio"], info["fg"]
        g = gender(bio)
        print("  照:", "YES" if ph else "NO", "| 性:", g)
        if bio: print("  简:", bio[:80], "...")
        if not ph: print("  [SKIP] no photo"); res["fail"]+=1; continue
        if args.dry_run: res["ok"]+=1; continue

        out = img_base / ("男" if g=="男" else ("女" if g=="女" else "未知")) / (name + ".jpg")
        if not dl(ph, out): res["fail"]+=1; continue

        pid_ = pid(name); aid_ = generate_titi_asset_id()
        write_meta(out, pid_, aid_, name, g, pos, bio, fg, url, "市政府领导 - "+name, ph)

        exist = conn.execute("SELECT person_id FROM people WHERE name=? AND city=?", (name, CITY)).fetchone()
        raw = json.dumps({"biography": bio, "division_of_work": fg}, ensure_ascii=False)
        if exist:
            conn.execute("UPDATE people SET position=?,division_of_work=?,biography=?," +
                "source_url=?,source_page_title=?,has_official_photo=1," +
                "image_status='official_photo',primary_image_path=?,gender=? " +
                "WHERE person_id=?",
                (pos, fg, bio, url, "市政府领导 - "+name, str(out), g, exist["person_id"]))
            pid_ = exist["person_id"]; print("  [UPD]", pid_)
        else:
            conn.execute("INSERT INTO people(person_id,name,gender,province,city," +
                "county_or_district,administrative_level,unit_name,department,position," +
                "division_of_work,biography,full_public_text,source_url,source_page_title," +
                "source_site_domain,source_unit_url,has_official_photo,has_news_photo," +
                "image_status,primary_image_path,raw_fields_json,crawled_at,notes) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,'official_photo',?,?," +
                "datetime('now'),?)",
                (pid_, name, g, PROV, CITY, "", "city", "临沂市人民政府", "", pos, fg,
                 bio, bio, url, "市政府领导 - "+name,
                 "www.linyi.gov.cn", BASE+"/zf/",
                 str(out), raw, "manual linyi leadership batch"))
            print("  [INS]", pid_)

        conn.execute("DELETE FROM image_assets WHERE person_id=?", (pid_,))
        conn.execute("INSERT INTO image_assets(asset_id,person_id,name,image_type," +
            "image_index,image_total,image_path,image_url,source_url,source_page_title," +
            "source_site_domain,sha256,width,height,is_usable,crawled_at) " +
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,datetime('now'))",
            (aid_, pid_, name, "official_photo", 1, 1, str(out), ph, url,
             "市政府领导 - "+name, "www.linyi.gov.cn", "", 0, 0))
        conn.commit(); res["ok"]+=1; time.sleep(2.5)

    if not args.dry_run:
        export_tables(conn)
        c = conn.cursor()
        print("\n完成! OK:", res["ok"], "FAIL:", res["fail"])
        print("临沂有图:", c.execute("SELECT count(*) FROM people WHERE city=? AND has_official_photo=1", (CITY,)).fetchone()[0])
        print("总usable资产:", c.execute("SELECT count(*) FROM image_assets WHERE is_usable=1").fetchone()[0])
    conn.close()

if __name__ == "__main__":
    main()
