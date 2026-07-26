#!/usr/bin/env python3
"""菏泽市政府领导专采 (via API)"""
import argparse, hashlib, json, re, sqlite3, sys, time, csv
from datetime import datetime
from pathlib import Path
import requests

PROJ = Path("/sessions/determined-sweet-cannon/mnt/bugemini/d2ilite")
PUB = PROJ / "data" / "public_archive" / "山东公开官员"
DB = Path("/tmp/linyi_work/registry/people.sqlite")
sys.path.insert(0, str(PROJ))
from metadata_writer import build_titi_json, generate_titi_asset_id, write_xmp_metadata

PROV, CITY = "山东省", "菏泽市"
API = "http://www.heze.gov.cn"
HEADERS = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json", "Accept": "application/json"}

LEADERS = [
    ("市长","李春英","1648184009714438144","1660561864020594688"),
    ("副市长","臧伟","1742836062495051776","1742835988671107072"),
    ("副市长","杨新胜","1876222058199912448","1876221965564514304"),
    ("副市长","王新国","1959878746420940800","1959878812359593984"),
    ("副市长","王昌华","1565225295194230784","1660561558054506496"),
    ("副市长","张鹏","1565225950730391552","1660561700589539328"),
    ("副市长","肖友华","1565226113117065216","1660561730834665472"),
    ("副市长","刘连栋","1565226292213846016","1660561759154606080"),
    ("副市长","姜凌刚","1565226459868565504","1660561779488591872"),
    ("副市长","冯艳丽","1795763096590094336","1795762961592225794"),
    ("秘书长","刘芙蓉","1750674756459433984","1750674667288530944"),
]

def fetch(catas, file_flag=None):
    if file_flag:
        r = requests.post(API + "/els-service/article/1/1",
            json={"dq": "0530", "catas": [catas], "file": "1"}, headers=HEADERS, timeout=15)
    else:
        r = requests.post(API + "/els-service/search/1/1",
            json={"dq": "0530", "catas": [catas]}, headers=HEADERS, timeout=15)
    return r.json()

def ext_photo(jjzp):
    data = fetch(jjzp, file_flag=True)
    contents = data.get("data", {}).get("contents", [])
    if contents:
        files = contents[0].get("files", [])
        if files:
            url = files[0].get("fileurl", "")
            if url:
                full = API + url if url.startswith("/") else url
                title = contents[0].get("subject", "")
                sub = contents[0].get("subtitle", "")
                return full, title, sub
    return "", "", ""

def ext_bio(jjzp):
    data = fetch(jjzp)
    contents = data.get("data", {}).get("contents", [])
    return contents[0].get("txtmemo", "") if contents else ""

def ext_fg(fgzp):
    data = fetch(fgzp)
    contents = data.get("data", {}).get("contents", [])
    return contents[0].get("txtmemo", "") if contents else ""

def gender(bio):
    if re.search(r"性别[：]?\s*男|[,，]\s*男[,，]", bio): return "男"
    if re.search(r"性别[：]?\s*女|[,，]\s*女[,，]", bio): return "女"
    return "未知"

def dl(url, path):
    if path.exists() and path.stat().st_size > 1000: return True
    path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30, stream=True)
    if r.status_code != 200 or "image" not in r.headers.get("Content-Type",""): return False
    with open(path, "wb") as f: f.write(r.content)
    print("    [OK]", len(r.content), "b ->", path.name); return True

def write_meta(path, pid, aid, name, g, pos, bio, fg, src_url, img_url):
    md = {
        "title": f"{pos} {name}",
        "description": bio,
        "keywords": [name, pos, CITY, "菏泽市人民政府"],
        "source": src_url,
        "city": CITY,
        "person": name,
        "titi_asset_id": aid,
        "d2i_profile": {
            "person_id": pid, "asset_id": aid, "name": name, "gender": g,
            "province": PROV, "city": CITY,
            "unit": "菏泽市人民政府", "unit_name": "菏泽市人民政府", "position": pos,
            "source_url": src_url,
            "source_page_title": "菏泽市政府领导信息",
            "source_site_domain": "www.heze.gov.cn",
            "image_url": img_url, "image_type": "official_photo",
            "full_content": bio,
            "division_of_work": fg,
            "crawled_at": datetime.now().isoformat(),
        },
    }
    titi = build_titi_json(md)
    write_xmp_metadata(str(path), titi)

def pid(name): return "p_" + hashlib.sha256(("heze-"+name).encode()).hexdigest()[:16]

def export_tables(conn):
    root = Path("/tmp/linyi_work")
    for table in ["people", "image_assets", "source_units"]:
        try:
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            cols = [d[1] for d in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            with open(root / "registry" / f"{table}.csv", "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f); w.writerow(cols)
                for r in rows: w.writerow([dict(r).get(c, "") for c in cols])
            with open(root / "registry" / f"{table}.jsonl", "w", encoding="utf-8") as f:
                for r in rows: f.write(json.dumps(dict(r), ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"  Export {table} error: {e}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--start", type=int, default=0)
    args = ap.parse_args()
    print("=" * 60)
    print("菏泽领导专采 | limit:", args.limit, "dry:", args.dry_run)
    print("=" * 60)
    
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    img_base = PUB / "山东省" / "菏泽市" / "市级" / "菏泽市人民政府"
    res = {"ok": 0, "fail": 0}
    batch = LEADERS[args.start:]
    if args.limit: batch = batch[:args.limit]
    
    for pos, name, jjzp, fgzp in batch:
        print(f"\n> {pos} {name}")
        photo_url, title, sub = ext_photo(jjzp)
        bio = ext_bio(jjzp)
        fg = ext_fg(fgzp)
        g = gender(bio)
        print(f"  照: {'YES' if photo_url else 'NO'} | 性: {g}")
        if bio: print(f"  简: {bio[:80]}...")
        
        if not photo_url: print("  [SKIP] no photo"); res["fail"]+=1; continue
        if args.dry_run: res["ok"]+=1; continue
        
        out = img_base / ("男" if g=="男" else ("女" if g=="女" else "未知")) / (name + ".jpg")
        if not dl(photo_url, out): res["fail"]+=1; continue
        
        pid_ = pid(name); aid_ = generate_titi_asset_id()
        write_meta(out, pid_, aid_, name, g, pos, bio, fg, API+"/zwgk/", photo_url)
        
        exist = conn.execute("SELECT person_id FROM people WHERE name=? AND city=?", (name, CITY)).fetchone()
        raw = json.dumps({"biography": bio, "division_of_work": fg}, ensure_ascii=False)
        if exist:
            conn.execute("UPDATE people SET position=?,division_of_work=?,biography=?," +
                "source_url=?,source_page_title=?,has_official_photo=1," +
                "image_status='official_photo',primary_image_path=?,gender=? " +
                "WHERE person_id=?",
                (pos, fg, bio, API+"/zwgk/", "菏泽市政府领导信息", str(out), g, exist["person_id"]))
            pid_ = exist["person_id"]; print("  [UPD]", pid_)
        else:
            conn.execute("INSERT INTO people(person_id,name,gender,province,city," +
                "county_or_district,administrative_level,unit_name,department,position," +
                "division_of_work,biography,full_public_text,source_url,source_page_title," +
                "source_site_domain,source_unit_url,has_official_photo,has_news_photo," +
                "image_status,primary_image_path,raw_fields_json,crawled_at,notes) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,'official_photo',?,?," +
                "datetime('now'),?)",
                (pid_, name, g, PROV, CITY, "", "city", "菏泽市人民政府", "", pos, fg,
                 bio, bio, API+"/zwgk/", "菏泽市政府领导信息",
                 "www.heze.gov.cn", API+"/zwgk/",
                 str(out), raw, "manual heze leadership batch"))
            print("  [INS]", pid_)
        
        conn.execute("DELETE FROM image_assets WHERE person_id=?", (pid_,))
        conn.execute("INSERT INTO image_assets(asset_id,person_id,name,image_type," +
            "image_index,image_total,image_path,image_url,source_url,source_page_title," +
            "source_site_domain,sha256,width,height,is_usable,crawled_at) " +
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,datetime('now'))",
            (aid_, pid_, name, "official_photo", 1, 1, str(out), photo_url, API+"/zwgk/",
             "菏泽市政府领导信息", "www.heze.gov.cn", "", 0, 0))
        conn.commit(); res["ok"]+=1; time.sleep(2)
    
    if not args.dry_run:
        export_tables(conn)
        c = conn.cursor()
        print(f"\n完成! OK: {res['ok']} FAIL: {res['fail']}")
        try:
            print(f"菏泽有图: {c.execute('SELECT count(*) FROM people WHERE city=? AND has_official_photo=1', (CITY,)).fetchone()[0]}")
        except:
            pass
    conn.close()

if __name__ == "__main__":
    main()
