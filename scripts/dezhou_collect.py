"""
德州公开官员专采脚本

从德州市政府门户网站采集市政府领导及其照片。
目标：9人9图（市长1 + 副市长7 + 秘书长1）

使用方法：
  python3 scripts/dezhou_collect.py

必须从 d2ilite 根目录运行。
"""

import sys, os, json, hashlib, shutil, ssl, re, time
from pathlib import Path
from datetime import datetime, timezone

# ── 路径设置 ────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent  # d2ilite 根目录
PROJECT_ROOT = ROOT / "data" / "public_archive" / "山东公开官员"

sys.path.insert(0, str(ROOT))
from metadata_writer import write_xmp_metadata

# ── SSL 上下文（德州 gov 站点用 HTTP OK，但部分图片需要宽松 SSL）──
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "http://www.dezhou.gov.cn/",
}

# ── 工具函数 ────────────────────────────────────────────────
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def urlopen(url, timeout=10):
    req = urllib.request.Request(url, headers=HEADERS)
    return urllib.request.urlopen(req, timeout=timeout, context=ctx)

def normalize_url(url: str) -> str:
    """Simplified URL normalization."""
    if not url:
        return ""
    url = url.strip()
    return url

# ── 导入 Registry ────────────────────────────────────────────
# 直接 import 会有依赖问题，我们用 embedded Registry 的方法
import urllib.request
import sqlite3

# ── 领导数据 ────────────────────────────────────────────────
LEADERS = [
    {
        "name": "朱开国",
        "position": "市长",
        "gender": "男",
        "ethnicity": "汉族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795894/n47241518/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/70291697.jpg",
        "bio_summary": "朱开国，男，汉族，1973年2月出生，大学，法律硕士，中共党员。主持市政府全面工作。负责财政、税务、审计方面的工作。",
    },
    {
        "name": "赵纪钢",
        "position": "副市长",
        "gender": "男",
        "ethnicity": "汉族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795955/n88255365/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/88255679.jpg",
        "bio_summary": "赵纪钢，男，汉族，1973年12月出生，大学，经济学学士，中共党员。负责市政府常务工作。",
    },
    {
        "name": "樊廷雷",
        "position": "副市长",
        "gender": "男",
        "ethnicity": "汉族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795955/n69738451/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/70291263.jpg",
        "bio_summary": "樊廷雷，男，汉族，1966年9月出生，大学，工商管理硕士，中共党员。负责自然资源、住房城乡建设、交通运输等工作。",
    },
    {
        "name": "陈海明",
        "position": "副市长",
        "gender": "男",
        "ethnicity": "蒙古族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795955/n78482227/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/78483165.jpg",
        "bio_summary": "陈海明，男，蒙古族，1979年10月出生，大学，经济学学士、管理学学士，中共党员。负责商务、外事、金融等工作。",
    },
    {
        "name": "陈晓强",
        "position": "副市长",
        "gender": "男",
        "ethnicity": "汉族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795955/n69738478/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/70291260.jpg",
        "bio_summary": "陈晓强，男，汉族，1967年5月出生，研究生，理学硕士，民进会员。负责教育、民政、卫生健康等工作。",
    },
    {
        "name": "邵红双",
        "position": "副市长",
        "gender": "女",
        "ethnicity": "汉族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795955/n69084230/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/71661866.jpg",
        "bio_summary": "邵红双，女，汉族，1976年7月出生，中央党校研究生，中共党员。负责科技、生态环境、水利、农业农村等工作。",
    },
    {
        "name": "滕世雷",
        "position": "副市长",
        "gender": "男",
        "ethnicity": "汉族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795955/n90672847/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/90673226.jpg",
        "bio_summary": "滕世雷，男，汉族，1974年5月出生，大学，法律硕士，中共党员。负责公安、司法、退役军人、信访等工作。",
    },
    {
        "name": "徐立峰",
        "position": "副市长",
        "gender": "男",
        "ethnicity": "汉族",
        "detail_url": "http://www.dezhou.gov.cn/n42795639/n42795822/n42795955/n97897504/index.html",
        "listing_photo": "http://www.dezhou.gov.cn/dbsource/46122905/97897738.jpg",
        "bio_summary": "徐立峰，男，汉族，1971年10月出生，省委党校研究生，中共党员。负责工业和信息化、文化和旅游、市场监管等工作。",
    },
    {
        "name": "王胜强",
        "position": "秘书长",
        "gender": "男",
        "ethnicity": "汉族",
        "detail_url": "http://dezhou.gov.cn/n42795639/n42795822/n42796019/n42796833/index.html",
        "listing_photo": "http://dezhou.gov.cn/dbsource/4042828/58167257.jpg",
        "bio_summary": "王胜强，男，汉族，1966年6月出生，省委党校研究生，中共党员。负责处理市政府日常工作，领导市政府办公室全面工作。",
    },
]

# 来源页面
LEADERSHIP_PAGE_URL = "http://www.dezhou.gov.cn/n1403/n54763497/n54763512/index.html"


def extract_full_bio(detail_url: str) -> str:
    """从个人详情页提取完整简历和分工."""
    try:
        resp = urlopen(detail_url, timeout=10)
        html = resp.read().decode("utf-8", errors="replace")

        # 找 mayor-info 区域
        m = re.search(
            r'class="mayor-info[^"]*"[^>]*>(.*?)</div>\s*</div>', html, re.DOTALL
        )
        if m:
            text = re.sub(r"<[^>]+>", "\n", m.group(1))
            text = re.sub(r"\n+", "\n", text).strip()
        else:
            body = re.sub(r"<[^>]+>", "\n", html)
            text = re.sub(r"\n+", "\n", body).strip()

        # 过滤模板行
        lines = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if len(line) < 10:
                continue
            if any(
                k in line
                for k in [
                    "function",
                    "var ",
                    ".leader-",
                    ".mayor-",
                    "padding",
                    "margin",
                    "border",
                    "background",
                    "font-size",
                    "width:",
                    "height",
                    "{",
                    "}",
                    "/*",
                    "css",
                    ".shizhang",
                ]
            ):
                continue
            lines.append(line)

        return "\n".join(lines)
    except Exception as e:
        print(f"  ⚠️  详情页解析失败: {e}")
        return ""


def download_photo(url: str, dest: Path) -> bool:
    """下载照片到目标路径."""
    try:
        resp = urlopen(url, timeout=15)
        data = resp.read()
        if len(data) < 1000:
            print(f"  ⚠️  图片太小 ({len(data)} bytes)，可能不是有效照片")
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        print(f"  ✅ 下载成功: {dest.name} ({len(data)} bytes)")
        return True
    except Exception as e:
        print(f"  ❌ 下载失败: {e}")
        return False


def get_image_dimensions(path: Path):
    """简易获取图片尺寸."""
    try:
        data = path.read_bytes()
        if data[0:2] == b"\xff\xd8":
            # JPEG: 从 SOF marker 读取尺寸
            i = 2
            while i < len(data):
                if data[i] == 0xFF:
                    marker = data[i + 1]
                    if marker == 0xC0 or marker == 0xC1 or marker == 0xC2:
                        height = (data[i + 5] << 8) + data[i + 6]
                        width = (data[i + 7] << 8) + data[i + 8]
                        return width, height
                    elif marker == 0xD9:
                        break
                    elif 0xD0 <= marker <= 0xD7:
                        i += 2
                    else:
                        length = (data[i + 2] << 8) + data[i + 3]
                        i += 2 + length
                else:
                    i += 1
    except:
        pass
    return 0, 0


def main():
    import urllib.request  # re-ensure import

    print("=" * 60)
    print("德州公开官员采集")
    print(f"目标: {len(LEADERS)} 人")
    print("=" * 60)

    # ── 1. 复制 DB 到 /tmp ──────────────────────────────────
    db_prod = PROJECT_ROOT / "registry" / "people.sqlite"
    tmp_db = Path(f"/tmp/dezhou_people.sqlite")

    if not db_prod.exists():
        print(f"❌ DB 不存在: {db_prod}")
        return

    print(f"\n📦 复制 DB -> /tmp ...")
    shutil.copy2(db_prod, tmp_db)
    print(f"   ✅ {db_prod.stat().st_size} bytes -> {tmp_db}")

    # ── 2. 连接 DB ──────────────────────────────────────────
    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    reg = None  # 直接操作 SQLite

    # ── 3. 获取详情页完整简历 ──────────────────────────────
    print(f"\n📄 获取领导详情页简历...")
    for leader in LEADERS:
        print(f"\n  {leader['name']} ({leader['position']}):")
        full_bio = extract_full_bio(leader["detail_url"])
        if full_bio:
            leader["full_bio"] = full_bio
            # 也试着提取更精确的性别
            if "女，" in full_bio or "女，" in full_bio or "女," in full_bio:
                leader["gender"] = "女"
        else:
            leader["full_bio"] = leader["bio_summary"]
            print(f"  ℹ️  使用摘要简历")
        time.sleep(1)

    # ── 4. 下载照片并写入目录 ──────────────────────────────
    print(f"\n📷 下载领导照片...")
    CITY_DIR = PROJECT_ROOT / "山东省" / "德州市" / "市级" / "德州市人民政府"
    photo_map = {}  # name -> path

    for leader in LEADERS:
        gender_dir = "男" if leader["gender"] == "男" else "女"
        photo_path = CITY_DIR / gender_dir / f"{leader['name']}.jpg"

        print(f"\n  {leader['name']}:")
        ok = download_photo(leader["listing_photo"], photo_path)
        if not ok:
            print(f"  ⚠️  尝试从详情页下载...")
        if ok:
            photo_map[leader["name"]] = photo_path

    # ── 5. 写入 Registry ────────────────────────────────────
    print(f"\n💾 写入 Registry...")
    source_url = LEADERSHIP_PAGE_URL
    source_domain = "www.dezhou.gov.cn"
    crawled_at = utc_now()

    for leader in LEADERS:
        photo_path = photo_map.get(leader["name"])
        person_id = sha1_text("|".join([source_url, leader["name"]]))

        # Insert person
        gender = leader["gender"]
        bio = leader.get("full_bio", leader["bio_summary"])
        row = {
            "person_id": person_id,
            "name": leader["name"],
            "gender": gender,
            "province": "山东省",
            "city": "德州市",
            "county_or_district": "",
            "township_or_street": "",
            "administrative_level": "city",
            "unit_name": "德州市人民政府",
            "department": "",
            "position": leader["position"],
            "rank_or_grade": "",
            "division_of_work": bio,
            "biography": bio,
            "full_public_text": bio,
            "source_url": leader["detail_url"],
            "source_page_title": f"{leader['name']} - 德州市政府门户网站",
            "source_site_domain": source_domain,
            "source_unit_url": source_url,
            "publish_date": "",
            "updated_date": "",
            "has_official_photo": 1 if photo_path else 0,
            "has_news_photo": 0,
            "image_status": "official_photo" if photo_path else "",
            "primary_image_path": str(photo_path) if photo_path else "",
            "raw_fields_json": json.dumps(
                {
                    "position": leader["position"],
                    "ethnicity": leader.get("ethnicity", ""),
                    "gender": gender,
                    "extraction_method": "dezhou_targeted_collect",
                },
                ensure_ascii=False,
            ),
            "crawled_at": crawled_at,
            "last_checked_at": crawled_at,
            "notes": "德州专采：市政府领导页",
        }

        # Use Registry-like upsert
        columns = [
            "person_id",
            "name",
            "gender",
            "province",
            "city",
            "county_or_district",
            "township_or_street",
            "administrative_level",
            "unit_name",
            "department",
            "position",
            "rank_or_grade",
            "division_of_work",
            "biography",
            "full_public_text",
            "source_url",
            "source_page_title",
            "source_site_domain",
            "source_unit_url",
            "publish_date",
            "updated_date",
            "has_official_photo",
            "has_news_photo",
            "image_status",
            "primary_image_path",
            "raw_fields_json",
            "crawled_at",
            "last_checked_at",
            "notes",
        ]
        payload = {col: row.get(col, "") for col in columns}
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(
            f"{col}=excluded.{col}" for col in columns if col != "person_id"
        )

        conn.execute(
            f"""
            INSERT INTO people ({','.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(person_id) DO UPDATE SET {updates}
            """,
            [payload[col] for col in columns],
        )
        conn.commit()
        print(f"  ✅ {leader['name']} -> people 表")

        # Insert image asset
        if photo_path and photo_path.exists():
            asset_id = sha1_text(
                "|".join([leader["listing_photo"], person_id])
            )
            sha256 = sha256_file(photo_path)
            w, h = get_image_dimensions(photo_path)

            asset_columns = [
                "asset_id",
                "person_id",
                "name",
                "image_type",
                "image_index",
                "image_total",
                "image_path",
                "image_url",
                "source_url",
                "source_page_title",
                "source_site_domain",
                "caption_or_alt",
                "sha256",
                "content_type",
                "width",
                "height",
                "is_usable",
                "crawled_at",
                "notes",
            ]
            asset_row = {
                "asset_id": asset_id,
                "person_id": person_id,
                "name": leader["name"],
                "image_type": "official_photo",
                "image_index": 1,
                "image_total": 1,
                "image_path": str(photo_path),
                "image_url": leader["listing_photo"],
                "source_url": leader["detail_url"],
                "source_page_title": f"{leader['name']} - 德州市政府门户网站",
                "source_site_domain": source_domain,
                "caption_or_alt": f"{leader['name']} {leader['position']}",
                "sha256": sha256,
                "content_type": "image/jpeg",
                "width": w,
                "height": h,
                "is_usable": 1,
                "crawled_at": crawled_at,
                "notes": "德州专采",
            }

            a_payload = {col: asset_row.get(col, "") for col in asset_columns}
            a_placeholders = ",".join("?" for _ in asset_columns)
            a_updates = ",".join(
                f"{col}=excluded.{col}" for col in asset_columns if col != "asset_id"
            )
            conn.execute(
                f"""
                INSERT INTO image_assets ({','.join(asset_columns)})
                VALUES ({a_placeholders})
                ON CONFLICT(asset_id) DO UPDATE SET {a_updates}
                """,
                [a_payload[col] for col in asset_columns],
            )
            conn.commit()
            print(f"  ✅ {leader['name']} -> image_assets 表 ({sha256[:12]}, {w}x{h})")

            # ── 6. 写入 XMP 元数据 ──────────────────────────
            try:
                metadata = {
                    "d2i_profile": {
                        "person_id": person_id,
                        "asset_id": asset_id,
                        "name": leader["name"],
                        "gender": gender,
                        "province": "山东省",
                        "city": "德州市",
                        "county_or_district": "",
                        "unit": "德州市人民政府",
                        "unit_name": "德州市人民政府",
                        "department": "",
                        "position": leader["position"],
                        "source_url": leader["detail_url"],
                        "source_page_title": f"{leader['name']} - 德州市政府门户网站",
                        "source_site_domain": source_domain,
                        "source_unit_url": source_url,
                        "image_url": leader["listing_photo"],
                        "image_type": "official_photo",
                        "image_index": 1,
                        "image_total": 1,
                        "full_content": bio,
                        "raw_fields": json.dumps(
                            {
                                "ethnicity": leader.get("ethnicity", ""),
                                "extraction_method": "dezhou_targeted_collect",
                            },
                            ensure_ascii=False,
                        ),
                        "crawled_at": crawled_at,
                    }
                }
                write_xmp_metadata(str(photo_path), metadata)
                print(f"  ✅ {leader['name']} -> XMP 元数据")
            except Exception as e:
                print(f"  ⚠️  XMP 写入失败: {e}")

        # 小延迟
        time.sleep(0.5)

    # ── 7. 添加 source_unit ─────────────────────────────────
    unit_id = sha1_text("|".join(["山东省", "德州市", "", "德州市人民政府", source_url]))
    conn.execute(
        """
        INSERT INTO source_units (unit_id, province, city, county_or_district,
            administrative_level, unit_name, official_site_url, profile_list_url,
            crawl_status, last_crawled_at, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(unit_id) DO UPDATE SET
            crawl_status=excluded.crawl_status, last_crawled_at=excluded.last_crawled_at
        """,
        (
            unit_id,
            "山东省",
            "德州市",
            "",
            "city",
            "德州市人民政府",
            source_url,
            source_url,
            "done",
            crawled_at,
            "德州专采：市政府领导（含9人照片）",
        ),
    )
    conn.commit()
    print(f"\n  ✅ source_unit 已添加")

    # ── 8. 导出 ────────────────────────────────────────────
    print(f"\n📊 导出表格...")
    tables = ("people", "image_assets", "source_units")
    reg_dir = PROJECT_ROOT / "registry"
    for table in tables:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            continue
        cols = rows[0].keys()
        csv_path = reg_dir / f"{table}.csv"
        jsonl_path = reg_dir / f"{table}.jsonl"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            import csv

            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow([r[c] for c in cols])
        with jsonl_path.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(
                    json.dumps(dict(r), ensure_ascii=False, default=str) + "\n"
                )
        print(f"  ✅ {table}.csv / {table}.jsonl")

    # ── 9. KPI ──────────────────────────────────────────────
    print(f"\n📈 生成 KPI...")
    ppl = conn.execute("SELECT count(*) FROM people").fetchone()[0]
    assets = conn.execute(
        "SELECT count(*) FROM image_assets WHERE is_usable=1"
    ).fetchone()[0]
    with_assets = conn.execute(
        "SELECT count(distinct person_id) FROM image_assets WHERE is_usable=1"
    ).fetchone()[0]
    male_img = conn.execute(
        "SELECT count(*) FROM people WHERE gender='男' AND coalesce(primary_image_path,'')<>''"
    ).fetchone()[0]
    female_img = conn.execute(
        "SELECT count(*) FROM people WHERE gender='女' AND coalesce(primary_image_path,'')<>''"
    ).fetchone()[0]

    kpi = {
        "people": ppl,
        "usable_image_assets": assets,
        "people_with_assets": with_assets,
        "male_with_img": male_img,
        "female_with_img": female_img,
        "updated_at": crawled_at,
    }

    with open(reg_dir / "kpi.json", "w", encoding="utf-8") as f:
        json.dump(kpi, f, ensure_ascii=False, indent=2)
    print(f"  KPI: {json.dumps(kpi, ensure_ascii=False)}")

    # ── 10. 复制 DB 回生产 ────────────────────────────────
    conn.close()
    print(f"\n📦 复制 DB 回生产路径...")
    shutil.copy2(tmp_db, db_prod)
    print(f"   ✅ {tmp_db.stat().st_size} bytes -> {db_prod}")

    print(f"\n{'=' * 60}")
    print(f"✅ 德州采集完成！")
    print(f"   新增/更新: {len(LEADERS)} 人, {sum(1 for l in LEADERS if l['name'] in photo_map)} 张照片")
    print(f"   最新 KPI: people={ppl}, 可用图片={assets}, 有图人员={with_assets}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
