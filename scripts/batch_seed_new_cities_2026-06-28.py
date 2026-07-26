# -*- coding: utf-8 -*-
"""
批量补种：将已发现的淄博/青岛/济宁等城市的高价值入口写入 crawl_queue

用法（在 Windows 上）：
  cd /d D:\bugemini\d2ilite
  .venv\Scripts\python.exe scripts\batch_seed_new_cities_2026-06-28.py

之后跑主采集器：
  .venv\Scripts\python.exe scraper\run_official_profile_collector.py ^
    --region shandong --max-workers 2 --max-pages 200 ^
    --delay-min 2.0 --delay-max 4.0 --progress-interval 30 --export-interval 90
"""

import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scraper.run_official_profile_collector import Registry, QueueItem

ROOT = Path(r"D:\bugemini\d2ilite\data\public_archive\山东公开官员")
reg = Registry(ROOT, "山东省")

seeds = [
    # ===== 淄博（22人0图，SPA）=====
    # 市政府领导搜索页（SPA，需浏览器渲染）
    ("山东省", "淄博市", "", "city", "淄博市人民政府",
     "https://www.zibo.gov.cn/gongkai/channel_5fc4b785f867d82dbc565b7e/?key=1606898238116", 120),
    # 机构职能
    ("山东省", "淄博市", "", "city", "淄博市人民政府",
     "https://www.zibo.gov.cn/gongkai/channel_c1534/", 110),
    # 职能配置
    ("山东省", "淄博市", "", "city", "淄博市人民政府",
     "https://www.zibo.gov.cn/gongkai/channel_5ff26df1f44cee8ffa5a7f18/", 110),
    # 赵庆文（市长）个人页
    ("山东省", "淄博市", "", "city", "淄博市人民政府",
     "https://www.zibo.gov.cn/gongkai/channel_5fc4b785f867d82dbc565b7e/?key=1606898238116", 130),
    # 宗志坚（秘书长）个人页
    ("山东省", "淄博市", "", "city", "淄博市人民政府",
     "https://www.zibo.gov.cn/gongkai/channel_5fc4b785f867d82dbc565b7e/?key=1693302114873", 130),
    # 淄博部门子站（可能含领导信息）
    ("山东省", "淄博市", "", "agency", "淄博市发展和改革委员会",
     "https://fgw.zibo.gov.cn/", 100),
    ("山东省", "淄博市", "", "agency", "淄博市工业和信息化局",
     "https://gxj.zibo.gov.cn/", 100),
    ("山东省", "淄博市", "", "agency", "淄博市公安局",
     "https://gaj.zibo.gov.cn/", 100),
    ("山东省", "淄博市", "", "agency", "淄博市人力资源和社会保障局",
     "https://hrss.zibo.gov.cn/", 100),
    ("山东省", "淄博市", "", "agency", "淄博市自然资源和规划局",
     "https://gtj.zibo.gov.cn/", 100),

    # ===== 青岛（无数据）=====
    # 部门信息公开目录
    ("山东省", "青岛市", "", "city", "青岛市人民政府",
     "https://www.qingdao.gov.cn/zwgk/xxgk/", 120),
    # 市政府办公厅
    ("山东省", "青岛市", "", "department", "青岛市人民政府办公厅",
     "https://www.qingdao.gov.cn/zwgk/xxgk/bgt/gkzn/", 110),
    # 各局（青岛使用独立CMS，/zwgk/xxgk/{dept}/gkzn/）
    ("山东省", "青岛市", "", "agency", "青岛市公安局",
     "https://www.qingdao.gov.cn/zwgk/xxgk/gaj/gkzn/", 110),
    ("山东省", "青岛市", "", "agency", "青岛市教育局",
     "https://www.qingdao.gov.cn/zwgk/xxgk/jyj/gkzn/", 110),
    ("山东省", "青岛市", "", "agency", "青岛市财政局",
     "https://www.qingdao.gov.cn/zwgk/xxgk/czj/gkzn/", 110),
    ("山东省", "青岛市", "", "agency", "青岛市自然资源和规划局",
     "https://www.qingdao.gov.cn/zwgk/xxgk/zygh/gkzn/", 110),

    # ===== 济宁（无数据，col/art CMS）=====
    ("山东省", "济宁市", "", "city", "济宁市人民政府",
     "https://www.jining.gov.cn/", 120),
    ("山东省", "济宁市", "", "city", "济宁市要闻动态",
     "https://www.jining.gov.cn/col/col65754/index.html", 100),

    # ===== 泰安（无数据，col/art CMS）=====
    ("山东省", "泰安市", "", "city", "泰安市人民政府",
     "https://www.taian.gov.cn/", 120),

    # ===== 威海（无数据）=====
    ("山东省", "威海市", "", "city", "威海市人民政府",
     "https://www.weihai.gov.cn/", 120),

    # ===== 日照（无数据）=====
    ("山东省", "日照市", "", "city", "日照市人民政府",
     "https://www.rizhao.gov.cn/", 120),

    # ===== 聊城（无数据）=====
    ("山东省", "聊城市", "", "city", "聊城市人民政府",
     "https://www.liaocheng.gov.cn/", 120),

    # ===== 滨州（无数据）=====
    ("山东省", "滨州市", "", "city", "滨州市人民政府",
     "https://www.binzhou.gov.cn/", 120),

    # ===== 菏泽已有10人10图，但可能漏部门页面 =====
    ("山东省", "菏泽市", "", "city", "菏泽市人民政府",
     "https://www.heze.gov.cn/zwgk/", 110),

    # ===== 东营已有9人9图，补扫额外col =====
    ("山东省", "东营市", "", "city", "东营市人民政府",
     "https://www.dongying.gov.cn/col/col38804/index.html?number=ldzc5201", 110),
]

# 写入 registry
count = 0
for province, city, county, level, unit_name, url, priority in seeds:
    reg.add_source_unit(
        province=province, city=city, county=county,
        level=level, unit_name=unit_name,
        official_site_url=url, profile_list_url=url,
        status="candidate",
        notes="batch seed 2026-06-28",
    )
    reg.reprioritize_or_enqueue(QueueItem(
        url=url, province=province, city=city,
        county_or_district=county, administrative_level=level,
        unit_name=unit_name, source_unit_url=url,
        depth=0, parent_url="", priority=priority,
    ))
    count += 1

reg.export_tables()
reg.conn.close()
print(f"✅ 已补种 {count} 个种子到 crawl_queue")
PYEOF
