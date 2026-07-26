> **口径已更替(2026-07-26)**:本文的终落点(`data/public_archive/山东公开官员`)与队列口径已被 D2I Cloud 取代——终根只允许 `角色肖像`,禁写 `山东公开官员`;以 [`D2I_Cloud产品契约_2026-07-25.md`](D2I_Cloud产品契约_2026-07-25.md) 与 [`d2i_cloud_template_extract_contract.md`](d2i_cloud_template_extract_contract.md) 为准。本文仅保留站点经验与历史进度供参考。

# 山东政府网站 bmml/VSB 采集方法与参考

> 创建时间：2026-06-28  
> 作者：d2ilite 采集项目  
> 适用范围：山东临沂市及同架构地市政府网站

---

## 1. 核心发现：bmml 与 VSB 系统

### 1.1 bmml 是什么

**bmml** = **部门目录**（bù mén mù lù）的拼音缩写。

这是临沂市政府网站（www.linyi.gov.cn）使用的URL路径模式，用于组织全市40多个部门的机构职能页面。格式：

```
/bmml/{部门代码}/{部门代码}_4778/{部门代码}_8902.htm
```

例如：

| 部门 | 代码 | bmml URL |
|------|------|----------|
| 市政府办公室 | zfb | `/bmml/zfb/zfb_4778/zfb_8902.htm` |
| 发改局 | fgw | `/bmml/fgw/fgw_4778/fgw_8902.htm` |
| 教育局 | jyj | `/bmml/jyj/jyj_4778/jyj_8902.htm` |
| 公安局 | gaj | `/bmml/gaj/gaj_4778/gaj_8902.htm` |
| 司法局 | sfj | `/bmml/sfj/sfj_4778/sfj_8902.htm` |

### 1.2 VSB 图片系统

**VSB** 是山东省政府网站使用的内容管理系统之一（推测与山东共用的 CMS 平台有关）。其图片URL模式：

```
/virtual_attach_file.vsb?afc=...&oid=...&tid=6166&nid=423539&e=.jpg
```

关键特征：
- URL 以 `/virtual_attach_file.vsb` 开头
- 参数包含 `afc`、`oid`、`tid`、`nid`、`e=.jpg`
- 需要 `Referer` 头才能正常返回图片（依赖域名）
- 图片以二进制流返回，无额外防盗链签名

### 1.3 部门列表入口

所有部门的 bmml 页面入口在：

```
https://www.linyi.gov.cn/bmgk/jgzn.htm
```

**bmgk** = **办公公开**（bàn gōng gōng kāi）的拼音缩写。  
该页面列出了全部约40个部门及其职能简介，每个部门有一个"查看"链接指向对应的 bmml 页面。

---

## 2. 采集方法

### 2.1 Python requests + VSB 图片提取

```python
import requests, re

# 必须加 Referer
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.linyi.gov.cn/',
}

# 获取 bmml 页面
r = requests.get(bmml_url, headers=headers, timeout=15)
r.encoding = 'utf-8'

# 提取 VSB 图片
vsb_imgs = re.findall(
    r'<img[^>]+src="(/virtual_attach_file\.vsb[^"]+)"[^>]*>',
    r.text
)

# 下载 VSB 图片
for vsb_url in vsb_imgs:
    img_url = 'https://www.linyi.gov.cn' + vsb_url
    resp = requests.get(img_url, headers=headers, timeout=15)
    # resp.content 即为图片二进制数据
```

### 2.2 人物简介提取

bmml 页面上，人物照片通常紧挨着文字简介。提取策略：

```python
# 对每张 VSB 图片，在页面源码中获取图片附近的文字
for vsb_url in vsb_imgs:
    pos = r.text.find(vsb_url)
    ctx = r.text[pos:pos+1200]  # 取图片后约1200字符
    
    # 去除HTML标签后分行
    ct = re.sub(r'<[^>]+>', '\n', ctx)
    lines = [l.strip() for l in ct.split('\n') if l.strip()]
    
    # 找包含"男"或"女"且有逗号的短行作为简介
    bio = ''
    for l in lines:
        if '，' in l and ('男' in l or '女' in l) and len(l) < 80:
            bio = l
            break
    
    # 从简介中提取姓名（简介的开头部分）
    name = bio.split('，')[0] if bio else ''
```

### 2.3 多行简介处理

有些页面的简介跨多行：

```
张成，
男，汉族，1973年3月生，省委党校研究生学历，中共党员，
现任市政府副秘书长、机关党组成员。
```

处理策略：

```python
# 用索引遍历行
for i, l in enumerate(lines):
    # 姓名行模式：2-4个汉字 + 逗号
    if re.match(r'^[一-鿿]{2,4}[，,]', l):
        if i+1 < len(lines) and re.match(r'^(男|女)', lines[i+1]):
            # 合并姓名行 + 后续行的性别/出生等信息
            parts = [l.rstrip('，, ')]
            for j in range(i+1, min(i+3, len(lines))):
                parts.append(lines[j])
            bio = '，'.join(parts)
            break
```

### 2.4 重要注意事项

```python
# ❌ 错误：会压扁所有换行
text = re.sub(r'\s+', ' ', raw_text)
# ✅ 正确：只压缩空格，保留换行
text = re.sub(r'[ \t]+', ' ', raw_text)
```

---

## 3. 已知部门代码表（临沂市）

| 代码 | 部门名称 | bmml 采集数 |
|------|----------|------------|
| zfb | 市政府办公室 | 9 |
| fgw | 发改委 | 5 |
| jyj | 教育局 | 5 |
| kxjsj | 科技局 | 5 |
| xxhwyh | 工信局 | 7 |
| gaj | 公安局 | 10 |
| mzj | 民政局 | 6 |
| sfj | 司法局 | 14 |
| czj | 财政局 | 5 |
| sbj | 人社局 | **0** (无VSB图片) |
| gtzy | 自然资源局 | 6 |
| hbj | 环保局 | 5 |
| zfw | 住建局 | 6 |
| cgj | 城管局 | 5 |
| jtysj | 交通局 | 8 |
| slj | 水利局 | 7 |
| nwh | 农业农村局 | 6 |
| swj | 商务局 | 7 |
| gdj | 文旅局 | 5 |
| wsj | 卫健委 | 4/6 |
| tyjrswj | 退役军人局 | **0** (无VSB图片) |
| ajj | 应急局 | 6 |
| sjj | 审计局 | 4 |
| wsqw | 外事办 | 4 |
| gzw | 国资委 | 9 |
| xzspj | 审批局 | 6 |
| scjdj | 市场监管局 | 5/7 |
| tyj | 体育局 | - |
| tjj | 统计局 | 7 |
| ybj | 医保局 | 7 |
| fkb | 国防办 | 6 |
| dsjj | 大数据局 | 4 |
| lsj | 粮食局 | 5 |
| lyj | 林业局 | 4 |
| ymssjdzgy | 地质公园局 | 4 |
| zfgjj | 公积金中心 | 4 |
| dzj | 地震台 | 8 |
| gxhzs | 供销社 | 6 |
| glj | 公路中心 | 6 |
| dftlj | 铁路民航中心 | 3 |
| jyjczx | 检验检测中心 | 6 |

**已知无图部门**：sbj（人社局）、tyjrswj（退役军人局）、部分特殊单位

---

## 4. 城市通用性分析

### 4.1 确认使用 VSB 系统的城市

| 城市 | 域名 | 是否使用 VSB | 备注 |
|------|------|-------------|------|
| **临沂** | linyi.gov.cn | ✅ 完整 | 40+部门 bmml，照片齐全 |
| 东营 | dongying.gov.cn | ✅ 部分 | col38804 领导页，col38800 无图 |
| 菏泽 | heze.gov.cn | ✅ 部分 | 领导页有VSB |
| 济南 | jinan.gov.cn | ❌ | 独立CMS文件路径 |
| 淄博 | zibo.gov.cn | ❌ | Vue.js SPA |
| 烟台 | yantai.gov.cn | ❌ | 独立CMS |
| 潍坊 | weifang.gov.cn | ❌ | 独立CMS |
| 枣庄 | zaozhuang.gov.cn | ❌ | 独立CMS |

### 4.2 VSB 发现方法

要检查一个城市是否使用 VSB 系统：

```bash
# 搜索 VSB 关键字
curl -sL 'https://{城市域名}/' | grep -c 'vsb\|virtual_attach_file'

# 搜索 bmml 路径
curl -sL 'https://{城市域名}/' | grep -c '/bmml/'
```

### 4.3 bmml 部门列表页位置

```
# 临沂
https://www.linyi.gov.cn/bmgk/jgzn.htm

# 其他城市如果使用同架构（需验证）
https://{城市}.gov.cn/bmgk/jgzn.htm
```

部分城市可能有类似路径：`/bmgl/`、`/jgzn/`、`/jgsz/`。

---

## 5. 批量采集脚本参考

### 5.1 部门列表采集脚本模板

```python
#!/usr/bin/env python3
"""批量采集所有临沂部门 bmml 页面"""
import requests, re, os, sqlite3, json, time, sys, hashlib
from pathlib import Path

DB_PATH = '/tmp/linyi_work/people.sqlite'  # 操作时用 /tmp/ 避免 FUSE 问题
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.linyi.gov.cn/',
}

def process_bmml(bmml_url, unit_name):
    """处理单个部门 bmml 页面"""
    r = requests.get(bmml_url, headers=HEADERS, timeout=15)
    r.encoding = 'utf-8'
    
    vsb_imgs = re.findall(r'<img[^>]+src="(/virtual_attach_file\.vsb[^"]+)"[^>]*>', r.text)
    
    for vsb_url in vsb_imgs:
        pos = r.text.find(vsb_url)
        ctx = r.text[pos:pos+1200]
        ct = re.sub(r'<[^>]+>', '\n', ctx)
        lines = [l.strip() for l in ct.split('\n') if l.strip()]
        
        # 提取简介
        bio = ''
        for l in lines:
            if '，' in l and ('男' in l or '女' in l) and len(l) < 80:
                bio = l
                break
        
        if not bio:
            # 多行简介兜底
            for i, l in enumerate(lines):
                if re.match(r'^[一-鿿]{2,4}[，,]', l):
                    if i+1 < len(lines) and re.match(r'^(男|女)', lines[i+1]):
                        parts = [l.rstrip('，, ')]
                        for j in range(i+1, min(i+3, len(lines))):
                            parts.append(lines[j])
                        bio = '，'.join(parts)
                        break
        
        name = bio.split('，')[0] if bio else ''
        if not name or len(name) > 8:
            # 从文件名或上下文中推断
            name = guess_name_from_context(r.text, vsb_url)
        
        # 下载 VSB 图片
        img_url = 'https://www.linyi.gov.cn' + vsb_url
        resp = requests.get(img_url, headers=HEADERS, timeout=15)
        # ... 写入 registry 和文件系统
```

### 5.2 FUSE 磁盘 I/O 错误规避

由于 d2ilite 项目目录位于 Docker/FUSE 挂载上，SQLite 操作可能出现 `disk I/O error`。必须：

```python
# 操作前
import shutil
shutil.copy2(PROD_DB_PATH, '/tmp/linyi_work/people.sqlite')

# 操作中使用 /tmp/linyi_work/people.sqlite

# 操作后同步回去
shutil.copy2('/tmp/linyi_work/people.sqlite', PROD_DB_PATH)
```

---

## 6. 五批采集实战记录

第一批（页面一瞥）：探路，确认VSB可用 → 从单个页面提取10张。  
第二批（部门A-K）：zfb, fgw, jyj, kxjsj, xxhwyh, gaj, mzj, sfj, czj, sbj  
第三批（部门L-W）：gtzy, hbj, zfw, cgj, jtysj, slj, nwh, swj, gdj, wsj, tyjrswj  
第四批（部门W-Z + 特殊）：ajj, sjj, wsqw, gzw, xzspj, scjdj, tyj, tjj, ybj, fkb  
第五批（特殊单位）：dsjj, lsj, lyj, ymssjdzgy, zfgjj, dzj, gxhzs, glj, dftlj, jyjczx

共约220张图片入库。

---

## 7. 复用条件检查清单

复用bmml/VSB采集策略到新城市时，需确认：

- [ ] 该城市域名是否使用 VSB（`/virtual_attach_file.vsb`）
- [ ] 是否有 `bmml/` 路径的部门页面
- [ ] 是否有 `bmgk/jgzn.htm` 部门列表（或类似路径）
- [ ] 页面内VSB图片是否需要Referer头
- [ ] 简介是否在同一页面且可提取
- [ ] 是否使用多行简介格式
- [ ] 是否有VSB但无人物简介（仅新闻图片/横幅）

---

## 8. 常见失败原因

| 失败现象 | 原因 | 解决方案 |
|----------|------|----------|
| `disk I/O error` | FUSE挂载操作SQLite | 复制DB到`/tmp/`操作 |
| 0 VSB图片 | 页面不是bmml类型 | 检查URL结构 |
| 简介返回空 | `\s+`正则压扁了换行 | 改用`[ \t]+` |
| 多人同图 | 页面结构特殊 | 手动提取/浏览器兜底 |
| 图片403 | 缺少Referer头 | 添加域名Referer |
| 姓名异常 | 图片附近无文字/标题误抓 | 调整上下文窗口到1200+字符 |

---

## 9. 参考链接

- 临沂市机构职能列表：https://www.linyi.gov.cn/bmgk/jgzn.htm
- 临沂市政府办公室：https://www.linyi.gov.cn/bmml/zfb/
- 临沂市公安局（警察页）：https://www.linyi.gov.cn/info/6166/423539.htm
