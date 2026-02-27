# -*- coding: utf-8 -*-
"""文本解析模块 - 从简介文本自动提取结构化信息"""

import re

_LOCATION_SUFFIX_PATTERN = re.compile(
    r"[\u4e00-\u9fa5]{1,12}?(?:特别行政区|自治区|自治州|省|市|地区|盟|州|县|区|旗)"
)

_POSITION_HINTS = (
    "民警", "辅警", "警长", "警员", "刑警", "交警", "特警", "网警", "片警", "法警", "狱警",
    "支队长", "副支队长", "大队长", "副大队长", "中队长", "教导员",
    "所长", "副所长", "局长", "副局长", "处长", "副处长", "科长", "副科长",
    "主任", "书记", "委员", "经理", "总监", "工程师", "医师", "教师",
)

_POSITION_ENDINGS = (
    "警长", "民警", "辅警", "警员", "队长", "教导员", "所长", "局长", "处长", "科长",
    "主任", "书记", "委员", "经理", "总监", "工程师", "医师", "教师", "官", "员", "师", "长",
)

_POSITION_BLACKLIST = (
    "中共党员", "党员", "出生", "参加公安工作", "参加工作", "警衔", "维吾尔族", "汉族", "回族",
    "不幸", "牺牲", "抢救", "确认", "因公", "学历",
)

_ORG_SPLIT_TOKENS = (
    "公安局", "公安分局", "分局", "派出所",
    "司法局", "法院", "检察院", "政府", "公司", "集团", "学校", "医院", "委员会", "办公室",
)

_KEYWORD_MAX_COUNT = 6
_KEYWORD_MAX_LENGTH = 10
_KEYWORD_ALLOWED_SINGLE = {"男", "女"}
_KEYWORD_UNKNOWN_TOKENS = {
    "unknown",
    "unkonw",
    "n/a",
    "na",
    "none",
    "null",
    "未知",
    "不详",
    "未详",
    "待补充",
    "-",
}


def _normalize_text(text):
    s = str(text or "").replace("\u3000", " ").replace("\r", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", s).strip()


def _sanitize_keyword_token(value):
    raw = _normalize_text(value).strip().strip(",，、;；|/\\")
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered in _KEYWORD_UNKNOWN_TOKENS or raw in _KEYWORD_UNKNOWN_TOKENS:
        return ""
    if re.match(r"^https?://", raw, flags=re.IGNORECASE):
        return ""
    if re.search(r"[，。；;！？!?：:\n\r\t]", raw):
        return ""
    if re.search(r"\s", raw):
        return ""
    if len(raw) == 1 and raw not in _KEYWORD_ALLOWED_SINGLE:
        return ""
    if len(raw) > _KEYWORD_MAX_LENGTH:
        return ""
    if re.fullmatch(r"\d+", raw):
        return ""
    if re.fullmatch(r"\d{4}(?:[年/-]\d{1,2}(?:[月/-]\d{1,2})?)?", raw):
        return ""
    if re.fullmatch(r"\d{1,3}岁", raw):
        return ""
    return raw


def _compact_keywords(values, max_count=_KEYWORD_MAX_COUNT):
    cleaned = []
    seen = set()
    for kw in values or []:
        token = _sanitize_keyword_token(kw)
        if not token:
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(token)
        if len(cleaned) >= max(1, int(max_count or _KEYWORD_MAX_COUNT)):
            break
    return cleaned


def _compact_location(candidate):
    s = _normalize_text(candidate)
    if not s:
        return ""
    segments = _LOCATION_SUFFIX_PATTERN.findall(s)
    if not segments:
        return ""
    deduped = []
    for seg in segments:
        if not deduped or deduped[-1] != seg:
            deduped.append(seg)
    if len(deduped) >= 3 and (
        deduped[0].endswith("省") or deduped[0].endswith("自治区") or deduped[0].endswith("特别行政区")
    ):
        out = "".join(deduped[-2:])
    elif len(deduped) >= 2:
        out = "".join(deduped[-2:])
    else:
        out = deduped[0]
    if len(out) > 24:
        out = deduped[-1]
    return out.strip()


def _looks_like_position(candidate):
    s = _normalize_text(candidate)
    if not s or len(s) < 2 or len(s) > 36:
        return False
    if any(bad in s for bad in _POSITION_BLACKLIST):
        return False
    if any(h in s for h in _POSITION_HINTS):
        return True
    return any(s.endswith(x) for x in _POSITION_ENDINGS)


def _clean_position_candidate(candidate):
    s = _normalize_text(candidate).strip("，,。；;：: ")
    if not s:
        return ""
    s = re.sub(r"^(?:一名|一位|名|位|个|该|原|前|后|其)\s*", "", s)
    best_tail = ""
    best_idx = -1
    for token in _ORG_SPLIT_TOKENS:
        idx = s.rfind(token)
        if idx >= 0:
            tail = s[idx + len(token):].strip("的，,。；;：: ")
            if _looks_like_position(tail):
                if idx > best_idx:
                    best_idx = idx
                    best_tail = tail
    if best_tail:
        return best_tail
    return s


def _extract_position(text):
    s = _normalize_text(text)
    if not s:
        return None

    clause_patterns = [
        r"(?:生前系|生前任|现任|曾任|担任|历任|任职于|任)\s*([^，。；;\n]{2,100})",
        r"(?:系|是)\s*([^，。；;\n]{2,80})",
    ]
    for pattern in clause_patterns:
        for m in re.finditer(pattern, s):
            candidate = _clean_position_candidate(m.group(1))
            if _looks_like_position(candidate):
                return candidate

    direct_patterns = [
        r"([\u4e00-\u9fa5]{2,36}(?:民警|辅警|警长|警员|支队长|副支队长|大队长|副大队长|教导员|所长|副所长|局长|副局长|处长|副处长|科长|副科长|主任|书记|委员|经理|总监|工程师|医师|教师))",
    ]
    for pattern in direct_patterns:
        m = re.search(pattern, s)
        if not m:
            continue
        candidate = _clean_position_candidate(m.group(1))
        if _looks_like_position(candidate):
            return candidate
    return None


def _extract_hometown_or_city(text):
    s = _normalize_text(text)
    if not s:
        return None

    explicit_patterns = [
        r"(?:籍贯|户籍(?:地)?|家住|家乡)[：:\s]*([^\s，,。；;]{2,40})",
        r"([\u4e00-\u9fa5]{2,40}(?:省|市|县|区|自治区|自治州|地区|盟|州|旗))人(?:[，,。；;]|$)",
        r"([\u4e00-\u9fa5]{2,6})\s*人(?:[，,。；;]|$)",
    ]
    for pattern in explicit_patterns:
        m = re.search(pattern, s)
        if not m:
            continue
        raw_location = _normalize_text(m.group(1))
        if not raw_location or raw_location.endswith("族") or raw_location in ("中国", "我国", "本地", "当地"):
            continue
        location = _compact_location(raw_location)
        if not location and re.fullmatch(r"[\u4e00-\u9fa5]{2,6}", raw_location):
            location = raw_location
        if location:
            return location

    org_context_patterns = [
        r"(?:生前系|生前任|现任|曾任|担任|历任|任职于|在)?\s*([\u4e00-\u9fa5]{2,80}(?:省|市|自治区|自治州|地区|盟|州|县|区|旗))[\u4e00-\u9fa5]{0,24}?(?:公安局|公安分局|分局|派出所|支队|大队|中队|司法局|法院|检察院)",
    ]
    for pattern in org_context_patterns:
        m = re.search(pattern, s)
        if not m:
            continue
        location = _compact_location(m.group(1))
        if location:
            return location
    return None


def looks_like_person_name(text):
    """
    粗略判断一个字段是否像“人名”（用于列识别与下载兜底）。
    说明：不依赖表头；仅根据文本形态/语义特征判断。
    """
    if text is None:
        return False

    s = str(text).strip()
    if not s:
        return False

    # 常见情况：标题/姓名被拼成 "姓名 - 简介"
    if " - " in s:
        s = s.split(" - ", 1)[0].strip()

    # 2-4 个汉字（绝大多数中文姓名）
    if not re.fullmatch(r"[\u4e00-\u9fa5]{2,4}", s):
        return False

    # 4 字一般是复姓（如“欧阳xx/司马xx”）；否则极易把“先进事迹/工作动态”误判为姓名
    if len(s) == 4:
        compound_surnames = (
            "欧阳", "司马", "诸葛", "上官", "夏侯", "东方", "尉迟", "皇甫", "令狐", "公孙",
            "慕容", "长孙", "宇文", "司徒", "司空", "端木", "独孤", "南宫", "呼延", "闻人",
        )
        if not any(s.startswith(x) for x in compound_surnames):
            return False

    # 排除明显的组织/岗位/地名等（2-4 字也可能误判）
    stop_substrings = (
        "公安", "法院", "检察", "政府", "医院", "大学", "学院", "学校", "中心", "公司", "集团",
        "支队", "大队", "派出所", "分局", "党委", "办公室", "委员会",
    )
    if any(x in s for x in stop_substrings):
        return False

    # 排除常见岗位后缀
    stop_suffix = ("长", "员", "师", "官", "警", "主任", "书记", "经理", "总监", "院长", "局长", "科长", "处长")
    if any(s.endswith(x) for x in stop_suffix):
        return False

    return True


def extract_name_from_text(text):
    """从简介/描述文本中尝试抽取姓名（常见：开头“张三，男...”）。"""
    if not text:
        return None

    s = str(text).strip()
    if not s:
        return None

    # 1) 明示字段
    m = re.search(r"(?:姓名|当事人|作者)[：:\s]*([\u4e00-\u9fa5]{2,4})", s)
    if m:
        candidate = m.group(1).strip()
        if looks_like_person_name(candidate):
            return candidate

    # 2) 句首：张三，男... / 张三（...）
    m = re.match(r"^([\u4e00-\u9fa5]{2,4})(?:[，,、\s]|（|\()", s)
    if m:
        candidate = m.group(1).strip()
        if looks_like_person_name(candidate):
            return candidate

    # 3) 句中：张三，男/女/xx岁/xxxx年...
    m = re.search(r"([\u4e00-\u9fa5]{2,4})(?:[，,、]|（|\()\s*(?:男|女|\d{1,3}\s*岁|\d{4}\s*年)", s)
    if m:
        candidate = m.group(1).strip()
        if looks_like_person_name(candidate):
            return candidate

    return None


def extract_person_info(text):
    """
    从简介文本自动提取结构化信息
    
    Args:
        text: 简介文本
    
    Returns:
        dict: {
            'gender': '男'|'女'|None,
            'birth_year': '1990'|None,
            'age': '35'|None,
            'profession': '教师'|None,
            'hometown': '山东省济南市'|None,
            'keywords': ['关键词1', '关键词2', ...]
        }
    """
    if not text:
        return {}
    
    info = {}
    keywords = []
    
    # ===== 性别提取 =====
    male_patterns = r'男性|男子|先生|他的|其父|丈夫|男，'
    female_patterns = r'女性|女子|女士|她的|其母|妻子|女，'
    
    male_count = len(re.findall(male_patterns, text))
    female_count = len(re.findall(female_patterns, text))
    
    if male_count > female_count:
        info['gender'] = '男'
        keywords.append('男')
    elif female_count > male_count:
        info['gender'] = '女'
        keywords.append('女')
    
    # ===== 出生年份 =====
    # 匹配：1990年出生、生于1990年、1990年生
    birth_patterns = [
        r'(\d{4})\s*年\s*(出生|生于|生)',
        r'生于\s*(\d{4})\s*年',
        r'(\d{4})\s*年\s*\d{1,2}\s*月.*?(出生|生)',
    ]
    for pattern in birth_patterns:
        match = re.search(pattern, text)
        if match:
            year = match.group(1)
            if 1900 <= int(year) <= 2025:
                info['birth_year'] = year
                break
    
    # ===== 年龄 =====
    age_match = re.search(r'(\d{1,3})\s*岁', text)
    if age_match:
        age = int(age_match.group(1))
        if 1 <= age <= 120:
            info['age'] = str(age)
    
    # ===== 职业（一般性分类）=====
    # 职业映射表：细分职业 -> 通用职业
    PROFESSION_MAPPING = {
        '刑警': '民警', '交警': '民警', '特警': '民警', '网警': '民警', 
        '狱警': '民警', '法警': '民警', '辅警': '民警', '片警': '民警',
        '武警': '军人', '解放军': '军人',
        '讲师': '教师', '教授': '教师', '副教授': '教师',
        '护师': '护士', '护士长': '护士',
        '程序员': '工程师', '架构师': '工程师',
    }
    
    # 职业词表（包含通用和细分）
    profession_keywords = [
        # 警务/执法 (细分)
        '刑警', '交警', '特警', '网警', '狱警', '法警', '辅警', '片警', '武警',
        # 警务/执法 (通用)
        '民警', '警察', '公安',
        # 公务员
        '公务员', '干部', '官员', '军人', '士兵', '军官',
        '法官', '检察官', '律师', '书记员',
        # 教育
        '教师', '教授', '讲师', '老师', '校长', '园长',
        # 医疗
        '医生', '护士', '护师', '医师', '药师', '主任医师',
        # 技术
        '工程师', '程序员', '技术员', '技师', '设计师', '建筑师',
        # 文艺
        '演员', '歌手', '作家', '记者', '编辑', '主持人', '导演',
        # 商业
        '会计', '经理', '总监', '董事', '企业家', '商人',
        # 其他
        '农民', '工人', '司机', '厨师', '服务员',
    ]
    
    raw_profession = None
    # 先从文本中匹配职业关键词
    for keyword in profession_keywords:
        if keyword in text:
            raw_profession = keyword
            # 映射到标准职业
            std_profession = PROFESSION_MAPPING.get(keyword, keyword)
            info['profession'] = std_profession
            
            # 将标准职业加入关键词
            if std_profession not in keywords:
                keywords.append(std_profession)
            break
    
    # ===== 职务（具体岗位）=====
    extracted_position = _extract_position(text)
    if extracted_position:
        info['position'] = extracted_position
        if extracted_position not in keywords:
            keywords.append(extracted_position)
    
    # 如果没有提取到具体职务，但有细分职业（如刑警），则将细分职业作为职务
    if 'position' not in info and raw_profession and raw_profession in PROFESSION_MAPPING:
        info['position'] = raw_profession
    if info.get('position') and (
        ('profession' not in info) or info.get('profession') in ('公安', '警察')
    ):
        pos = str(info['position'])
        if any(x in pos for x in ('民警', '辅警', '警长', '警员', '刑警', '交警', '特警', '网警', '法警', '狱警', '片警')):
            info['profession'] = '民警'
    if info.get('profession') and info['profession'] not in keywords:
        keywords.insert(0, info['profession'])
        
    # 确保原始细分职业（如刑警）也在关键词里
    if raw_profession and raw_profession not in keywords:
        keywords.append(raw_profession)
    
    # ===== 籍贯/地点 =====
    extracted_hometown = _extract_hometown_or_city(text)
    if extracted_hometown:
        info['hometown'] = extracted_hometown
        if extracted_hometown not in keywords:
            keywords.append(extracted_hometown)
    
    # ===== 荣誉/称号 =====
    honor_patterns = [
        r'(全国[\u4e00-\u9fa5]{2,8})',
        r'([\u4e00-\u9fa5]{2,4}模范)',
        r'(一等功|二等功|三等功)',
        r'获得["""]([\u4e00-\u9fa5]{3,15})["""]',
        r'荣获["""]([\u4e00-\u9fa5]{3,15})["""]',
    ]
    
    for pattern in honor_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and len(match) >= 3:
                keywords.append(match)
    
    # 去重并限制关键词数量（少而精）
    info['keywords'] = _compact_keywords(keywords, max_count=_KEYWORD_MAX_COUNT)
    
    return info


def build_metadata_from_item(item):
    """
    从数据项构建完整的元数据字典
    
    Args:
        item: {
            'name': str,
            'intro': str,
            'url': str,            # 原图链接（兼容旧字段）
            'image_url': str,      # 原图链接（新字段，优先）
            'source': str (optional)  # 来源页面链接
        }
    
    Returns:
        dict: 可直接传给 write_xmp_metadata 的元数据
    """
    raw_name = item.get('name', '')
    name = str(raw_name).strip() if raw_name is not None else ''
    intro = item.get('intro', '')

    # 兜底：抓取数据里“题头/标题”常不可靠，优先从简介语义中提取姓名
    if name and " - " in name:
        candidate = name.split(" - ", 1)[0].strip()
        if looks_like_person_name(candidate):
            name = candidate

    intro_text = str(intro).strip() if intro else ''
    derived_name = extract_name_from_text(intro_text)
    if derived_name:
        # 只要简介里能明确抽取到姓名，就优先用它（抓取题头/岗位经常误导）
        if (not name) or (not intro_text.startswith(name)) or (not looks_like_person_name(name)):
            name = derived_name

    # image_url：原图链接（用于重下/溯源）
    raw_image_url = item.get('image_url')
    if raw_image_url is None or str(raw_image_url).strip() == '':
        raw_image_url = item.get('url', '')
    image_url = str(raw_image_url).strip() if raw_image_url is not None else ''

    # source：来源页面（若缺失，回退到 image_url）
    raw_source = item.get('source')
    source = str(raw_source).strip() if raw_source is not None else ''
    if not source:
        source = image_url
    
    # 从简介提取信息
    extracted = extract_person_info(intro)
    
    # 构建关键词列表
    keywords = extracted.get('keywords', [])
    
    # 添加职业和性别到关键词（如果尚未包含）
    if extracted.get('profession') and extracted['profession'] not in keywords:
        keywords.insert(0, extracted['profession'])
    if extracted.get('gender') and extracted['gender'] not in keywords:
        keywords.insert(0, extracted['gender'])
    keywords = _compact_keywords(keywords, max_count=_KEYWORD_MAX_COUNT)
    
    return {
        # 将简介追加到标题，以便在 Windows 悬停时显示完整信息
        'title': f"{name} - {intro}" if intro else name,
        'description': intro,
        'keywords': keywords,
        'source': source,
        'image_url': image_url,
        'city': extracted.get('hometown'),
        'person': name,
        'gender': extracted.get('gender'),
        'position': extracted.get('position'),  # 传递职务字段
        'extracted': extracted  # 保留提取的原始信息用于调试
    }
