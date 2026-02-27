# -*- coding: utf-8 -*-
"""
元数据管理模块 - 扫描、读取、更新、清洗图片元数据
"""

import os
import re
import hashlib
import shutil
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from PIL import Image
import piexif

# 尝试导入 pyexiv2
try:
    import pyexiv2
    HAS_PYEXIV2 = True
    try:
        pyexiv2.registerNs('urn:titi:ns:1.0', 'titi')
    except Exception:
        pass
except ImportError:
    HAS_PYEXIV2 = False

from text_parser import extract_person_info, extract_name_from_text, build_metadata_from_item

KEYWORD_MAX_COUNT = 6
KEYWORD_MAX_LENGTH = 10
KEYWORD_ALLOWED_SINGLE = {"男", "女"}
KEYWORD_UNKNOWN_TOKENS = {
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


class MetadataStatus(Enum):
    """元数据状态"""
    NONE = "none"           # 无元数据
    PARTIAL = "partial"     # 部分元数据（有描述但缺关键词等）
    COMPLETE = "complete"   # 完整元数据


@dataclass
class ImageMetadataInfo:
    """图片元数据信息"""
    filepath: str
    filename: str
    filesize: int
    modified_time: datetime
    
    # A层：人类可读元数据
    title: str = ""
    description: str = ""
    keywords: List[str] = field(default_factory=list)
    source: str = ""
    image_url: str = ""  # 原始图片直链（用于重下载）
    city: str = ""
    person: str = ""
    gender: str = ""
    position: str = ""  # 职务/职称
    police_id: str = ""  # 警号/证号（可留空）
    
    # B层：TITI 身份字段
    titi_asset_id: str = ""
    titi_world_id: str = ""
    titi_json: Optional[Dict] = None
    
    # 其他原有字段（保留用）
    other_xmp: Dict = field(default_factory=dict)
    other_exif: Dict = field(default_factory=dict)
    other_iptc: Dict = field(default_factory=dict)
    
    # 状态
    status: MetadataStatus = MetadataStatus.NONE
    
    # 匹配的表格行（如果有）
    matched_row: Optional[dict] = None
    
    @property
    def keywords_count(self) -> int:
        return len(self.keywords)
    
    @property
    def status_text(self) -> str:
        if self.status == MetadataStatus.COMPLETE:
            return "✓ 完整"
        elif self.status == MetadataStatus.PARTIAL:
            return "⚠ 部分"
        else:
            return "✗ 无"


def _looks_garbled_question_marks(text: str) -> bool:
    s = str(text or "").replace("\x00", "").strip()
    if not s:
        return False
    chars = [ch for ch in s if not ch.isspace()]
    if not chars:
        return False
    q_like = sum(1 for ch in chars if ch in {"?", "？", "�"})
    if q_like < 6:
        return False
    return (q_like / max(len(chars), 1)) >= 0.45


def _normalize_gender_value(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    unknown_tokens = {
        "unknown",
        "unkonw",
        "n/a",
        "na",
        "none",
        "null",
        "未知",
        "未详",
        "不详",
        "待补充",
        "-",
    }
    if lowered in unknown_tokens or raw in unknown_tokens:
        return ""
    if lowered in {"male", "m", "man", "男性"} or raw == "男":
        return "男"
    if lowered in {"female", "f", "woman", "女性"} or raw == "女":
        return "女"
    return raw


def _normalize_police_id_value(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    unknown_tokens = {
        "unknown",
        "unkonw",
        "n/a",
        "na",
        "none",
        "null",
        "未知",
        "未详",
        "不详",
        "待补充",
        "-",
    }
    if lowered in unknown_tokens or raw in unknown_tokens:
        return ""
    return raw


def _extract_police_id_from_profile(profile: Any) -> str:
    if not isinstance(profile, dict):
        return ""
    candidate_keys = (
        "police_id",
        "police_no",
        "police_number",
        "badge_no",
        "badge_id",
        "badge_number",
        "officer_id",
        "警号",
    )
    for key in candidate_keys:
        value = _normalize_police_id_value(profile.get(key))
        if value:
            return value
    extra_fields = profile.get("extra_fields")
    if isinstance(extra_fields, dict):
        for key in candidate_keys:
            value = _normalize_police_id_value(extra_fields.get(key))
            if value:
                return value
    return ""


def _extract_police_id_from_titi_json(titi_json: Any) -> str:
    if not isinstance(titi_json, dict):
        return ""
    return _extract_police_id_from_profile(titi_json.get("d2i_profile"))


def _sanitize_human_description(text: str) -> str:
    s = str(text or "").replace("\x00", "").strip()
    if not s:
        return ""
    # 兼容部分库把二进制字段序列化成 "85 78 73 ..." 或 "(85, 78, ...)" 文本。
    # 优先尝试还原成真实文本；无法还原则直接丢弃，避免污染描述。
    decoded_blob = _decode_numeric_blob_text(s)
    if decoded_blob:
        s = decoded_blob
    elif _parse_numeric_blob_to_bytes(s) is not None:
        return ""
    if _looks_garbled_question_marks(s):
        return ""
    return s


def _parse_numeric_blob_to_bytes(text: str) -> Optional[bytes]:
    s = str(text or "").strip()
    if not s:
        return None
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()
    if not s:
        return None
    s = s.replace(",", " ").replace(";", " ")
    parts = [p.strip() for p in s.split() if p.strip()]
    if len(parts) < 8:
        return None
    nums: List[int] = []
    for p in parts:
        if not p.lstrip("+-").isdigit():
            return None
        n = int(p)
        if n < 0 or n > 255:
            return None
        nums.append(n)
    return bytes(nums) if nums else None


def _decode_binary_text(raw: bytes) -> str:
    if not raw:
        return ""
    # EXIF UserComment 规范前缀处理
    if raw.startswith(b"UNICODE\x00"):
        payload = raw[8:]
        for enc in ("utf-16le", "utf-16be", "utf-8"):
            try:
                s = payload.decode(enc, errors="ignore").replace("\x00", "").strip().lstrip("\ufeff")
            except Exception:
                continue
            if s and (not _looks_garbled_question_marks(s)):
                return s
        return ""
    if raw.startswith(b"ASCII\x00"):
        try:
            s = raw[8:].decode("ascii", errors="ignore").replace("\x00", "").strip()
            if s and (not _looks_garbled_question_marks(s)):
                return s
        except Exception:
            pass
        return ""

    for enc in ("utf-16le", "utf-16be", "utf-8", "gb18030", "latin1"):
        try:
            s = raw.decode(enc, errors="ignore").replace("\x00", "").strip()
        except Exception:
            continue
        s = s.lstrip("\ufeff")
        if not s:
            continue
        if _looks_garbled_question_marks(s):
            continue
        bad = sum(1 for ch in s if (ord(ch) < 32 and ch not in {"\n", "\r", "\t"}))
        if bad > max(2, len(s) // 20):
            continue
        return s
    return ""


def _decode_numeric_blob_text(text: str) -> str:
    raw = _parse_numeric_blob_to_bytes(text)
    if raw is None:
        return ""
    return _decode_binary_text(raw)


def _suggest_fill_from_description(info: ImageMetadataInfo) -> Dict[str, Any]:
    """
    从 description 生成“可选自动填空建议”（不修改 info、不写回文件）。
    仅返回当前缺失字段的候选值。
    """
    suggestion: Dict[str, Any] = {}
    desc = str(getattr(info, "description", "") or "").strip()
    if not desc:
        return suggestion
    try:
        extracted = extract_person_info(desc) or {}
    except Exception:
        extracted = {}

    person_candidate = ""
    if not info.person:
        try:
            guessed = extract_name_from_text(desc)
            if isinstance(guessed, str) and guessed.strip():
                person_candidate = guessed.strip()
        except Exception:
            person_candidate = ""
    if (not person_candidate) and (not info.person):
        # 兼容少数民族姓名（常含中点，如“阿力木·阿不力米提”）
        try:
            m = re.match(r"^\s*([^\s，,、；;：:（(]{2,40})[，,、；;：:（(]", desc)
            if m:
                candidate = str(m.group(1) or "").strip()
                # 只接受包含中文字符的候选，避免把整句误当姓名。
                if re.search(r"[\u4e00-\u9fff]", candidate):
                    person_candidate = candidate
        except Exception:
            person_candidate = ""
    if person_candidate:
        suggestion["person"] = person_candidate

    if (not info.title) and person_candidate:
        suggestion["title"] = person_candidate

    if not info.gender:
        g = _normalize_gender_value(extracted.get("gender"))
        if g:
            suggestion["gender"] = g

    if (not info.position) and isinstance(extracted.get("position"), str):
        p = str(extracted.get("position") or "").strip()
        if p:
            suggestion["position"] = p

    if (not info.city) and isinstance(extracted.get("hometown"), str):
        c = str(extracted.get("hometown") or "").strip()
        if c:
            suggestion["city"] = c

    if (not info.keywords) and isinstance(extracted.get("keywords"), list):
        kws = clean_keywords(extracted.get("keywords") or [])
        if kws:
            suggestion["keywords"] = kws
    return suggestion


def suggest_metadata_fill(info: ImageMetadataInfo) -> Dict[str, Any]:
    """
    公开接口：基于当前图片信息生成自动填空建议（手动触发时调用）。
    """
    if not isinstance(info, ImageMetadataInfo):
        return {}
    try:
        return _suggest_fill_from_description(info)
    except Exception:
        return {}


def _extract_first_http_url(text: Any) -> str:
    s = str(text or "").replace("\x00", "").strip()
    if not s:
        return ""

    m = re.search(r"https?://", s, flags=re.IGNORECASE)
    if not m:
        return s

    tail = s[m.start():]
    # 两个 URL 粘连时，截取第一个。
    m2 = re.search(r"https?://", tail[len(m.group(0)):], flags=re.IGNORECASE)
    if m2:
        cut = len(m.group(0)) + m2.start()
        tail = tail[:cut]

    # 去掉常见尾部分隔符/噪声。
    for sep in ('"', "'", "<", ">", " ", "\r", "\n", "\t"):
        idx = tail.find(sep)
        if idx >= 0:
            tail = tail[:idx]
            break

    tail = tail.strip().rstrip("，,。.;；）)]}>")
    return tail


def _clean_link_text(text: Any) -> str:
    return _extract_first_http_url(text)


def _extract_image_url_from_titi_json(titi_json: Any) -> str:
    if not isinstance(titi_json, dict):
        return ""

    # 1) 直接字段（推荐）
    direct = _clean_link_text(titi_json.get("source_image") or titi_json.get("image_url"))
    if direct:
        return direct

    # 2) source_images 列表
    src_images = titi_json.get("source_images")
    if isinstance(src_images, list):
        for val in src_images:
            s = _clean_link_text(val)
            if s:
                return s

    # 3) source_inputs 结构
    src_inputs = titi_json.get("source_inputs")
    if isinstance(src_inputs, list):
        for item in src_inputs:
            if not isinstance(item, dict):
                continue
            for key in ("source_image", "url", "image_url", "filename", "path"):
                s = _clean_link_text(item.get(key))
                if s:
                    return s

    # 4) d2i_profile 回退
    profile = titi_json.get("d2i_profile")
    if isinstance(profile, dict):
        s = _clean_link_text(profile.get("image_url") or profile.get("url"))
        if s:
            return s

    return ""


def _pixel_fingerprint(filepath: str) -> Optional[Tuple[Tuple[int, int], str, str]]:
    """
    计算图片像素指纹（尺寸 + 模式 + 像素字节 SHA256）。

    说明：
    - 该指纹只用于“元数据写入前后像素是否变化”的守卫；
    - 读取失败时返回 None，调用方可选择跳过像素校验。
    """
    path = str(filepath or "").strip()
    if not path or (not os.path.isfile(path)):
        return None
    try:
        with Image.open(path) as img:
            img.load()
            size = (int(img.size[0]), int(img.size[1]))
            mode = str(img.mode or "")
            payload = img.tobytes()
        digest = hashlib.sha256(payload).hexdigest()
        return (size, mode, digest)
    except Exception:
        return None


def _safe_replace_with_pixel_guard(
    original_path: str,
    candidate_path: str,
    *,
    original_fp: Optional[Tuple[Tuple[int, int], str, str]],
    op_label: str,
) -> bool:
    """
    用候选文件覆盖原文件前后做像素一致性校验。

    行为：
    1) 若可计算指纹，则先校验 candidate 与 original 像素一致；
    2) 覆盖前做临时备份，覆盖后再次校验；
    3) 任一环节失败则回滚原图并返回 False。
    """
    src = str(original_path or "").strip()
    cand = str(candidate_path or "").strip()
    if (not src) or (not cand) or (not os.path.isfile(src)) or (not os.path.isfile(cand)):
        return False

    backup_path = ""
    ext = os.path.splitext(src)[1] or ".bak"
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    try:
        # 覆盖前先验证候选文件像素未变化。
        if original_fp is not None:
            cand_fp = _pixel_fingerprint(cand)
            if (cand_fp is None) or (cand_fp != original_fp):
                reject_copy = f"{src}.reject_{op_label}_{ts}{ext}"
                try:
                    shutil.copy2(cand, reject_copy)
                except Exception:
                    pass
                print(f"[错误] {op_label} 像素校验失败（覆盖前），已拒绝覆盖: {src}")
                return False

        fd, backup_path = tempfile.mkstemp(suffix=ext)
        os.close(fd)
        shutil.copy2(src, backup_path)
        shutil.copy2(cand, src)

        # 覆盖后复核，异常则回滚。
        if original_fp is not None:
            final_fp = _pixel_fingerprint(src)
            if (final_fp is None) or (final_fp != original_fp):
                try:
                    shutil.copy2(backup_path, src)
                except Exception:
                    pass
                print(f"[错误] {op_label} 像素校验失败（覆盖后），已自动回滚: {src}")
                return False

        return True
    except Exception as e:
        if backup_path and os.path.exists(backup_path):
            try:
                shutil.copy2(backup_path, src)
            except Exception:
                pass
        print(f"[错误] {op_label} 覆盖失败，已尝试回滚 ({src}): {e}")
        return False
    finally:
        if backup_path and os.path.exists(backup_path):
            try:
                os.remove(backup_path)
            except Exception:
                pass


def clear_exif_image_description(filepath: str) -> bool:
    """
    清空 EXIF ImageDescription(0x010E)。

    返回:
        bool: 是否实际写入了变更
    """
    path = str(filepath or "").strip()
    if not path or (not os.path.isfile(path)):
        return False

    # 该字段主要存在于 JPEG/TIFF/WEBP；其它格式直接跳过。
    ext = os.path.splitext(path)[1].lower()
    if ext not in {".jpg", ".jpeg", ".tif", ".tiff", ".webp"}:
        return False

    original_fp = _pixel_fingerprint(path)

    def _clear_with_pillow_piexif(target_path: str) -> bool:
        try:
            with Image.open(target_path) as pil_img:
                exif_obj = pil_img.getexif()
                if 270 not in exif_obj:
                    return False
                exif_obj[270] = ""
                exif_bytes = exif_obj.tobytes()
            piexif.insert(exif_bytes, target_path)
            return True
        except Exception:
            return False

    def _clear_with_pyexiv2(target_path: str) -> bool:
        if not HAS_PYEXIV2:
            return False
        try:
            with pyexiv2.Image(target_path) as img:
                exif_data = img.read_exif() or {}
                if "Exif.Image.ImageDescription" not in exif_data:
                    return False
                img.modify_exif({"Exif.Image.ImageDescription": ""})
                return True
        except Exception:
            return False

    def _clear_with_piexif_delete(target_path: str) -> bool:
        try:
            exif_dict = piexif.load(target_path)
            zeroth = exif_dict.get("0th")
            if isinstance(zeroth, dict) and (piexif.ImageIFD.ImageDescription in zeroth):
                del zeroth[piexif.ImageIFD.ImageDescription]
                exif_dict["0th"] = zeroth
                piexif.insert(piexif.dump(exif_dict), target_path)
                return True
        except Exception:
            return False
        return False

    # 安全写入：在临时副本上执行，成功且像素一致后再覆盖原图。
    fd, tmp_path = tempfile.mkstemp(suffix=ext or ".jpg")
    os.close(fd)
    changed = False
    try:
        shutil.copy2(path, tmp_path)
        changed = _clear_with_pillow_piexif(tmp_path)
        if not changed:
            changed = _clear_with_pyexiv2(tmp_path)
        if not changed:
            changed = _clear_with_piexif_delete(tmp_path)
        if not changed:
            return False
        return _safe_replace_with_pixel_guard(
            path,
            tmp_path,
            original_fp=original_fp,
            op_label="clear_exif_desc",
        )
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def scan_directory(directory: str, extensions: tuple = ('.jpg', '.jpeg', '.png')) -> List[str]:
    """
    扫描目录获取所有图片文件
    
    Args:
        directory: 目录路径
        extensions: 支持的文件扩展名
    
    Returns:
        图片文件路径列表
    """
    images = []
    
    if not os.path.isdir(directory):
        return images
    
    for filename in os.listdir(directory):
        if filename.lower().endswith(extensions):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                images.append(filepath)
    
    return sorted(images)


def read_image_metadata(filepath: str) -> ImageMetadataInfo:
    """
    读取图片的所有元数据
    
    Args:
        filepath: 图片路径
    
    Returns:
        ImageMetadataInfo 对象
    """
    filename = os.path.basename(filepath)
    stat = os.stat(filepath)
    
    info = ImageMetadataInfo(
        filepath=filepath,
        filename=filename,
        filesize=stat.st_size,
        modified_time=datetime.fromtimestamp(stat.st_mtime)
    )
    
    if HAS_PYEXIV2:
        try:
            _read_with_pyexiv2(filepath, info)
        except Exception:
            # pyexiv2 在 Windows 上对中文路径/长路径/网络盘有已知兼容问题；
            # 读取失败时回退到 piexif，至少保证 EXIF/XP* 可用。
            _read_with_piexif(filepath, info)
    else:
        _read_with_piexif(filepath, info)

    # B 层回退：PNG text（key=titi）
    if not info.titi_json and filepath.lower().endswith('.png'):
        try:
            import json
            with Image.open(filepath) as img:
                raw = None
                if isinstance(getattr(img, "info", None), dict):
                    raw = img.info.get("titi") or img.info.get("TITI")
                if raw:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="ignore")
                    s = str(raw).strip().strip("\x00")
                    if s.startswith("{") and s.endswith("}"):
                        parsed = json.loads(s)
                        if isinstance(parsed, dict) and ("titi_asset_id" in parsed or parsed.get("schema") == "titi-meta"):
                            info.titi_json = parsed
                            info.titi_asset_id = parsed.get("titi_asset_id", "")
                            info.titi_world_id = parsed.get("titi_world_id", "")
                            info.image_url = _extract_image_url_from_titi_json(parsed)
                            gender = (parsed.get("d2i_profile") or {}).get("gender")
                            g = _normalize_gender_value(gender)
                            if g:
                                info.gender = g
                            if not info.person:
                                name = (parsed.get("d2i_profile") or {}).get("name")
                                if isinstance(name, str) and name.strip():
                                    info.person = name.strip()
        except Exception:
            pass

    # 性别兜底：优先使用 titi:meta(d2i_profile.gender)，其次从关键词中提取
    info.gender = _normalize_gender_value(info.gender)
    if not info.gender and info.keywords:
        kw_set = set([str(k).strip() for k in info.keywords if k is not None])
        male_hits = {"男", "男性", "male", "m", "man"} & {k.lower() if isinstance(k, str) else str(k) for k in kw_set}
        female_hits = {"女", "女性", "female", "f", "woman"} & {k.lower() if isinstance(k, str) else str(k) for k in kw_set}
        if male_hits and not female_hits:
            info.gender = "男"
        elif female_hits and not male_hits:
            info.gender = "女"

    # 原图链接兜底：从 titi_json/source_inputs/source_images 提取
    if (not info.image_url) and info.titi_json:
        info.image_url = _extract_image_url_from_titi_json(info.titi_json)

    # 警号兜底：从 titi_json.d2i_profile 及其 extra_fields 提取
    if (not info.police_id) and info.titi_json:
        info.police_id = _extract_police_id_from_titi_json(info.titi_json)

    # 判断状态
    info.status = _determine_status(info)
    
    return info



def _read_with_pyexiv2(filepath: str, info: ImageMetadataInfo):
    """使用 pyexiv2 读取元数据（含 Unicode 路径兼容处理）"""
    
    def _decode_exif_text(val) -> str:
        """将常见的 EXIF/XMP 值尽可能转成 str"""
        if val is None:
            return ""
        if isinstance(val, str):
            decoded_blob = _decode_numeric_blob_text(val)
            if decoded_blob:
                return decoded_blob
            if _parse_numeric_blob_to_bytes(val) is not None:
                return ""
            return val
        if isinstance(val, bytes):
            # EXIF UserComment 常见前缀
            if val.startswith(b"UNICODE\x00"):
                data = val[8:]
                text = _decode_binary_text(data)
                if text:
                    return text
            if val.startswith(b"ASCII\x00"):
                try:
                    text = val[8:].decode("ascii", errors="ignore").replace("\x00", "").strip()
                    if text:
                        return text
                except Exception:
                    return ""
            return _decode_binary_text(val)
        if isinstance(val, (list, tuple)):
            try:
                nums = [int(x) for x in val]
                if nums and all(0 <= n <= 255 for n in nums):
                    return _decode_binary_text(bytes(nums))
            except Exception:
                return ""
        return str(val)

    def _try_parse_titi_json(text: str) -> Optional[Dict]:
        if not text:
            return None
        s = str(text).strip().strip("\x00")
        if not s:
            return None
        if not (s.startswith("{") and s.endswith("}")):
            return None
        try:
            import json
            data = json.loads(s)
            if isinstance(data, dict) and ("titi_asset_id" in data or data.get("schema") == "titi-meta"):
                return data
        except Exception:
            return None
        return None

    def _extract_data(img):
        xmp_data = img.read_xmp()
        
        # 标题
        if 'Xmp.dc.title' in xmp_data:
            title = xmp_data['Xmp.dc.title']
            if isinstance(title, dict):
                info.title = list(title.values())[0] if title else ''
            else:
                info.title = str(title)
        
        # 描述
        if 'Xmp.dc.description' in xmp_data:
            desc = xmp_data['Xmp.dc.description']
            if isinstance(desc, dict):
                info.description = list(desc.values())[0] if desc else ''
            else:
                info.description = str(desc)
            info.description = _sanitize_human_description(info.description)
        
        # 关键词
        if 'Xmp.dc.subject' in xmp_data:
            keywords = xmp_data['Xmp.dc.subject']
            if isinstance(keywords, list):
                info.keywords = keywords
            elif keywords:
                info.keywords = [str(keywords)]
        
        # 来源
        if 'Xmp.dc.source' in xmp_data:
            info.source = xmp_data['Xmp.dc.source']
        
        # 原图链接（若存在自定义字段）
        if 'Xmp.titi.sourceImage' in xmp_data:
            info.image_url = _clean_link_text(xmp_data.get('Xmp.titi.sourceImage'))
        elif 'Xmp.titi.imageUrl' in xmp_data:
            info.image_url = _clean_link_text(xmp_data.get('Xmp.titi.imageUrl'))
        
        # 城市
        if 'Xmp.photoshop.City' in xmp_data:
            info.city = xmp_data['Xmp.photoshop.City']
        
        # 人物
        if 'Xmp.iptcExt.PersonInImage' in xmp_data:
            persons = xmp_data['Xmp.iptcExt.PersonInImage']
            info.person = persons[0] if persons else ''
        
        # 职务 (Job Title)
        if 'Xmp.photoshop.AuthorsPosition' in xmp_data:
            info.position = xmp_data['Xmp.photoshop.AuthorsPosition']
        elif 'Xmp.iptc.CreatorJobTitle' in xmp_data:
            info.position = xmp_data['Xmp.iptc.CreatorJobTitle']
        
        # B层：TITI 身份
        if 'Xmp.titi.meta' in xmp_data:
            try:
                import json
                titi_json = json.loads(xmp_data['Xmp.titi.meta'])
                info.titi_asset_id = titi_json.get('titi_asset_id', '')
                info.titi_world_id = titi_json.get('titi_world_id', '')
                info.titi_json = titi_json
                if not info.image_url:
                    info.image_url = _extract_image_url_from_titi_json(titi_json)
                gender = (titi_json.get("d2i_profile") or {}).get("gender")
                g = _normalize_gender_value(gender)
                if g:
                    info.gender = g
            except Exception:
                pass

        # 如果 PersonInImage 缺失，尝试从 TITI JSON(d2i_profile.name) 回填姓名
        if not info.person and info.titi_json:
            try:
                name = (info.titi_json.get("d2i_profile") or {}).get("name")
                if isinstance(name, str) and name.strip():
                    info.person = name.strip()
            except Exception:
                pass
        
        # 保存完整 XMP 原始字段（用于“全量元数据”展示与调试）。
        info.other_xmp = dict(xmp_data or {})
        
        # 读取 EXIF
        exif_data = img.read_exif()
        info.other_exif = exif_data
        try:
            info.other_iptc = dict(img.read_iptc() or {})
        except Exception:
            info.other_iptc = {}

        # B 层回退：尝试从 EXIF UserComment 解析 TITI JSON（仅 JSON，不当作简介）
        if not info.titi_json and 'Exif.Photo.UserComment' in exif_data:
            try:
                comment_text = _decode_exif_text(exif_data.get('Exif.Photo.UserComment'))
                parsed = _try_parse_titi_json(comment_text)
                if parsed:
                    info.titi_json = parsed
                    info.titi_asset_id = parsed.get('titi_asset_id', '') or info.titi_asset_id
                    info.titi_world_id = parsed.get('titi_world_id', '') or info.titi_world_id
                    if not info.image_url:
                        info.image_url = _extract_image_url_from_titi_json(parsed)
                    gender = (parsed.get("d2i_profile") or {}).get("gender")
                    g = _normalize_gender_value(gender)
                    if g:
                        info.gender = g
                    if not info.person:
                        name = (parsed.get("d2i_profile") or {}).get("name")
                        if isinstance(name, str) and name.strip():
                            info.person = name.strip()
            except Exception:
                pass
        
        # --- 回退策略：如果 XMP 没读到，尝试读 EXIF ---
        
        # 1. 描述回退
        if not info.description:
            # 优先 XPComment (Windows 备注)，通常是 UTF-16LE
            if 'Exif.Image.XPComment' in exif_data:
                try:
                    val = exif_data['Exif.Image.XPComment']
                    info.description = _decode_exif_text(val)
                    info.description = _sanitize_human_description(info.description)
                except Exception:
                    pass

            # 回退 Exif.Image.ImageDescription（ASCII-only，可能只是短摘要/占位）
            if not info.description and 'Exif.Image.ImageDescription' in exif_data:
                try:
                    info.description = _sanitize_human_description(_decode_exif_text(exif_data.get('Exif.Image.ImageDescription')))
                except Exception:
                    info.description = _sanitize_human_description(str(exif_data['Exif.Image.ImageDescription']))

            # 兼容旧数据：UserComment 不是 JSON 时才允许当作简介回退
            if not info.description and 'Exif.Photo.UserComment' in exif_data:
                try:
                    user_comment_text = _decode_exif_text(exif_data.get('Exif.Photo.UserComment'))
                    if not _try_parse_titi_json(user_comment_text):
                        info.description = _sanitize_human_description(user_comment_text)
                except Exception:
                    pass

        # 4. 描述最终兜底：如果展示位已损坏，尝试从 titi_json.d2i_profile.description 回填
        if not info.description and info.titi_json:
            try:
                prof_desc = (info.titi_json.get("d2i_profile") or {}).get("description")
                if isinstance(prof_desc, str) and prof_desc.strip():
                    info.description = _sanitize_human_description(prof_desc)
            except Exception:
                pass

        # 2. 标题回退 (XPTitle)
        if not info.title and 'Exif.Image.XPTitle' in exif_data:
            try:
                val = exif_data['Exif.Image.XPTitle']
                info.title = _decode_exif_text(val)
                if _looks_garbled_question_marks(info.title):
                    info.title = ""
            except: pass

        # 3. 关键词回退 (XPKeywords)
        if not info.keywords and 'Exif.Image.XPKeywords' in exif_data:
            try:
                val = exif_data['Exif.Image.XPKeywords']
                decoded = _decode_exif_text(val)
                
                if decoded:
                    info.keywords = [k.strip() for k in re.split(r"[;,，、\n]+", decoded) if k.strip()]
            except: pass

    try:
        with pyexiv2.Image(filepath) as img:
            _extract_data(img)

    except Exception as e:
        # Unicode/长路径/网络盘 兼容：如果文件存在但 pyexiv2 打不开，则复制到临时 ASCII 路径再读
        err = str(e)
        try_temp = False
        try:
            if os.path.exists(filepath):
                if (
                    "Illegal byte sequence" in err
                    or "errno = 42" in err
                    or "errno = 2" in err
                    or "No such file or directory" in err
                    or "Failed to open the data source" in err
                ):
                    try_temp = True
        except Exception:
            try_temp = False

        if try_temp:
            import tempfile
            import shutil

            fd, tmp_path = tempfile.mkstemp(suffix=os.path.splitext(filepath)[1])
            os.close(fd)
            try:
                shutil.copy2(filepath, tmp_path)
                with pyexiv2.Image(tmp_path) as img:
                    _extract_data(img)
                return
            finally:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass

        # 其他错误抛出
        raise e


def _read_with_piexif(filepath: str, info: ImageMetadataInfo):
    """使用 piexif 读取元数据（回退方案）"""
    try:
        img = Image.open(filepath)
        exif_data = img.info.get('exif')
        
        if not exif_data:
            return
        
        exif_dict = piexif.load(exif_data)
        
        # XPComment: Windows 备注（UTF-16LE）优先
        xp_comment = exif_dict["0th"].get(piexif.ImageIFD.XPComment)
        if xp_comment:
            try:
                if isinstance(xp_comment, bytes):
                    info.description = _decode_binary_text(xp_comment)
                elif isinstance(xp_comment, (list, tuple)):
                    try:
                        nums = [int(x) for x in xp_comment]
                        info.description = _decode_binary_text(bytes(nums))
                    except Exception:
                        info.description = ""
                else:
                    s = str(xp_comment)
                    info.description = _decode_numeric_blob_text(s) or s
                info.description = _sanitize_human_description(info.description)
            except Exception:
                pass

        # 回退读取 ImageDescription（ASCII-only，可能只是短摘要/占位）
        if not info.description:
            desc = exif_dict["0th"].get(piexif.ImageIFD.ImageDescription)
            if desc:
                if isinstance(desc, bytes):
                    info.description = _sanitize_human_description(desc.decode('ascii', errors='ignore').replace("\x00", ""))
                else:
                    info.description = _sanitize_human_description(str(desc))

        # UserComment: 优先当作 TITI JSON（不当作简介）
        if not info.titi_json:
            try:
                user_comment = exif_dict.get("Exif", {}).get(piexif.ExifIFD.UserComment)
                if user_comment:
                    raw = user_comment
                    if isinstance(raw, (bytes, bytearray)):
                        data = bytes(raw)
                        if data.startswith(b"UNICODE\x00"):
                            payload = data[8:]
                            s = ""
                            for enc in ("utf-16le", "utf-16be"):
                                try:
                                    s = payload.decode(enc, errors="ignore").replace("\x00", "")
                                    break
                                except Exception:
                                    s = ""
                        elif data.startswith(b"ASCII\x00"):
                            s = data[8:].decode("ascii", errors="ignore").replace("\x00", "")
                        else:
                            s = data.decode("utf-8", errors="ignore").replace("\x00", "")
                    else:
                        raw_s = str(raw)
                        s = _decode_numeric_blob_text(raw_s) or raw_s

                    s = (s or "").strip().strip("\x00")
                    if s.startswith("{") and s.endswith("}"):
                        import json
                        parsed = json.loads(s)
                        if isinstance(parsed, dict) and ("titi_asset_id" in parsed or parsed.get("schema") == "titi-meta"):
                            info.titi_json = parsed
                            info.titi_asset_id = parsed.get("titi_asset_id", "")
                            info.titi_world_id = parsed.get("titi_world_id", "")
                            if not info.image_url:
                                info.image_url = _extract_image_url_from_titi_json(parsed)
                            gender = (parsed.get("d2i_profile") or {}).get("gender")
                            g = _normalize_gender_value(gender)
                            if g:
                                info.gender = g
                            if not info.person:
                                name = (parsed.get("d2i_profile") or {}).get("name")
                                if isinstance(name, str) and name.strip():
                                    info.person = name.strip()
            except Exception:
                pass

        if not info.description and info.titi_json:
            try:
                prof_desc = (info.titi_json.get("d2i_profile") or {}).get("description")
                if isinstance(prof_desc, str) and prof_desc.strip():
                    info.description = _sanitize_human_description(prof_desc)
            except Exception:
                pass

        if not info.image_url and info.titi_json:
            try:
                info.image_url = _extract_image_url_from_titi_json(info.titi_json)
            except Exception:
                pass

        info.other_exif = exif_dict
        
    except Exception as e:
        print(f"[警告] 读取 EXIF 失败 ({info.filename}): {e}")


def _determine_status(info: ImageMetadataInfo) -> MetadataStatus:
    """判断元数据状态"""
    has_title = bool(info.title)
    has_desc = bool(info.description)
    has_keywords = bool(info.keywords)
    
    if has_title and has_desc and has_keywords:
        return MetadataStatus.COMPLETE
    elif has_title or has_desc:
        return MetadataStatus.PARTIAL
    else:
        return MetadataStatus.NONE


def clean_text(text: str) -> str:
    """
    清洗文本格式
    
    - 去除多余空格（连续空格 → 单空格）
    - 去除首尾空白
    - 统一换行符（\r\n → \n）
    - 去除不可见控制字符
    """
    if not text:
        return text
    
    # 统一换行符
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    
    # 去除控制字符（保留换行和制表符）
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    
    # 连续空格 → 单空格
    text = re.sub(r'[ \t]+', ' ', text)
    
    # 连续换行 → 最多两个
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # 去除首尾空白
    text = text.strip()
    
    return text


def _normalize_keyword_token(value: Any) -> str:
    raw = clean_text(str(value or "")).strip().strip(",，、;；|/\\")
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered in KEYWORD_UNKNOWN_TOKENS or raw in KEYWORD_UNKNOWN_TOKENS:
        return ""
    if re.match(r"^https?://", raw, flags=re.IGNORECASE):
        return ""
    if re.search(r"[，。；;！？!?：:\n\r\t]", raw):
        return ""
    if re.search(r"\s", raw):
        return ""
    if len(raw) == 1 and raw not in KEYWORD_ALLOWED_SINGLE:
        return ""
    if len(raw) > KEYWORD_MAX_LENGTH:
        return ""
    if re.fullmatch(r"\d+", raw):
        return ""
    if re.fullmatch(r"\d{4}(?:[年/-]\d{1,2}(?:[月/-]\d{1,2})?)?", raw):
        return ""
    if re.fullmatch(r"\d{1,3}岁", raw):
        return ""
    return raw


def clean_keywords(keywords: List[str], max_count: int = KEYWORD_MAX_COUNT) -> List[str]:
    """清洗关键词列表（少而精，默认最多 6 个）"""
    if not isinstance(keywords, list):
        return []
    cleaned = []
    seen = set()

    for kw in keywords:
        kw = _normalize_keyword_token(kw)
        key = kw.casefold() if kw else ""
        if kw and key not in seen:
            seen.add(key)
            cleaned.append(kw)
            if len(cleaned) >= max(1, int(max_count or KEYWORD_MAX_COUNT)):
                break

    return cleaned


def update_metadata_preserve_others(
    filepath: str,
    new_metadata: dict,
    clean_format: bool = True
) -> bool:
    """
    更新元数据，保留其他字段（含 Unicode 路径兼容处理）
    """
    if not HAS_PYEXIV2:
        print("[错误] 未安装 pyexiv2，无法写入元数据")
        return False
    path = str(filepath or "").strip()
    if not path or (not os.path.isfile(path)):
        print(f"[错误] 文件不存在，无法写入元数据: {filepath}")
        return False
        
    def _apply_update(img):
        # 1. 读取现有 XMP
        xmp_data = img.read_xmp() or {}
        
        # 2. 清洗新数据
        title = new_metadata.get('title', '')
        desc = new_metadata.get('description', '')
        keywords = new_metadata.get('keywords', [])
        source = new_metadata.get('source', '')
        image_url = new_metadata.get('image_url', '') or new_metadata.get('url', '')
        city = new_metadata.get('city', '')
        person = new_metadata.get('person', '')
        gender = new_metadata.get('gender', '')
        position = new_metadata.get('position', '') # 新增：职务
        police_id = new_metadata.get('police_id', '')  # 新增：警号
        
        # 额外字段
        titi_asset_id = new_metadata.get('titi_asset_id', '')
        titi_world_id = new_metadata.get('titi_world_id', '')
        
        if clean_format:
            title = clean_text(title)
            desc = clean_text(desc)
            keywords = clean_keywords(keywords) if isinstance(keywords, list) else []
            source = clean_text(source)
            image_url = clean_text(image_url)
            city = clean_text(city)
            person = clean_text(person)
            gender = clean_text(gender)
            position = clean_text(position)
            police_id = clean_text(police_id)

        gender = _normalize_gender_value(gender)
        police_id = _normalize_police_id_value(police_id)
        
        # 3. 更新字段
        if title:
            xmp_data['Xmp.dc.title'] = {'lang="x-default"': title}

        if desc:
            xmp_data['Xmp.dc.description'] = {'lang="x-default"': desc}
            
        if keywords:
            xmp_data['Xmp.dc.subject'] = keywords
            
        if source:
            xmp_data['Xmp.dc.source'] = source
        if image_url:
            # 非标准字段，便于快速查看；机器消费仍以 Xmp.titi.meta 为准。
            xmp_data['Xmp.titi.sourceImage'] = image_url
            
        if city:
            xmp_data['Xmp.photoshop.City'] = city
            
        if person:
            xmp_data['Xmp.iptcExt.PersonInImage'] = [person]
            
        if position:
            xmp_data['Xmp.photoshop.AuthorsPosition'] = position
            xmp_data['Xmp.iptc.CreatorJobTitle'] = position

        # 4. 更新/合并 TITI 自定义元数据（尽量保留未知字段）
        import json
        import uuid

        old_titi = {}
        if 'Xmp.titi.meta' in xmp_data:
            try:
                old_titi = json.loads(xmp_data['Xmp.titi.meta'])
            except Exception:
                old_titi = {}
        if not isinstance(old_titi, dict):
            old_titi = {}

        # schema/app/component（仅补缺）
        if not old_titi.get("schema"):
            old_titi["schema"] = "titi-meta"
        if not isinstance(old_titi.get("schema_version"), int):
            old_titi["schema_version"] = 1
        if old_titi.get("app") in (None, "", "D2I"):
            old_titi["app"] = "PWI"
        if not old_titi.get("component"):
            old_titi["component"] = "forge"

        # ID / world
        asset_id = titi_asset_id or old_titi.get("titi_asset_id") or str(uuid.uuid4())
        old_titi["titi_asset_id"] = asset_id
        old_titi["titi_world_id"] = titi_world_id or old_titi.get("titi_world_id") or "default"

        # 可选：内容 hash
        if new_metadata.get("titi_content_hash"):
            old_titi["titi_content_hash"] = new_metadata["titi_content_hash"]

        if image_url:
            old_titi["source_image"] = image_url

        # d2i_profile 合并（只更新非空）
        profile = old_titi.get("d2i_profile")
        if not isinstance(profile, dict):
            profile = {}
        else:
            profile = dict(profile)

        if isinstance(new_metadata.get("d2i_profile"), dict):
            for k, v in new_metadata["d2i_profile"].items():
                if v not in (None, "", [], {}):
                    profile[k] = v

        if person:
            profile["name"] = person
        if desc:
            profile["description"] = desc
        if keywords:
            profile["keywords"] = keywords
        if source:
            profile["source"] = source
        if image_url:
            profile["image_url"] = image_url
        if city:
            profile["city"] = city
        if gender:
            profile["gender"] = gender
        elif "gender" in profile:
            existing_gender = _normalize_gender_value(profile.get("gender"))
            if existing_gender:
                profile["gender"] = existing_gender
            else:
                profile.pop("gender", None)

        if (not police_id) and isinstance(new_metadata.get("d2i_profile"), dict):
            police_id = _extract_police_id_from_profile(new_metadata.get("d2i_profile"))
        police_id = _normalize_police_id_value(police_id)
        if police_id:
            profile["police_id"] = police_id
        elif "police_id" in profile:
            existing_police_id = _normalize_police_id_value(profile.get("police_id"))
            if existing_police_id:
                profile["police_id"] = existing_police_id
            else:
                profile.pop("police_id", None)
        if profile:
            profile["extracted_at"] = datetime.utcnow().isoformat() + "Z"
            old_titi["d2i_profile"] = profile

        # 可选：角色别名
        if isinstance(new_metadata.get("role_aliases"), list) and new_metadata["role_aliases"]:
            old_titi["role_aliases"] = new_metadata["role_aliases"]

        titi_json_str = json.dumps(old_titi, ensure_ascii=False)
        xmp_data['Xmp.titi.meta'] = titi_json_str

        # 5. 写入 XMP
        img.modify_xmp(xmp_data)

        # 6. Windows 兼容：写入 EXIF（XPComment/UserComment）
        new_exif = {
            "Exif.Image.Software": "PWI Forge",
            # 0x010E (ImageDescription) 统一保持空，避免第三方软件把它当主描述并显示乱码问号。
            "Exif.Image.ImageDescription": "",
            "Exif.Photo.UserComment": titi_json_str,
        }
        if desc:
            new_exif["Exif.Image.XPComment"] = desc
        img.modify_exif(new_exif)

    # 安全写入：始终在临时副本上操作，写入成功且像素校验通过后再覆盖原图。
    original_fp = _pixel_fingerprint(path)
    fd, tmp_path = tempfile.mkstemp(suffix=os.path.splitext(path)[1] or ".tmp")
    os.close(fd)
    try:
        shutil.copy2(path, tmp_path)
        with pyexiv2.Image(tmp_path) as img:
            _apply_update(img)
        return _safe_replace_with_pixel_guard(
            path,
            tmp_path,
            original_fp=original_fp,
            op_label="update_metadata",
        )
    except Exception as e:
        print(f"[错误] 写入元数据失败 ({path}): {e}")
        return False
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def rebuild_from_description(info: ImageMetadataInfo) -> dict:
    """
    从现有描述重新提取元数据
    
    Args:
        info: 图片元数据信息
    
    Returns:
        提取的元数据字典
    """
    if not info.description:
        return {}
    
    # 构建 item 用于提取
    item = {
        'name': info.title or os.path.splitext(info.filename)[0],
        'intro': info.description,
        'url': info.image_url or info.source or '',
        'source': info.source or ''
    }
    
    return build_metadata_from_item(item)


def match_filename_to_table(
    filename: str,
    table_data: List[dict],
    name_key: str = 'name'
) -> Optional[dict]:
    """
    通过文件名匹配表格行
    
    Args:
        filename: 文件名（不含路径）
        table_data: 表格数据列表
        name_key: 姓名字段的键名
    
    Returns:
        匹配的行，如未匹配返回 None
    """
    # 去除扩展名
    base_name = os.path.splitext(filename)[0]
    
    # 去除后缀数字（如 张三_2 → 张三）
    base_name = re.sub(r'[_\-]\d+$', '', base_name)
    
    # 精确匹配
    for row in table_data:
        if row.get(name_key) == base_name:
            return row
    
    return None


def export_report(
    images: List[ImageMetadataInfo],
    output_path: str,
    format: str = 'csv'
) -> bool:
    """
    导出元数据报告
    
    Args:
        images: 图片信息列表
        output_path: 输出路径
        format: 格式 ('csv' 或 'xlsx')
    
    Returns:
        是否成功
    """
    try:
        if format == 'csv':
            import csv
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                # 表头
                writer.writerow([
                    '文件名', '标题', '描述', '关键词', '关键词数',
                    '来源页面', '原图链接', '城市', '人物', '警号', '状态', '修改时间'
                ])
                # 数据
                for img in images:
                    writer.writerow([
                        img.filename,
                        img.title,
                        img.description[:100] + '...' if len(img.description) > 100 else img.description,
                        '|'.join(img.keywords),
                        img.keywords_count,
                        img.source,
                        img.image_url,
                        img.city,
                        img.person,
                        img.police_id,
                        img.status_text,
                        img.modified_time.strftime('%Y-%m-%d %H:%M')
                    ])
            return True
        
        elif format == 'xlsx':
            from openpyxl import Workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "元数据报告"
            
            # 表头
            headers = ['文件名', '标题', '描述', '关键词', '关键词数',
                      '来源页面', '原图链接', '城市', '人物', '警号', '状态', '修改时间']
            ws.append(headers)
            
            # 数据
            for img in images:
                ws.append([
                    img.filename,
                    img.title,
                    img.description[:100] + '...' if len(img.description) > 100 else img.description,
                    '|'.join(img.keywords),
                    img.keywords_count,
                    img.source,
                    img.image_url,
                    img.city,
                    img.person,
                    img.police_id,
                    img.status_text,
                    img.modified_time.strftime('%Y-%m-%d %H:%M')
                ])
            
            wb.save(output_path)
            return True
        
    except Exception as e:
        print(f"[错误] 导出报告失败: {e}")
        return False


# ========== 筛选器 ==========

def filter_by_status(images: List[ImageMetadataInfo], status: MetadataStatus) -> List[ImageMetadataInfo]:
    """按状态筛选"""
    return [img for img in images if img.status == status]


def filter_by_keyword_search(images: List[ImageMetadataInfo], keyword: str) -> List[ImageMetadataInfo]:
    """按文件名关键词搜索"""
    if not keyword:
        return images
    keyword = keyword.lower()
    return [img for img in images if keyword in img.filename.lower()]


def filter_by_time_range(
    images: List[ImageMetadataInfo],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None
) -> List[ImageMetadataInfo]:
    """按时间范围筛选"""
    result = images
    
    if start:
        result = [img for img in result if img.modified_time >= start]
    if end:
        result = [img for img in result if img.modified_time <= end]
    
    return result


def filter_missing_keywords(images: List[ImageMetadataInfo]) -> List[ImageMetadataInfo]:
    """筛选有描述但缺少关键词的"""
    return [img for img in images if img.description and not img.keywords]


def fix_image_metadata(file_path: str) -> bool:
    """
    修复单个图片的元数据（应用最新标准，如 Title 格式、AuthorsPosition 等）
    这会从现有的 TITI JSON 或描述中重新生成元数据并覆盖旧的 XMP。
    """
    try:
        # 1. 读取现有数据
        info = read_image_metadata(file_path)
        if not info: return False
        
        # 名字优先级：Person > Title > 文件名
        name = info.person or info.title
        if isinstance(name, str) and " - " in name:
            name = name.split(" - ", 1)[0].strip()
        if not name and info.filename:
            # 去除扩展名和可能的后缀数字 (张三_1.jpg)
            base = os.path.splitext(info.filename)[0]
            name = re.sub(r'[_\-]\d+$', '', base)
            
        intro = info.description
        source = info.source
        image_url = info.image_url
        
        # 优先从 TITI JSON 获取最原始信息
        if info.titi_json and 'd2i_profile' in info.titi_json:
            profile = info.titi_json['d2i_profile']
            profile_name = profile.get('name')
            if isinstance(profile_name, str) and " - " in profile_name:
                profile_name = profile_name.split(" - ", 1)[0].strip()
            name = profile_name or name
            intro = profile.get('description') or intro
            source = profile.get('source') or source
            image_url = profile.get('image_url') or image_url
        image_url = _extract_image_url_from_titi_json(info.titi_json) or image_url
        
        if not intro:
            print(f"[跳过] 无描述信息: {info.filename}")
            return False # 没有描述，无法做增强修复
            
        # 2. 构造虚拟 item，复用 text_parser 的最新逻辑
        # 这样能确保 Title 格式、职业分类、职务提取等都应用最新的规则
        item = {
            'name': name,
            'intro': intro,
            'url': image_url or '',  # 原图链接
            'image_url': image_url or '',
            'source': source
        }
        
        from text_parser import build_metadata_from_item
        new_metadata = build_metadata_from_item(item)
        
        # 3. 补充 ID 信息（保持 ID 不变）
        if info.titi_asset_id:
            new_metadata['titi_asset_id'] = info.titi_asset_id
        
        # 4. 写入（使用 preserve_others 避免丢数据）
        # 注意：这会覆盖旧的 Title, Keywords, Description 等
        update_metadata_preserve_others(file_path, new_metadata)
        return True
        
    except Exception as e:
        print(f"[错误] 修复失败 {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_metadata_health(directory: str, callback=None) -> dict:
    """
    检查目录下的元数据健康状况
    
    Returns:
        dict: {
            'total': int,
            'missing_position': int, # 缺少职务字段
            'old_title': int,        # 标题格式旧 (纯姓名)
            'no_metadata': int,      # 无元数据
            'good': int              # 完好
        }
    """
    if not os.path.exists(directory):
        return {}
        
    files = [f for f in os.listdir(directory) if f.lower().endswith(('.jpg', '.jpeg'))]
    report = {
        'total': len(files),
        'missing_position': 0,
        'old_title': 0,
        'no_metadata': 0,
        'good': 0
    }
    
    for i, f in enumerate(files):
        path = os.path.join(directory, f)
        info = read_image_metadata(path)
        
        has_issue = False
        
        # 0. 无元数据
        # 只要有 title, description, 或 titi_json 中的任何一个，就算有数据
        has_basic_meta = bool(info.description or info.title)
        has_titi_meta = bool(info.titi_json or info.titi_asset_id)
        
        if not has_basic_meta and not has_titi_meta:
            report['no_metadata'] += 1
            has_issue = True
        else:
            # 1. 检查职务
            # 如果简介里有"律师"、"民警"等词但 position 为空
            # 或者有 TITI 数据但没有基本数据（这种情况也属于missing position/title context）
            if (info.description and not info.position) or (has_titi_meta and not info.position):
                report['missing_position'] += 1
                has_issue = True
                
            # 2. 检查标题格式 (Name - Intro)
            current_title = info.title or ""
            if (info.description or has_titi_meta) and " - " not in current_title:
                report['old_title'] += 1
                has_issue = True
        
        if not has_issue:
            report['good'] += 1
            
        if callback:
            callback(i + 1, len(files), f)
            
    return report


def batch_fix_metadata_in_dir(directory: str, callback=None):
    """
    批量修复目录下图片的元数据
    
    Args:
        directory: 目录路径
        callback: 进度回调 (current, total, filename)
    
    Returns:
        (success_count, total_count)
    """
    if not os.path.exists(directory):
        return 0, 0
        
    files = [f for f in os.listdir(directory) if f.lower().endswith(('.jpg', '.jpeg'))]
    total = len(files)
    success = 0
    
    for i, f in enumerate(files):
        path = os.path.join(directory, f)
        if fix_image_metadata(path):
            success += 1
        
        if callback:
            callback(i + 1, total, f)
            
    return success, total
