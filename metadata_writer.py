# -*- coding: utf-8 -*-
"""
图片元数据写入模块 - v3.0 (TITI 兼容版)

实现双轨写入策略：
A层 - 人类可读（用于展示/检索）：
  - Xmp.dc.title/description/subject/source
  - Xmp.photoshop.City
  - Xmp.iptcExt.PersonInImage
  - EXIF XPComment（Windows 兼容）

B层 - 机器可读（TITI 身份/溯源）：
  - Xmp.titi.meta（JSON）
  - EXIF UserComment（JSON 回退）

参考规范：XMP_METADATA_SPEC.md
"""

import os
import json
import tempfile
import shutil
import uuid as uuid_module
from datetime import datetime, timezone
from PIL import Image
import piexif
from metadata_manager import update_metadata_preserve_others

# 尝试导入 pyexiv2
try:
    import pyexiv2
    HAS_PYEXIV2 = True
    # 注册 TITI 命名空间
    try:
        pyexiv2.registerNs('urn:titi:ns:1.0', 'titi')
    except Exception:
        pass  # 可能已注册
except ImportError:
    HAS_PYEXIV2 = False
    print("[警告] pyexiv2 未安装，将使用旧版 EXIF 方式写入元数据")


def generate_titi_asset_id():
    """生成新的 TITI 资产 ID (UUID v4)"""
    return str(uuid_module.uuid4())


def _decode_exif_text(val) -> str:
    """将常见的 EXIF/XMP 值尽可能转成 str"""
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    if isinstance(val, bytes):
        if val.startswith(b"UNICODE\x00"):
            payload = val[8:]
            for enc in ("utf-16le", "utf-16be"):
                try:
                    return payload.decode(enc, errors="ignore").replace("\x00", "")
                except Exception:
                    pass
        if val.startswith(b"ASCII\x00"):
            try:
                return val[8:].decode("ascii", errors="ignore").replace("\x00", "")
            except Exception:
                return ""
        for enc in ("utf-8", "utf-16le", "utf-16be", "latin1"):
            try:
                return val.decode(enc, errors="ignore").replace("\x00", "")
            except Exception:
                continue
        return ""
    if isinstance(val, (list, tuple)):
        try:
            return bytes(val).decode("utf-16le", errors="ignore").replace("\x00", "")
        except Exception:
            return ""
    return str(val)


def _try_parse_titi_json(text: str):
    if not text:
        return None
    s = str(text).strip().strip("\x00")
    if not s:
        return None
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict) and ("titi_asset_id" in parsed or parsed.get("schema") == "titi-meta"):
            return parsed
    except Exception:
        return None
    return None


def _extract_image_url_from_titi_json(titi_json):
    if not isinstance(titi_json, dict):
        return ""

    direct = str(titi_json.get("source_image") or titi_json.get("image_url") or "").strip()
    if direct:
        return direct

    source_images = titi_json.get("source_images")
    if isinstance(source_images, list):
        for val in source_images:
            s = str(val or "").strip()
            if s:
                return s

    source_inputs = titi_json.get("source_inputs")
    if isinstance(source_inputs, list):
        for item in source_inputs:
            if not isinstance(item, dict):
                continue
            for key in ("source_image", "url", "image_url", "filename", "path"):
                s = str(item.get(key) or "").strip()
                if s:
                    return s

    profile = titi_json.get("d2i_profile")
    if isinstance(profile, dict):
        s = str(profile.get("image_url") or profile.get("url") or "").strip()
        if s:
            return s
    return ""


def build_titi_json(metadata, existing_json=None, existing_asset_id=None):
    """
    构建 titi:meta JSON（支持 merge，尽量保留未知字段）

    Args:
        metadata: Forge(D2I) 侧元数据字典
        existing_json: 已存在的 titi:meta JSON(dict)，用于合并（可选）
        existing_asset_id: 兼容旧调用方式的参数（可选）

    Returns:
        dict: 合并后的 titi:meta JSON
    """
    import copy

    now = datetime.now(timezone.utc).isoformat()
    base = copy.deepcopy(existing_json) if isinstance(existing_json, dict) else {}

    # schema 标识（用于兼容演进）
    if not base.get("schema"):
        base["schema"] = "titi-meta"
    if not isinstance(base.get("schema_version"), int):
        base["schema_version"] = 1

    # 生态标识
    if base.get("app") in (None, "", "D2I"):
        base["app"] = "PWI"
    if not base.get("component"):
        base["component"] = "forge"

    # ID 字段
    asset_id = metadata.get("titi_asset_id") or existing_asset_id or base.get("titi_asset_id") or generate_titi_asset_id()
    base["titi_asset_id"] = asset_id
    base["titi_world_id"] = metadata.get("titi_world_id") or base.get("titi_world_id") or "default"

    # 可选：内容 hash
    if metadata.get("titi_content_hash"):
        base["titi_content_hash"] = metadata["titi_content_hash"]

    # D2I/Forge 扩展信息（只更新非空字段）
    profile = base.get("d2i_profile")
    if not isinstance(profile, dict):
        profile = {}
    else:
        profile = dict(profile)

    # 姓名优先来自 PersonInImage / metadata.person；Title 仅作兜底（兼容旧数据）
    person_name = metadata.get("person") or metadata.get("name")
    if not person_name:
        person_name = metadata.get("title", "")
        if isinstance(person_name, str) and " - " in person_name:
            person_name = person_name.split(" - ", 1)[0].strip()

    if isinstance(person_name, str) and person_name.strip():
        profile["name"] = person_name.strip()

    desc = metadata.get("description")
    if isinstance(desc, str) and desc.strip():
        profile["description"] = desc

    keywords = metadata.get("keywords")
    if isinstance(keywords, list) and keywords:
        profile["keywords"] = keywords

    source = metadata.get("source")
    if isinstance(source, str) and source.strip():
        profile["source"] = source

    image_url = metadata.get("image_url") or metadata.get("url")
    if isinstance(image_url, str) and image_url.strip():
        image_url = image_url.strip()
        profile["image_url"] = image_url
        base["source_image"] = image_url

    city = metadata.get("city")
    if isinstance(city, str) and city.strip():
        profile["city"] = city

    def _normalize_gender(val):
        if val is None:
            return None
        if not isinstance(val, str):
            val = str(val)
        s = val.strip().lower()
        if not s:
            return None
        mapping = {
            "男": "男",
            "male": "男",
            "m": "男",
            "man": "男",
            "男性": "男",
            "女": "女",
            "female": "女",
            "f": "女",
            "woman": "女",
            "女性": "女",
        }
        return mapping.get(s) or mapping.get(val.strip())  # 兼容中文大小写/全角等

    gender = metadata.get("gender")
    if gender is None and isinstance(metadata.get("extracted"), dict):
        gender = metadata["extracted"].get("gender")
    if gender is None and isinstance(metadata.get("d2i_profile"), dict):
        gender = metadata["d2i_profile"].get("gender")
    gender = _normalize_gender(gender)
    if gender:
        profile["gender"] = gender

    if profile:
        profile["extracted_at"] = now
        base["d2i_profile"] = profile

    # 可选：角色别名（如上游传入则写入；否则保留已有）
    role_aliases = metadata.get("role_aliases")
    if isinstance(role_aliases, list) and role_aliases:
        base["role_aliases"] = role_aliases

    return base


def write_xmp_metadata(image_path, metadata):
    """
    写入 XMP 元数据（双轨写入：A层人类可读 + B层 TITI 身份）
    
    Args:
        image_path: 图片文件路径
        metadata: {
            'title': str,        # 姓名
            'description': str,  # 完整简介
            'keywords': list,    # 关键词列表
            'source': str,       # 来源URL
            'city': str,         # 籍贯城市
            'person': str,       # 人物姓名
            'titi_asset_id': str,  # 可选，已有的资产ID
            'titi_world_id': str,  # 可选，世界ID
        }
    
    Returns:
        str: 保存后的图片路径
    """
    if not metadata:
        return image_path

    # 安全优先：统一走 metadata_manager 的安全写入（临时副本 + 像素校验 + 回滚）。
    safe_path = _ensure_jpeg(image_path)
    safe_payload = dict(metadata or {})
    if (not safe_payload.get("person")) and safe_payload.get("name"):
        safe_payload["person"] = safe_payload.get("name")

    ok = bool(update_metadata_preserve_others(safe_path, safe_payload, clean_format=True))
    if not ok:
        raise RuntimeError(f"安全写入失败: {safe_path}")
    return safe_path
    
    # 如果 pyexiv2 不可用，退回到旧版方式
    if not HAS_PYEXIV2:
        description = metadata.get('description', '')
        return write_description_legacy(image_path, description)
    
    try:
        # 先确保图片是 JPEG 格式
        image_path = _ensure_jpeg(image_path)
        
        # pyexiv2 在 Windows 上不支持中文路径
        # 解决方案：复制到临时目录（纯ASCII路径），写入元数据后再移回
        temp_dir = tempfile.gettempdir()
        temp_filename = f"xmp_{uuid_module.uuid4().hex}.jpg"
        temp_path = os.path.join(temp_dir, temp_filename)
        
        # 复制到临时位置
        shutil.copy2(image_path, temp_path)
        
        try:
            # 读取现有的 TITI JSON（如果有，用于合并）
            existing_titi_json = None
            try:
                with pyexiv2.Image(temp_path) as img:
                    xmp_data_existing = img.read_xmp()
                    if 'Xmp.titi.meta' in xmp_data_existing:
                        existing_titi_json = json.loads(xmp_data_existing['Xmp.titi.meta'])
            except Exception:
                pass
            
            # 构建/合并 TITI JSON（保留未知字段，避免“洗掉”Titi provenance）
            titi_json = build_titi_json(metadata, existing_json=existing_titi_json)
            titi_json_str = json.dumps(titi_json, ensure_ascii=False)
            
            # 使用 pyexiv2 写入元数据
            with pyexiv2.Image(temp_path) as img:
                xmp_data = img.read_xmp() or {}
                
                # ===== A层：人类可读 =====
                
                # 标题
                if metadata.get('title'):
                    xmp_data['Xmp.dc.title'] = {'lang="x-default"': metadata['title']}
                
                # 描述
                if metadata.get('description'):
                    xmp_data['Xmp.dc.description'] = {'lang="x-default"': metadata['description']}
                
                # 关键词/标签
                if metadata.get('keywords'):
                    xmp_data['Xmp.dc.subject'] = metadata['keywords']
                
                # 来源URL
                if metadata.get('source'):
                    xmp_data['Xmp.dc.source'] = metadata['source']
                
                # 城市
                if metadata.get('city'):
                    xmp_data['Xmp.photoshop.City'] = metadata['city']
                
                # 人物姓名（IPTC Extension）
                if metadata.get('person'):
                    xmp_data['Xmp.iptcExt.PersonInImage'] = [metadata['person']]

                # 职务 (Job Title)
                if metadata.get('position'):
                    xmp_data['Xmp.photoshop.AuthorsPosition'] = metadata['position']
                    xmp_data['Xmp.iptc.CreatorJobTitle'] = metadata['position']
                
                # ===== B层：TITI 身份 =====
                
                # TITI JSON
                xmp_data['Xmp.titi.meta'] = titi_json_str
                
                # 写入 XMP
                if xmp_data:
                    img.modify_xmp(xmp_data)
                
                # ===== EXIF 兼容字段 =====
                
                exif_data = img.read_exif()
                new_exif = {}
                
                # XPComment: 描述（Windows 兼容）
                if metadata.get('description'):
                    # pyexiv2 会自动处理编码
                    new_exif['Exif.Image.XPComment'] = metadata['description']
                
                # ImageDescription(0x010E)：统一保持空，避免第三方软件错误展示为主描述。
                new_exif['Exif.Image.ImageDescription'] = ""
                
                # Software: 标识
                new_exif['Exif.Image.Software'] = 'PWI Forge'
                
                # UserComment: TITI JSON（B层回退）
                # 格式：UNICODE\0 + UTF-16LE
                # pyexiv2 会自动处理
                new_exif['Exif.Photo.UserComment'] = titi_json_str
                
                if new_exif:
                    img.modify_exif(new_exif)
            
            # 元数据写入成功，移回原位置
            if os.path.exists(image_path):
                os.remove(image_path)
            shutil.move(temp_path, image_path)
            return image_path
            
        finally:
            # 清理临时文件（如果还存在）
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
        
    except Exception as e:
        # 如果 XMP 写入失败，退回到旧版
        print(f"[警告] XMP 写入失败 ({str(e)})，使用旧版方式")
        description = metadata.get('description', '')
        return write_description_legacy(image_path, description)


def _ensure_jpeg(image_path):
    """
    确保图片是 JPEG 格式
    
    Args:
        image_path: 原始图片路径
    
    Returns:
        str: JPEG 图片路径
    """
    base, ext = os.path.splitext(image_path)
    
    # 处理临时文件扩展名 (.tmp)
    if ext.lower() == '.tmp':
        real_base, real_ext = os.path.splitext(base)
        if real_ext.lower() in ('.jpg', '.jpeg'):
            new_path = base
            try:
                if os.path.exists(image_path):
                    if os.path.exists(new_path):
                        os.remove(new_path)
                    os.rename(image_path, new_path)
                return new_path
            except Exception as e:
                print(f"[警告] 重命名临时文件失败: {e}")
                return image_path
        else:
            new_path = real_base + '.jpg'
    elif ext.lower() in ('.jpg', '.jpeg'):
        return image_path
    else:
        new_path = base + '.jpg'
    
    try:
        img = Image.open(image_path)
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        
        if os.path.exists(new_path):
            os.remove(new_path)
        
        img.save(new_path, 'JPEG', quality=95)
        
        if os.path.exists(image_path):
            os.remove(image_path)
        
        return new_path
    except Exception as e:
        print(f"[警告] 转换图片格式失败: {e}")
        return image_path


def read_xmp_metadata(image_path):
    """
    读取图片的 XMP 元数据
    
    Args:
        image_path: 图片文件路径
    
    Returns:
        dict: 元数据字典（包含 A层 和 B层 信息）
    """
    if not HAS_PYEXIV2:
        desc = read_description(image_path)
        return {'description': desc} if desc else {}
    
    try:
        with pyexiv2.Image(image_path) as img:
            xmp_data = img.read_xmp()
            
            result = {}
            
            # ===== A层：标准字段 =====
            
            # 标题
            if 'Xmp.dc.title' in xmp_data:
                title = xmp_data['Xmp.dc.title']
                if isinstance(title, dict):
                    result['title'] = list(title.values())[0] if title else ''
                else:
                    result['title'] = str(title)
            
            # 描述
            if 'Xmp.dc.description' in xmp_data:
                desc = xmp_data['Xmp.dc.description']
                if isinstance(desc, dict):
                    result['description'] = list(desc.values())[0] if desc else ''
                else:
                    result['description'] = str(desc)
            
            # 关键词
            if 'Xmp.dc.subject' in xmp_data:
                result['keywords'] = xmp_data['Xmp.dc.subject']
            
            # 来源
            if 'Xmp.dc.source' in xmp_data:
                result['source'] = xmp_data['Xmp.dc.source']

            # 原图链接（自定义字段）
            if 'Xmp.titi.sourceImage' in xmp_data:
                result['image_url'] = str(xmp_data['Xmp.titi.sourceImage'] or '').strip()
            elif 'Xmp.titi.imageUrl' in xmp_data:
                result['image_url'] = str(xmp_data['Xmp.titi.imageUrl'] or '').strip()
            
            # 城市
            if 'Xmp.photoshop.City' in xmp_data:
                result['city'] = xmp_data['Xmp.photoshop.City']
            
            # 人物
            if 'Xmp.iptcExt.PersonInImage' in xmp_data:
                persons = xmp_data['Xmp.iptcExt.PersonInImage']
                result['person'] = persons[0] if persons else ''

            # 职务
            if 'Xmp.photoshop.AuthorsPosition' in xmp_data:
                result['position'] = xmp_data['Xmp.photoshop.AuthorsPosition']
            elif 'Xmp.iptc.CreatorJobTitle' in xmp_data:
                result['position'] = xmp_data['Xmp.iptc.CreatorJobTitle']
            
            # ===== B层：TITI 身份 =====
            
            # 优先 XMP titi:meta
            if 'Xmp.titi.meta' in xmp_data:
                try:
                    titi_json = json.loads(xmp_data['Xmp.titi.meta'])
                    if isinstance(titi_json, dict):
                        result['titi_asset_id'] = titi_json.get('titi_asset_id')
                        result['titi_world_id'] = titi_json.get('titi_world_id')
                        result['titi_json'] = titi_json
                        if not result.get("image_url"):
                            result["image_url"] = _extract_image_url_from_titi_json(titi_json)
                        gender = (titi_json.get("d2i_profile") or {}).get("gender")
                        if isinstance(gender, str) and gender.strip():
                            result["gender"] = gender.strip()
                except Exception:
                    pass

            # 回退：PNG text titi
            if not result.get("titi_json") and image_path.lower().endswith(".png"):
                try:
                    with Image.open(image_path) as im:
                        raw = None
                        if isinstance(getattr(im, "info", None), dict):
                            raw = im.info.get("titi") or im.info.get("TITI")
                        parsed = _try_parse_titi_json(raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else raw)
                        if parsed:
                            result["titi_json"] = parsed
                            result["titi_asset_id"] = parsed.get("titi_asset_id")
                            result["titi_world_id"] = parsed.get("titi_world_id")
                            if not result.get("image_url"):
                                result["image_url"] = _extract_image_url_from_titi_json(parsed)
                            gender = (parsed.get("d2i_profile") or {}).get("gender")
                            if isinstance(gender, str) and gender.strip():
                                result["gender"] = gender.strip()
                except Exception:
                    pass

            # 回退：EXIF UserComment titi JSON
            if not result.get("titi_json"):
                try:
                    exif_data = img.read_exif()
                    parsed = _try_parse_titi_json(_decode_exif_text(exif_data.get("Exif.Photo.UserComment")))
                    if parsed:
                        result["titi_json"] = parsed
                        result["titi_asset_id"] = parsed.get("titi_asset_id")
                        result["titi_world_id"] = parsed.get("titi_world_id")
                        if not result.get("image_url"):
                            result["image_url"] = _extract_image_url_from_titi_json(parsed)
                        gender = (parsed.get("d2i_profile") or {}).get("gender")
                        if isinstance(gender, str) and gender.strip():
                            result["gender"] = gender.strip()
                except Exception:
                    pass
            
            return result
            
    except Exception:
        return {}


def read_titi_asset_id(image_path):
    """
    读取图片的 TITI 资产 ID
    优先级：XMP titi:meta > EXIF UserComment
    
    Args:
        image_path: 图片文件路径
    
    Returns:
        str: 资产 ID，如果没有则返回 None
    """
    if not HAS_PYEXIV2:
        return None
    
    try:
        with pyexiv2.Image(image_path) as img:
            # 优先从 XMP 读取
            xmp_data = img.read_xmp()
            if 'Xmp.titi.meta' in xmp_data:
                try:
                    titi_json = json.loads(xmp_data['Xmp.titi.meta'])
                    if 'titi_asset_id' in titi_json:
                        return titi_json['titi_asset_id']
                except Exception:
                    pass
            
            # 回退到 PNG text
            if image_path.lower().endswith(".png"):
                try:
                    with Image.open(image_path) as im:
                        raw = None
                        if isinstance(getattr(im, "info", None), dict):
                            raw = im.info.get("titi") or im.info.get("TITI")
                        parsed = _try_parse_titi_json(raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else raw)
                        if parsed and parsed.get("titi_asset_id"):
                            return parsed["titi_asset_id"]
                except Exception:
                    pass

            # 回退到 EXIF UserComment
            exif_data = img.read_exif()
            parsed = _try_parse_titi_json(_decode_exif_text(exif_data.get("Exif.Photo.UserComment")))
            if parsed and parsed.get("titi_asset_id"):
                return parsed["titi_asset_id"]
            
            return None
            
    except Exception:
        return None


# ========== 旧版 EXIF 兼容函数 ==========

def write_description_legacy(image_path, description):
    """
    [旧版兼容] 将描述信息写入图片的 EXIF 元数据
    注意：此函数仅作为回退，不写入 TITI JSON
    
    Args:
        image_path: 图片文件路径
        description: 要写入的描述文本
    
    Returns:
        str: 保存后的图片路径
    """
    if not description:
        return image_path
    
    try:
        img = Image.open(image_path)
        
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        
        try:
            exif_dict = piexif.load(img.info.get('exif', b''))
        except Exception:
            exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}
        
        # ImageDescription(0x010E) 不再承载描述；统一清空避免第三方软件误读。
        if piexif.ImageIFD.ImageDescription in exif_dict["0th"]:
            try:
                del exif_dict["0th"][piexif.ImageIFD.ImageDescription]
            except Exception:
                exif_dict["0th"][piexif.ImageIFD.ImageDescription] = b""
        
        # XPComment: 完整描述（Windows 兼容）
        # XPComment 使用 UTF-16LE 编码
        xp_comment = description.encode('utf-16le') + b'\x00\x00'
        exif_dict["0th"][piexif.ImageIFD.XPComment] = xp_comment
        
        # Software
        exif_dict["0th"][piexif.ImageIFD.Software] = b'PWI Forge'
        
        # 生成 EXIF 字节
        exif_bytes = piexif.dump(exif_dict)
        
        # 保存
        base, ext = os.path.splitext(image_path)
        
        # 如果是临时文件，处理真正的扩展名
        if ext.lower() == '.tmp':
            base, ext = os.path.splitext(base)
            
        if ext.lower() not in ('.jpg', '.jpeg'):
            new_path = base + '.jpg'
        else:
            new_path = base + ext
        
        img.save(new_path, 'JPEG', exif=exif_bytes, quality=95)
        
        if new_path != image_path and os.path.exists(image_path):
            os.remove(image_path)
        
        return new_path
        
    except Exception as e:
        raise Exception(f"写入元数据失败: {str(e)}")


# 保持向后兼容
def write_description(image_path, description):
    """向后兼容的别名"""
    return write_description_legacy(image_path, description)


def read_description(image_path):
    """
    读取图片的描述元数据
    
    Args:
        image_path: 图片文件路径
    
    Returns:
        str: 描述文本
    """
    try:
        img = Image.open(image_path)
        exif_data = img.info.get('exif')
        
        if not exif_data:
            return ''
        
        exif_dict = piexif.load(exif_data)
        
        # 优先读取 XPComment（Windows 备注，UTF-16LE）
        xp_comment = exif_dict["0th"].get(piexif.ImageIFD.XPComment)
        if xp_comment:
            try:
                if isinstance(xp_comment, bytes):
                    return xp_comment.decode("utf-16le", errors="ignore").replace("\x00", "")
                if isinstance(xp_comment, (list, tuple)):
                    return bytes(xp_comment).decode("utf-16le", errors="ignore").replace("\x00", "")
                return str(xp_comment)
            except Exception:
                pass

        # 回退读取 ImageDescription（ASCII）
        desc = exif_dict["0th"].get(piexif.ImageIFD.ImageDescription)
        if desc:
            if isinstance(desc, bytes):
                return desc.decode("ascii", errors="ignore").replace("\x00", "")
            return str(desc)
        
        return ''
        
    except Exception:
        return ''
