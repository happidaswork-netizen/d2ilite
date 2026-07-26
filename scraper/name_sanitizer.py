"""Shared person-name sanitization (single source of truth).

Extracted verbatim from the strict requests_jsl implementation in
``public_profile_spider.py`` so that the spider, the browser-mode runner
(``run_public_scraper.py``) and ``llm_enricher.py`` all apply the same gate:
navigation/org chrome text and bare titles are rejected, and there is NO
fallback to raw page text when no usable person name can be extracted.

Standard library only — must stay importable inside the Scrapy runtime and
from the repo root (``scraper.name_sanitizer``) alike.
"""

import re
from typing import Any, Optional


def _strip_html_markup(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    if "<" in text and ">" in text:
        text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
        text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = (
            text.replace("&nbsp;", " ")
            .replace("&#160;", " ")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#39;", "'")
        )
    return text


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = " ".join(_strip_html_markup(value).split()).strip()
    if not text:
        return ""
    if re.search(r"[\u4e00-\u9fff]", text):
        return text
    latin1_like = sum(1 for ch in text if 0x80 <= ord(ch) <= 0xFF)
    if latin1_like < 2:
        return text
    try:
        repaired = text.encode("latin1").decode("utf-8")
    except Exception:
        return text
    if re.search(r"[\u4e00-\u9fff]", repaired):
        return repaired
    return text


_NAV_OR_ORG_NAME_RE = re.compile(
    r"(?:"
    r"\u4e2a\u4eba\u7b80\u5386|\u5e02\u653f\u5e9c\u9886\u5bfc|\u4eba\u6c11\u653f\u5e9c|"
    r"\u9996\u9875|\u7f51\u7ad9\u5730\u56fe|\u5173\u4e8e\u672c\u7ad9|"
    r"\u8981\u95fb\u52a8\u6001|\u653f\u6c11\u4e92\u52a8|\u9b45\u529b\u72ee\u57ce|\u653f\u52a1\u516c\u5f00|"
    r"\u4fe1\u606f\u516c\u5f00|\u8fd4\u56de\u9876\u90e8|\u4e8c\u7ef4\u7801|\u65e0\u969c\u788d|"
    r"\u65b0\u95fb|\u901a\u77e5\u516c\u544a|\u6587\u4ef6\u516c\u5f00|\u653f\u7b56\u89e3\u8bfb"
    r")"
)
_ORG_FRAGMENT_NAME_RE = re.compile(
    r"(?:"
    r"\u5e02\u4eba$|\u653f\u5e9c$|\u9886\u5bfc$|\u7b80\u5386$|\u52a8\u6001$|\u4e92\u52a8$|"
    r"\u516c\u5f00$|\u90e8\u95e8$|\u59d4\u5458\u4f1a$|\u529e\u516c\u5ba4$"
    r")"
)
_BARE_TITLE_NAME_RE = re.compile(
    r"^(?:"
    r"\u4ee3\u7406\u5e02\u957f|\u5e38\u52a1\u526f\u5e02\u957f|\u526f\u5e02\u957f|\u5e02\u957f|"
    r"\u4e66\u8bb0|\u526f\u4e66\u8bb0|\u4e3b\u4efb|\u526f\u4e3b\u4efb|\u5c40\u957f|\u526f\u5c40\u957f|"
    r"\u59d4\u5458|\u4e3b\u5e2d|\u526f\u4e3b\u5e2d"
    r")$"
)


def _is_usable_person_name(name: Any) -> bool:
    text = _normalize_text(str(name or ""))
    if not text:
        return False
    lowered = text.lower()
    if "<title" in lowered or "</title>" in lowered:
        return False
    compact = re.sub(r"\s+", "", text)
    if _NAV_OR_ORG_NAME_RE.search(compact) or _NAV_OR_ORG_NAME_RE.search(text):
        return False
    if _ORG_FRAGMENT_NAME_RE.search(compact):
        return False
    if _BARE_TITLE_NAME_RE.fullmatch(compact):
        return False
    if re.fullmatch(r"[\u4e00-\u9fff]{2,4}", compact):
        return True
    # Title + name lines are usable only when a trailing person token can be extracted.
    token = _extract_person_name_token(text)
    if not token or not re.fullmatch(r"[\u4e00-\u9fff]{2,4}", token):
        return False
    if _BARE_TITLE_NAME_RE.fullmatch(token):
        return False
    return True


def _extract_person_name_token(value: Any) -> str:
    text = _normalize_text(str(value or ""))
    if not text:
        return ""
    compact = re.sub(r"\s+", "", text)
    if _NAV_OR_ORG_NAME_RE.search(compact) or _ORG_FRAGMENT_NAME_RE.search(compact):
        # Still allow "代理市长 刘勇" style titles that embed a person name.
        pass
    else:
        if re.fullmatch(r"[\u4e00-\u9fff]{2,4}", compact):
            return compact
    m = re.search(
        r"(?:\u4ee3\u7406\u5e02\u957f|\u5e38\u52a1\u526f\u5e02\u957f|\u526f\u5e02\u957f|\u5e02\u957f|"
        r"\u4e66\u8bb0|\u526f\u4e66\u8bb0|\u4e3b\u4efb|\u5c40\u957f|\u59d4\u5458|"
        r"\u5e02\u59d4\u526f\u4e66\u8bb0)[\uff1a:\s\u3000]*"
        r"([\u4e00-\u9fff](?:\s*[\u4e00-\u9fff]){1,3})\s*$",
        text,
    )
    if m:
        token = re.sub(r"\s+", "", m.group(1))
        if token and not _NAV_OR_ORG_NAME_RE.search(token) and not _ORG_FRAGMENT_NAME_RE.search(token):
            return token
    # "市委副书记、代理市长 刘勇" — take last 2-4 han after punctuation/space
    trailing = re.search(r"[\u3001\u3002,\s\u3000]+([\u4e00-\u9fff]{2,4})\s*$", text)
    if trailing:
        token = trailing.group(1)
        if not _NAV_OR_ORG_NAME_RE.search(token) and not _ORG_FRAGMENT_NAME_RE.search(token):
            return token
    if re.fullmatch(r"[\u4e00-\u9fff]{2,4}", compact):
        if not _NAV_OR_ORG_NAME_RE.search(compact) and not _ORG_FRAGMENT_NAME_RE.search(compact):
            return compact
    return ""


def _prefer_person_name(detail_name: Any, seed_name: Any) -> str:
    detail_token = _extract_person_name_token(detail_name)
    if detail_token and _is_usable_person_name(detail_token):
        return detail_token
    seed_token = _extract_person_name_token(seed_name)
    if seed_token and _is_usable_person_name(seed_token):
        return seed_token
    return ""


# Public aliases (same objects, no underscore prefix) for new call sites.
strip_html_markup = _strip_html_markup
normalize_text = _normalize_text
is_usable_person_name = _is_usable_person_name
extract_person_name_token = _extract_person_name_token
prefer_person_name = _prefer_person_name

__all__ = [
    "strip_html_markup",
    "normalize_text",
    "is_usable_person_name",
    "extract_person_name_token",
    "prefer_person_name",
    "_strip_html_markup",
    "_normalize_text",
    "_is_usable_person_name",
    "_extract_person_name_token",
    "_prefer_person_name",
]
