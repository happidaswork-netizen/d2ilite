# -*- coding: utf-8 -*-
"""Read-only source_url probe for bad-image triage (gap P0-4).

Never starts queues. Never bypasses login walls. requests/urllib only.
"""

from __future__ import annotations

import re
import ssl
import urllib.error
import urllib.request
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

# Conclusion enum (handoff)
CONCLUSIONS = frozenset(
    {
        "has_single_portrait",
        "placeholder_only",
        "http_404",
        "login_wall",
        "multi_person_news",
        "unknown",
    }
)

# Suggested actions
SUGGESTIONS = frozenset({"no_photo", "fix_template", "must_recrawl", "blocked", "rebind"})

_PLACEHOLDER_MARKERS = (
    "pub-user-portrait",
    "default_avatar",
    "default-avatar",
    "noavatar",
    "no-avatar",
    "placeholder",
    "user_portrait",
    "head_default",
    "avatar_default",
    "/nopic",
    "nobody",
)

_LOGIN_MARKERS = (
    "login",
    "signin",
    "passport",
    "sso",
    "cas/login",
    "统一身份认证",
    "请先登录",
    "用户登录",
)

_NEWS_MARKERS = (
    "news",
    "要闻",
    "新闻",
    "访谈",
    "在线访谈",
    "图集",
    "gallery",
)

_USER_AGENT = (
    "D2I-Cloud-SourceProbe/1.0 (+read-only triage; contact ops; no automated crawl)"
)


class _ImgCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.imgs: List[Dict[str, str]] = []
        self.title = ""
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        ad = {k.lower(): (v or "") for k, v in attrs}
        if tag.lower() == "img":
            src = ad.get("src") or ad.get("data-src") or ad.get("data-original") or ""
            self.imgs.append(
                {
                    "src": src,
                    "alt": ad.get("alt") or "",
                    "class": ad.get("class") or "",
                    "id": ad.get("id") or "",
                    "width": ad.get("width") or "",
                    "height": ad.get("height") or "",
                }
            )
        if tag.lower() == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data


def _fetch(url: str, *, timeout: float = 12.0) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5",
        },
        method="GET",
    )
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read(512_000)  # cap 512KB
            charset = "utf-8"
            ctype = str(resp.headers.get("Content-Type") or "")
            m = re.search(r"charset=([\\w-]+)", ctype, re.I)
            if m:
                charset = m.group(1)
            try:
                text = raw.decode(charset, errors="replace")
            except Exception:
                text = raw.decode("utf-8", errors="replace")
            return {
                "ok": True,
                "status": int(getattr(resp, "status", 200) or 200),
                "final_url": str(resp.geturl() or url),
                "content_type": ctype,
                "text": text,
                "error": "",
            }
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read(64_000).decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return {
            "ok": False,
            "status": int(exc.code or 0),
            "final_url": url,
            "content_type": "",
            "text": body,
            "error": f"HTTPError:{exc.code}",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "status": 0,
            "final_url": url,
            "content_type": "",
            "text": "",
            "error": f"{type(exc).__name__}:{exc}",
        }


def _is_placeholder_src(src: str, meta: str = "") -> bool:
    blob = f"{src} {meta}".lower()
    return any(m in blob for m in _PLACEHOLDER_MARKERS)


def _looks_login(url: str, text: str, title: str) -> bool:
    blob = f"{url} {title} {text[:2000]}".lower()
    hits = sum(1 for m in _LOGIN_MARKERS if m.lower() in blob)
    return hits >= 2 or ("login" in urlparse(url).path.lower() and "password" in blob)


def _score_portrait_candidate(img: Dict[str, str], page_url: str) -> int:
    src = img.get("src") or ""
    if not src or src.startswith("data:"):
        return -100
    meta = f"{img.get('class')} {img.get('id')} {img.get('alt')}"
    if _is_placeholder_src(src, meta):
        return -50
    score = 0
    low = f"{src} {meta}".lower()
    for token in ("avatar", "portrait", "photo", "head", "leader", "领导", "头像", "人物"):
        if token in low:
            score += 8
    # tiny declared dimensions
    try:
        w = int(re.sub(r"\\D", "", img.get("width") or "") or "0")
        h = int(re.sub(r"\\D", "", img.get("height") or "") or "0")
        if w and h and min(w, h) < 40:
            score -= 30
        if w and h and min(w, h) >= 80:
            score += 5
    except Exception:
        pass
    # prefer same-host images
    try:
        if urlparse(urljoin(page_url, src)).netloc == urlparse(page_url).netloc:
            score += 2
    except Exception:
        pass
    return score


def classify_page(
    *,
    url: str,
    status: int,
    final_url: str,
    text: str,
    error: str = "",
) -> Dict[str, Any]:
    """Pure classifier over fetched HTML (unit-testable without network)."""
    if status == 404 or (not text and "404" in (error or "")):
        return {
            "conclusion": "http_404",
            "suggest_action": "no_photo",
            "confidence": 0.9,
            "reason": "HTTP 404 or empty not-found page",
            "images": [],
            "best_image": "",
        }
    if status in {401, 403} or _looks_login(final_url or url, text, ""):
        parser = _ImgCollector()
        try:
            parser.feed(text or "")
        except Exception:
            pass
        if _looks_login(final_url or url, text, parser.title):
            return {
                "conclusion": "login_wall",
                "suggest_action": "blocked",
                "confidence": 0.85,
                "reason": "login / SSO markers on page or URL",
                "images": [],
                "best_image": "",
                "title": parser.title.strip(),
            }

    parser = _ImgCollector()
    try:
        parser.feed(text or "")
    except Exception:
        pass
    title = parser.title.strip()
    imgs = parser.imgs
    scored: List[Tuple[int, Dict[str, str]]] = []
    placeholders = 0
    for im in imgs:
        src = im.get("src") or ""
        meta = f"{im.get('class')} {im.get('id')} {im.get('alt')}"
        if _is_placeholder_src(src, meta):
            placeholders += 1
            scored.append((-50, im))
            continue
        scored.append((_score_portrait_candidate(im, final_url or url), im))
    scored.sort(key=lambda x: -x[0])

    page_blob = f"{title} {text[:3000]}".lower()
    newsy = any(m.lower() in page_blob for m in _NEWS_MARKERS)
    good = [im for sc, im in scored if sc >= 5]
    weak = [im for sc, im in scored if 0 <= sc < 5]

    if placeholders and not good:
        return {
            "conclusion": "placeholder_only",
            "suggest_action": "no_photo",
            "confidence": 0.8,
            "reason": f"only placeholder avatars ({placeholders}); e.g. pub-user-portrait",
            "images": [urljoin(final_url or url, im.get("src") or "") for _, im in scored[:8]],
            "best_image": "",
            "title": title,
            "image_count": len(imgs),
            "placeholder_count": placeholders,
        }

    if len(good) >= 2 and newsy:
        return {
            "conclusion": "multi_person_news",
            "suggest_action": "no_photo",
            "confidence": 0.7,
            "reason": "news/interview page with multiple non-placeholder images",
            "images": [urljoin(final_url or url, im.get("src") or "") for im in good[:8]],
            "best_image": urljoin(final_url or url, good[0].get("src") or ""),
            "title": title,
            "image_count": len(imgs),
        }

    if len(good) == 1 or (len(good) >= 1 and not newsy and scored[0][0] >= 8):
        best = good[0]
        abs_src = urljoin(final_url or url, best.get("src") or "")
        # Local file missing → must_recrawl; caller may override to rebind if on disk.
        return {
            "conclusion": "has_single_portrait",
            "suggest_action": "fix_template",
            "confidence": 0.75 if scored[0][0] >= 8 else 0.6,
            "reason": "single likely portrait image on detail page",
            "images": [urljoin(final_url or url, im.get("src") or "") for im in good[:5]],
            "best_image": abs_src,
            "title": title,
            "image_count": len(imgs),
        }

    if not imgs:
        return {
            "conclusion": "unknown",
            "suggest_action": "must_recrawl",
            "confidence": 0.4,
            "reason": "page fetched but no <img> found",
            "images": [],
            "best_image": "",
            "title": title,
        }

    if weak and not good:
        return {
            "conclusion": "unknown",
            "suggest_action": "must_recrawl",
            "confidence": 0.45,
            "reason": "images present but none look like a single official portrait",
            "images": [urljoin(final_url or url, im.get("src") or "") for im in weak[:5]],
            "best_image": urljoin(final_url or url, weak[0].get("src") or ""),
            "title": title,
            "image_count": len(imgs),
        }

    return {
        "conclusion": "unknown",
        "suggest_action": "must_recrawl",
        "confidence": 0.35,
        "reason": "unable to classify page layout confidently",
        "images": [urljoin(final_url or url, im.get("src") or "") for _, im in scored[:5]],
        "best_image": "",
        "title": title,
        "image_count": len(imgs),
    }


def probe_source_url(url: str, *, timeout: float = 12.0) -> Dict[str, Any]:
    """Fetch + classify one source_url. Read-only."""
    raw_url = str(url or "").strip()
    if not raw_url:
        return {
            "ok": False,
            "error": "source_url required",
            "conclusion": "unknown",
            "suggest_action": "blocked",
        }
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        return {
            "ok": False,
            "error": f"unsupported scheme: {parsed.scheme}",
            "conclusion": "unknown",
            "suggest_action": "blocked",
            "source_url": raw_url,
        }
    fetched = _fetch(raw_url, timeout=timeout)
    classified = classify_page(
        url=raw_url,
        status=int(fetched.get("status") or 0),
        final_url=str(fetched.get("final_url") or raw_url),
        text=str(fetched.get("text") or ""),
        error=str(fetched.get("error") or ""),
    )
    return {
        "ok": True,
        "source_url": raw_url,
        "http_status": fetched.get("status"),
        "final_url": fetched.get("final_url"),
        "fetch_error": fetched.get("error") or "",
        "fetched_ok": bool(fetched.get("ok")),
        **classified,
    }


def probe_person(
    person_id: str = "",
    *,
    source_url: str = "",
    people_db=None,
    timeout: float = 12.0,
    local_on_disk: Optional[bool] = None,
) -> Dict[str, Any]:
    """Probe a person row's source_url; refine suggest_action with local disk state."""
    from cloud import people_workflow

    row = None
    pid = str(person_id or "").strip()
    if pid:
        row = people_workflow.get_person(pid, people_db=people_db)
        if not row:
            return {"ok": False, "error": f"person not found: {pid}", "person_id": pid}
        source_url = source_url or str(row.get("source_url") or "")
        if local_on_disk is None:
            path_info = row.get("path_info") if isinstance(row.get("path_info"), dict) else {}
            local_on_disk = bool(path_info.get("on_disk"))
    if not source_url:
        return {
            "ok": False,
            "error": "no source_url",
            "person_id": pid,
            "conclusion": "unknown",
            "suggest_action": "blocked",
        }
    out = probe_source_url(source_url, timeout=timeout)
    out["person_id"] = pid
    if row:
        out["name"] = row.get("name")
        out["unit_name"] = row.get("unit_name")
        out["source_bucket"] = row.get("source_bucket")
    # Refine action with local state
    conclusion = out.get("conclusion")
    if conclusion == "has_single_portrait":
        if local_on_disk:
            out["suggest_action"] = "rebind"
            out["reason"] = (out.get("reason") or "") + " · local file present → prefer rebind"
        else:
            out["suggest_action"] = "must_recrawl"
            out["reason"] = (out.get("reason") or "") + " · no local file → must_recrawl / fix_template"
    # legacy bucket: never default to fix_template
    if row and row.get("source_bucket") == "legacy_import":
        if out.get("suggest_action") == "fix_template":
            out["suggest_action"] = "must_recrawl" if not local_on_disk else "rebind"
            out["reason"] = (out.get("reason") or "") + " · legacy_import: no template-fix default"
    return out
