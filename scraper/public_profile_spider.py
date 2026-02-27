import hashlib
import json
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse

import scrapy
from scrapy import Request, Selector
from scrapy.http import Response
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.python.failure import Failure


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = " ".join(str(value).split()).strip()
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


def _normalize_multiline_text(value: Optional[str]) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text:
        return ""
    lines: List[str] = []
    for raw_line in text.split("\n"):
        line = _normalize_text(raw_line)
        if line:
            lines.append(line)
    return "\n".join(lines)


def _normalize_gender(value: Any) -> str:
    raw = _normalize_text(str(value or ""))
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


def _ensure_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    return [str(value)]


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


class PublicProfileSpider(scrapy.Spider):
    name = "public_profile_spider"

    def __init__(self, config: Dict[str, Any], output_dir: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.config = config
        self.selectors = config.get("selectors", {})
        self.rules = config.get("rules", {})
        self.crawl_cfg = config.get("crawl", {})

        self.start_urls = [str(u) for u in config.get("start_urls", []) if u]
        self.allowed_domains = [str(d) for d in config.get("allowed_domains", []) if d]

        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "raw"
        self.state_dir = self.output_dir / "state"
        self.snapshots_dir = self.output_dir / "snapshots"
        self.reports_dir = self.output_dir / "reports"

        self.list_path = self.raw_dir / "list_records.jsonl"
        self.profile_path = self.raw_dir / "profiles.jsonl"
        self.review_path = self.raw_dir / "review_queue.jsonl"
        self.failures_path = self.raw_dir / "failures.jsonl"

        self.snapshot_html = bool(self.rules.get("snapshot_html", True))
        self.blocked_statuses = {
            int(s)
            for s in self.crawl_cfg.get("blocked_statuses", [403, 429])
            if str(s).strip().isdigit()
        }
        self.jsl_clearance_enabled = bool(self.rules.get("jsl_clearance_enabled", False))
        self.jsl_max_retries = max(1, int(self.rules.get("jsl_max_retries", 3)))
        http_statuses = set(self.blocked_statuses).union({404})
        if self.jsl_clearance_enabled:
            http_statuses.update({412, 521})
        self.handle_httpstatus_list = sorted(http_statuses)

        self.required_fields = _ensure_list(
            self.rules.get("required_fields", ["name", "detail_url", "image_url"])
        )
        self.default_gender = _normalize_gender(self.rules.get("default_gender", ""))
        self.field_map = self._resolve_field_map()
        self.gender_map: Dict[str, str] = {}
        for k, v in dict(self.rules.get("gender_map", {})).items():
            key_raw = str(k)
            key_norm = _normalize_text(key_raw)
            value_norm = _normalize_gender(v)
            self.gender_map[key_raw] = value_norm
            if key_norm:
                self.gender_map[key_norm] = value_norm

        self.blocked_until: Optional[str] = None
        self.blocked_reason: Optional[str] = None
        self.metrics: Dict[str, int] = {
            "list_rows_seen": 0,
            "detail_requests_enqueued": 0,
            "detail_pages_saved": 0,
            "detail_duplicates_skipped": 0,
            "missing_required_items": 0,
            "failures": 0,
        }

        self._known_detail_urls = self._load_existing_detail_urls()
        self._seen_list_urls: set = set()
        self._clearance_cookie_header: str = ""

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        if self.snapshot_html:
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)

    def parse(self, response: Response, **kwargs: Any) -> Iterable[Request]:
        del kwargs
        self._remember_set_cookies(response)
        retry_req = self._retry_jsl_clearance(
            response,
            callback=self.parse,
            errback=self.errback_list,
            phase="list",
        )
        if retry_req is not None:
            yield retry_req
            return

        self._seen_list_urls.add(response.url)
        if self._check_blocked(response):
            return
        if response.status != 200:
            self._record_failure(
                url=response.url,
                reason=f"list_http_{response.status}",
                context={"phase": "list"},
            )
            return

        self._save_snapshot("list", response)
        list_source = self._build_selector_source(response, phase="list")
        list_item_selector = self.selectors.get("list_item")
        if not list_item_selector:
            self.logger.error("selectors.list_item is required")
            return

        list_nodes = list_source.css(str(list_item_selector))
        for node in list_nodes:
            self.metrics["list_rows_seen"] += 1
            name = self._extract_first(node, self.selectors.get("name"))
            detail_link = self._extract_first(node, self.selectors.get("detail_link"))
            detail_url = response.urljoin(detail_link) if detail_link else ""
            list_fields: Dict[str, str] = {}
            for key, selector in dict(self.selectors.get("list_fields", {})).items():
                field_key = str(key).strip()
                if not field_key:
                    continue
                field_value = self._normalize_optional_field(self._extract_first(node, selector))
                if field_value:
                    list_fields[field_key] = field_value

            list_record = {
                "scraped_at": _utc_now_iso(),
                "list_url": response.url,
                "name": name,
                "detail_url": detail_url,
                "fields": list_fields,
            }
            self._append_jsonl(self.list_path, list_record)

            if not detail_url:
                self.metrics["missing_required_items"] += 1
                self._append_jsonl(
                    self.review_path,
                    {
                        "scraped_at": _utc_now_iso(),
                        "reason": "missing_detail_url_from_list",
                        "record": list_record,
                    },
                )
                continue

            if detail_url in self._known_detail_urls:
                self.metrics["detail_duplicates_skipped"] += 1
                continue

            self._known_detail_urls.add(detail_url)
            self.metrics["detail_requests_enqueued"] += 1
            req = response.follow(
                detail_url,
                callback=self.parse_detail,
                errback=self.errback_detail,
                meta={
                    "seed_name": name,
                    "list_url": response.url,
                    "seed_fields": list_fields,
                },
            )
            yield self._apply_clearance_cookie_to_request(req)

        for next_url in self._iter_next_pages(response, list_source):
            full_next_url = response.urljoin(next_url)
            if full_next_url in self._seen_list_urls:
                continue
            self._seen_list_urls.add(full_next_url)
            req = response.follow(
                full_next_url,
                callback=self.parse,
                errback=self.errback_list,
            )
            yield self._apply_clearance_cookie_to_request(req)

    def parse_detail(self, response: Response) -> Iterable[Request]:
        self._remember_set_cookies(response)
        retry_req = self._retry_jsl_clearance(
            response,
            callback=self.parse_detail,
            errback=self.errback_detail,
            phase="detail",
        )
        if retry_req is not None:
            yield retry_req
            return

        if self._check_blocked(response):
            return
        if response.status != 200:
            self.metrics["failures"] += 1
            self._record_failure(
                url=response.url,
                reason=f"detail_http_{response.status}",
                context={"phase": "detail", "list_url": response.meta.get("list_url", "")},
            )
            return

        self._save_snapshot("detail", response)
        detail_source = self._build_selector_source(response, phase="detail")

        detail_name = self._extract_first(detail_source, self.selectors.get("detail_name"))
        seed_name = _normalize_text(str(response.meta.get("seed_name", "")))
        name = detail_name or seed_name
        image_url_raw = self._extract_first(detail_source, self.selectors.get("detail_image"))
        image_url = response.urljoin(image_url_raw) if image_url_raw else ""
        gender_text = self._extract_first(detail_source, self.selectors.get("detail_gender"))
        gender_lookup = _normalize_text(gender_text)
        mapped_gender = self.gender_map.get(gender_text)
        if mapped_gender is None and gender_lookup:
            mapped_gender = self.gender_map.get(gender_lookup)
        gender = _normalize_gender(mapped_gender if mapped_gender is not None else gender_text)
        if not gender:
            gender = self.default_gender
        summary = self._extract_joined_text(detail_source, self.selectors.get("detail_summary"))
        full_content = self._extract_full_content_text(detail_source)

        seed_fields = response.meta.get("seed_fields", {})
        base_fields: Dict[str, str] = {}
        if isinstance(seed_fields, dict):
            for key, value in seed_fields.items():
                field_key = str(key).strip()
                field_value = self._normalize_optional_field(value)
                if field_key and field_value:
                    base_fields[field_key] = field_value

        extra_fields: Dict[str, str] = {}
        for key, selector in dict(self.selectors.get("detail_fields", {})).items():
            field_key = str(key).strip()
            if not field_key:
                continue
            field_value = self._normalize_optional_field(self._extract_first(detail_source, selector))
            if field_value:
                extra_fields[field_key] = field_value

        merged_fields: Dict[str, str] = dict(base_fields)
        for key, value in extra_fields.items():
            if value:
                merged_fields[key] = value

        mapped_fields = self._apply_field_map(
            name=name,
            gender=gender,
            summary=summary,
            full_content=full_content,
            detail_url=response.url,
            list_url=str(response.meta.get("list_url", "")),
            image_url=image_url,
            fields=merged_fields,
        )

        record = {
            "scraped_at": _utc_now_iso(),
            "name": name,
            "detail_url": response.url,
            "list_url": str(response.meta.get("list_url", "")),
            "image_url": image_url,
            "gender": gender,
            "gender_raw": gender_text,
            "summary": summary,
            "full_content": full_content,
            "fields": merged_fields,
            "mapped": mapped_fields,
        }
        self._append_jsonl(self.profile_path, record)
        self.metrics["detail_pages_saved"] += 1

        missing_fields = [field for field in self.required_fields if not record.get(field)]
        if missing_fields:
            self.metrics["missing_required_items"] += 1
            self._append_jsonl(
                self.review_path,
                {
                    "scraped_at": _utc_now_iso(),
                    "reason": "missing_required_fields",
                    "missing_fields": missing_fields,
                    "record": record,
                },
            )

    def _retry_jsl_clearance(
        self,
        response: Response,
        *,
        callback: Any,
        errback: Any,
        phase: str,
    ) -> Optional[Request]:
        if not self.jsl_clearance_enabled:
            return None
        if response.status not in {412, 521}:
            return None
        body = response.text or ""
        if "<script" not in body.lower():
            return None

        retry_count = int(response.meta.get("jsl_retry_count", 0))
        if retry_count >= self.jsl_max_retries:
            self._record_failure(
                url=response.url,
                reason="jsl_retry_exhausted",
                context={"phase": phase, "status": response.status},
            )
            return None

        cookie_pair = self._solve_jsl_cookie(response.url, body)
        if not cookie_pair:
            self._record_failure(
                url=response.url,
                reason="jsl_solve_failed",
                context={"phase": phase, "status": response.status},
            )
            return None

        new_request = response.request.replace(
            callback=callback,
            errback=errback,
            dont_filter=True,
        )
        request_cookie_header = response.request.headers.get("Cookie", b"").decode(
            "utf-8", errors="ignore"
        ).strip()
        existing_cookie_header = self._merge_cookie_header(
            self._clearance_cookie_header,
            request_cookie_header,
        )
        merged_cookie_header = existing_cookie_header
        for raw_set_cookie in response.headers.getlist("Set-Cookie"):
            set_cookie_text = raw_set_cookie.decode("utf-8", errors="ignore")
            merged_cookie_header = self._merge_cookie_header(merged_cookie_header, set_cookie_text)
        merged_cookie_header = self._merge_cookie_header(merged_cookie_header, cookie_pair)
        if merged_cookie_header:
            self._clearance_cookie_header = self._merge_cookie_header(
                self._clearance_cookie_header,
                merged_cookie_header,
            )
            new_request.headers["Cookie"] = self._clearance_cookie_header.encode("utf-8")
        new_request.meta["jsl_retry_count"] = retry_count + 1
        self.logger.info(
            "Solved jsl clearance (%s), retry %s (attempt %s)",
            response.status,
            response.url,
            retry_count + 1,
        )
        return new_request

    def _solve_jsl_cookie(self, url: str, payload: str) -> str:
        script_match = re.search(r"<script[^>]*>(.*?)</script>", payload, flags=re.S | re.I)
        if not script_match:
            return ""
        script = script_match.group(1)

        parsed = urlparse(url)
        path = parsed.path or "/"
        search = f"?{parsed.query}" if parsed.query else ""

        node_code = (
            "let assigned='';\n"
            "const document={_cookie:'',set cookie(v){assigned=v;this._cookie=v;},get cookie(){return this._cookie;}};\n"
            f"const location={{pathname:{json.dumps(path)},search:{json.dumps(search)},href:''}};\n"
            "const window={location,navigator:{userAgent:'Mozilla/5.0'},outerHeight:1000,innerHeight:1000,"
            "outerWidth:1200,innerWidth:1200,Firebug:false,_phantom:false,__phantomas:false,chrome:{runtime:{}}};\n"
            "global.window=window;global.document=document;global.location=location;\n"
            "global.alert=function(){};global.setTimeout=function(fn,ms){fn();return 0;};\n"
            f"{script}\n"
            "console.log((assigned||document._cookie||'').split(';')[0]);\n"
        )

        try:
            proc = subprocess.run(
                ["node", "-"],
                input=node_code.encode("utf-8"),
                capture_output=True,
                timeout=20,
            )
        except Exception:
            self.logger.exception("failed to execute node for jsl cookie: %s", url)
            return ""
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="ignore").strip()
            self.logger.warning("jsl node solver failed (%s): %s", url, stderr[:280])
            return ""

        output_lines = proc.stdout.decode("utf-8", errors="ignore").strip().splitlines()
        if not output_lines:
            return ""
        first = output_lines[0].strip()
        if "=" not in first:
            return ""
        return first

    @staticmethod
    def _merge_cookie_header(existing_header: str, cookie_pair: str) -> str:
        merged: Dict[str, str] = {}

        for chunk in str(existing_header or "").split(";"):
            part = chunk.strip()
            if not part or "=" not in part:
                continue
            key, value = part.split("=", 1)
            merged[key.strip()] = value.strip()

        pair = str(cookie_pair or "").split(";", 1)[0].strip()
        if pair and ("=" in pair):
            key, value = pair.split("=", 1)
            merged[key.strip()] = value.strip()

        return "; ".join([f"{k}={v}" for k, v in merged.items()])

    def _remember_set_cookies(self, response: Response) -> None:
        for raw_set_cookie in response.headers.getlist("Set-Cookie"):
            set_cookie_text = raw_set_cookie.decode("utf-8", errors="ignore")
            self._clearance_cookie_header = self._merge_cookie_header(
                self._clearance_cookie_header,
                set_cookie_text,
            )

    def _apply_clearance_cookie_to_request(self, request: Request) -> Request:
        if self._clearance_cookie_header:
            request.headers["Cookie"] = self._clearance_cookie_header.encode("utf-8")
        return request

    def errback_list(self, failure: Failure) -> None:
        self.metrics["failures"] += 1
        self._record_failure_from_errback(failure, phase="list")

    def errback_detail(self, failure: Failure) -> None:
        self.metrics["failures"] += 1
        self._record_failure_from_errback(failure, phase="detail")

    def closed(self, reason: str) -> None:
        report = {
            "closed_at": _utc_now_iso(),
            "close_reason": reason,
            "blocked_until": self.blocked_until,
            "blocked_reason": self.blocked_reason,
            "metrics_this_run": self.metrics,
            "totals_on_disk": {
                "list_records": _count_jsonl(self.list_path),
                "profiles": _count_jsonl(self.profile_path),
                "review_queue": _count_jsonl(self.review_path),
                "failures": _count_jsonl(self.failures_path),
            },
        }
        self._write_json(self.reports_dir / "crawl_report.json", report)

        if self.blocked_until:
            self._write_json(
                self.state_dir / "backoff_state.json",
                {
                    "blocked_until": self.blocked_until,
                    "blocked_reason": self.blocked_reason,
                    "recorded_at": _utc_now_iso(),
                },
            )

    def _iter_next_pages(self, response: Response, list_source: Any) -> Iterable[str]:
        for selector in _ensure_list(self.selectors.get("next_page")):
            for raw_url in self._select_values(list_source, selector):
                next_url = self._normalize_next_page_url(raw_url)
                if next_url:
                    yield next_url

    def _extract_first(self, selector_source: Any, selector_spec: Any) -> str:
        for selector in _ensure_list(selector_spec):
            values = self._select_values(selector_source, selector)
            for value in values:
                normalized = _normalize_text(value)
                if normalized:
                    return normalized
        return ""

    def _extract_joined_text(self, selector_source: Any, selector_spec: Any) -> str:
        values: List[str] = []
        for selector in _ensure_list(selector_spec):
            for value in self._select_values(selector_source, selector):
                normalized = _normalize_text(value)
                if normalized:
                    values.append(normalized)
        return _normalize_multiline_text("\n".join(values))

    def _extract_full_content_text(self, selector_source: Any) -> str:
        configured = self.selectors.get("detail_full_text")
        if configured:
            extracted = self._extract_joined_text(selector_source, configured)
            if extracted:
                return extracted

        fallback_xpath = (
            "xpath://body//text()[normalize-space() and not(ancestor::script) and "
            "not(ancestor::style) and not(ancestor::noscript)]"
        )
        chunks = self._select_values(selector_source, fallback_xpath)
        merged: List[str] = []
        last_line = ""
        for chunk in chunks:
            line = _normalize_text(chunk)
            if not line:
                continue
            if line == last_line:
                continue
            merged.append(line)
            last_line = line
        return _normalize_multiline_text("\n".join(merged))

    @staticmethod
    def _normalize_optional_field(value: Any) -> str:
        raw = _normalize_text(str(value or ""))
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

    @staticmethod
    def _strip_prefixed_label(value: str, labels: List[str]) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        for label in labels:
            for sep in ("：", ":"):
                prefix = f"{label}{sep}"
                if text.startswith(prefix):
                    return text[len(prefix) :].strip()
        return text

    def _resolve_field_map(self) -> Dict[str, List[str]]:
        merged: Dict[str, Any] = {}
        if isinstance(self.selectors.get("field_map"), dict):
            merged.update(dict(self.selectors.get("field_map", {})))
        if isinstance(self.rules.get("field_map"), dict):
            merged.update(dict(self.rules.get("field_map", {})))
        normalized: Dict[str, List[str]] = {}
        for target_key, source_spec in merged.items():
            target = str(target_key or "").strip()
            if not target:
                continue
            sources = [str(x).strip() for x in _ensure_list(source_spec) if str(x).strip()]
            if sources:
                normalized[target] = sources
        return normalized

    def _resolve_field_value_from_source(
        self,
        source_key: str,
        *,
        name: str,
        gender: str,
        summary: str,
        full_content: str,
        detail_url: str,
        list_url: str,
        image_url: str,
        fields: Dict[str, str],
    ) -> str:
        token = str(source_key or "").strip()
        if not token:
            return ""
        lowered = token.lower()

        if lowered.startswith("field.") or lowered.startswith("fields."):
            field_key = token.split(".", 1)[1].strip()
            return self._normalize_optional_field(fields.get(field_key, ""))

        builtins: Dict[str, str] = {
            "name": _normalize_text(name),
            "person": _normalize_text(name),
            "gender": _normalize_gender(gender),
            "summary": _normalize_multiline_text(summary),
            "full_content": _normalize_multiline_text(full_content),
            "detail_url": _normalize_text(detail_url),
            "source_url": _normalize_text(list_url),
            "list_url": _normalize_text(list_url),
            "image_url": _normalize_text(image_url),
        }
        if lowered in builtins:
            return builtins[lowered]

        from_fields = self._normalize_optional_field(fields.get(token, ""))
        if from_fields:
            return from_fields
        return builtins.get(lowered, "")

    def _apply_field_map(
        self,
        *,
        name: str,
        gender: str,
        summary: str,
        full_content: str,
        detail_url: str,
        list_url: str,
        image_url: str,
        fields: Dict[str, str],
    ) -> Dict[str, str]:
        mapped: Dict[str, str] = {}
        for target, source_tokens in self.field_map.items():
            value = ""
            for token in source_tokens:
                candidate = self._resolve_field_value_from_source(
                    token,
                    name=name,
                    gender=gender,
                    summary=summary,
                    full_content=full_content,
                    detail_url=detail_url,
                    list_url=list_url,
                    image_url=image_url,
                    fields=fields,
                )
                if candidate:
                    value = candidate
                    break
            if not value:
                continue
            target_lower = target.lower()
            if target_lower == "gender":
                value = _normalize_gender(value)
            elif target_lower in {"description", "summary", "full_content"}:
                value = _normalize_multiline_text(value)
            elif target_lower in {"email", "email_text"}:
                value = self._strip_prefixed_label(value, ["邮箱", "Email", "email"])
            elif target_lower in {"city", "location", "location_text"}:
                value = self._strip_prefixed_label(value, ["工作地点", "地点", "城市", "City", "city"])
            else:
                value = self._normalize_optional_field(value)
            if value:
                mapped[target] = value
        return mapped

    def _select_values(self, selector_source: Any, selector: str) -> List[str]:
        try:
            if selector.startswith("xpath:"):
                return selector_source.xpath(selector[len("xpath:") :]).getall()
            if selector.startswith("/"):
                return selector_source.xpath(selector).getall()
            return selector_source.css(selector).getall()
        except Exception:
            self.logger.exception("selector failed: %s", selector)
            return []

    def _build_selector_source(self, response: Response, phase: str) -> Any:
        transform_key = f"{phase}_response_transform"
        transform = _normalize_text(str(self.selectors.get(transform_key, ""))).lower()
        if transform != "document_write_html":
            return response

        html_payload = self._extract_document_write_html(response.text)
        if not html_payload:
            self.logger.warning("document_write_html transform produced empty payload: %s", response.url)
            return response
        return Selector(text=html_payload, type="html")

    @staticmethod
    def _decode_js_string(value: str) -> str:
        # Minimal JS-string unescape for document.write payloads.
        result = value
        result = re.sub(
            r"\\u([0-9a-fA-F]{4})",
            lambda m: chr(int(m.group(1), 16)),
            result,
        )
        result = result.replace(r"\/", "/")
        result = result.replace(r"\"", '"')
        result = result.replace(r"\'", "'")
        result = result.replace(r"\r", "\r")
        result = result.replace(r"\n", "\n")
        result = result.replace(r"\t", "\t")
        result = result.replace(r"\\", "\\")
        return result

    def _extract_document_write_html(self, payload: str) -> str:
        if not payload:
            return ""
        pattern = re.compile(r'document\.write\("((?:\\.|[^"\\])*)"\);?', flags=re.S)
        parts: List[str] = []
        for match in pattern.finditer(payload):
            decoded = self._decode_js_string(match.group(1))
            if decoded.strip():
                parts.append(decoded)
        if parts:
            return "\n".join(parts)
        if payload.lstrip().startswith("<"):
            return payload
        return ""

    def _normalize_next_page_url(self, raw_url: str) -> str:
        candidate = _normalize_text(raw_url)
        if not candidate or candidate == "#":
            return ""
        if "SYCmsPage(" in candidate:
            return self._parse_sycms_page_onclick(candidate)
        lower = candidate.lower()
        if "return false" in lower:
            return ""
        if lower.startswith("javascript:"):
            return ""
        if candidate.startswith(("/", "http://", "https://", "?")):
            return candidate
        return candidate

    @staticmethod
    def _parse_sycms_page_onclick(onclick: str) -> str:
        match = re.search(
            r"SYCmsPage\('(?P<page>\d+)','(?P<obji>[^']*)','(?P<temurl>[^']*)','(?P<query>[^']*)'",
            onclick,
        )
        if not match:
            return ""
        page = match.group("page")
        obji = match.group("obji")
        temurl = match.group("temurl").replace(r"\/", "/").strip()
        query = match.group("query").replace(r"\/", "/").lstrip("&")
        if not temurl:
            return ""

        params: Dict[str, str] = {}
        for key, value in parse_qsl(query, keep_blank_values=True):
            if key.lower() == "page":
                continue
            params[key] = value
        params["Page"] = page
        if obji:
            has_obji = any(k.lower() == "obji" for k in params.keys())
            if not has_obji:
                params["ObjI"] = obji

        query_string = urlencode(params)
        delimiter = "&" if "?" in temurl else "?"
        return f"{temurl}{delimiter}{query_string}"

    def _check_blocked(self, response: Response) -> bool:
        if response.status not in self.blocked_statuses:
            return False
        backoff_hours = float(self.crawl_cfg.get("blocked_backoff_hours", 6))
        unblock_at = datetime.now(timezone.utc) + timedelta(hours=backoff_hours)
        self.blocked_until = unblock_at.replace(microsecond=0).isoformat()
        self.blocked_reason = f"http_{response.status}_at_{response.url}"
        self.logger.warning("Blocked by %s, close spider until %s", response.status, self.blocked_until)
        crawler = getattr(self, "crawler", None)
        if crawler and crawler.engine:
            crawler.engine.close_spider(self, reason=f"blocked_{response.status}")
        return True

    def _save_snapshot(self, phase: str, response: Response) -> None:
        if not self.snapshot_html:
            return
        hashed = hashlib.sha1(response.url.encode("utf-8")).hexdigest()
        file_path = self.snapshots_dir / f"{phase}_{hashed}.html"
        if file_path.exists():
            return
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(response.body)

    def _record_failure(self, url: str, reason: str, context: Dict[str, Any]) -> None:
        self._append_jsonl(
            self.failures_path,
            {
                "failed_at": _utc_now_iso(),
                "url": url,
                "reason": reason,
                "context": context,
            },
        )

    def _record_failure_from_errback(self, failure: Failure, phase: str) -> None:
        request = getattr(failure, "request", None)
        url = getattr(request, "url", "")
        reason = failure.getErrorMessage()
        if failure.check(HttpError):
            response = failure.value.response
            reason = f"http_error_{response.status}"
        self._record_failure(
            url=url,
            reason=reason,
            context={"phase": phase},
        )

    def _load_existing_detail_urls(self) -> set:
        known: set = set()
        if not self.profile_path.exists():
            return known
        with self.profile_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                detail_url = str(record.get("detail_url", "")).strip()
                if detail_url:
                    known.add(detail_url)
        return known

    @staticmethod
    def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    @staticmethod
    def _write_json(path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def default_output_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.replace(":", "_")
    token = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    return f"{host}_{token}"
