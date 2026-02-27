import hashlib
import json
import re
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
    return " ".join(value.split()).strip()


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
        self.handle_httpstatus_list = sorted(self.blocked_statuses.union({404}))

        self.required_fields = _ensure_list(
            self.rules.get("required_fields", ["name", "detail_url", "image_url"])
        )
        self.default_gender = str(self.rules.get("default_gender", "unknown"))
        self.gender_map = {str(k): str(v) for k, v in self.rules.get("gender_map", {}).items()}

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

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        if self.snapshot_html:
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)

    def parse(self, response: Response, **kwargs: Any) -> Iterable[Request]:
        del kwargs
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

            list_record = {
                "scraped_at": _utc_now_iso(),
                "list_url": response.url,
                "name": name,
                "detail_url": detail_url,
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
            yield response.follow(
                detail_url,
                callback=self.parse_detail,
                errback=self.errback_detail,
                meta={
                    "seed_name": name,
                    "list_url": response.url,
                },
            )

        for next_url in self._iter_next_pages(response, list_source):
            full_next_url = response.urljoin(next_url)
            if full_next_url in self._seen_list_urls:
                continue
            self._seen_list_urls.add(full_next_url)
            yield response.follow(
                full_next_url,
                callback=self.parse,
                errback=self.errback_list,
            )

    def parse_detail(self, response: Response) -> None:
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
        gender = self.gender_map.get(gender_text, self.default_gender)
        summary = self._extract_joined_text(detail_source, self.selectors.get("detail_summary"))

        extra_fields: Dict[str, str] = {}
        for key, selector in dict(self.selectors.get("detail_fields", {})).items():
            extra_fields[str(key)] = self._extract_first(detail_source, selector)

        record = {
            "scraped_at": _utc_now_iso(),
            "name": name,
            "detail_url": response.url,
            "list_url": str(response.meta.get("list_url", "")),
            "image_url": image_url,
            "gender": gender,
            "gender_raw": gender_text,
            "summary": summary,
            "fields": extra_fields,
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
        return "\n".join(values)

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
