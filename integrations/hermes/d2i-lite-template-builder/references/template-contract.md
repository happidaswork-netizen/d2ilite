# D2I Lite Template Contract

## Minimal shape

```json
{
  "site_name": "stable_ascii_or_chinese_id",
  "template_version": 1,
  "start_urls": ["https://example.com/people"],
  "allowed_domains": ["example.com"],
  "selectors": {
    "list_item": "...",
    "name": ["...::text"],
    "detail_link": "...::attr(href)",
    "next_page": ["...::attr(href)"],
    "detail_name": ["...::text"],
    "detail_image": "...::attr(src)",
    "detail_summary": ["...::text"],
    "detail_full_text": ["... *::text"],
    "detail_fields": {},
    "detail_field_labels": {}
  },
  "rules": {
    "obey_robots_txt": true,
    "image_download_mode": "requests_jsl",
    "auto_fallback_to_browser": true,
    "required_fields": ["name", "detail_url", "image_url"],
    "field_map": {
      "person": ["name"],
      "summary": ["summary"],
      "full_content": ["full_content"]
    }
  },
  "crawl": {
    "concurrent_requests": 1,
    "download_delay": 8,
    "retry_times": 3,
    "timeout_seconds": 30,
    "blocked_statuses": [403, 429],
    "blocked_backoff_hours": 8
  }
}
```

## Field boundaries

- `person/name/gender/city/unit/position` are sourced facts and map to `people_profile`.
- Template and collection identity map to `d2i_profile`.
- Full page text, HTML snapshots, source byte hashes and raw fields remain in D-layer JSONL/records.
- Initial collection writes `photo_audit.status=pending`; it must not invent archive decisions.

## Evidence checklist

- Record the entrance page, representative list page, at least three detail pages and pagination behavior.
- Verify that name and image selectors are scoped to the detail subject rather than shared navigation.
- Verify referer requirements and whether image URLs are absolute, relative, lazy-loaded or generated.
- Record selector hit counts and missing-field counts.

## Delivery document

The validation Markdown must contain:

1. Target and scope
2. Template path and SHA256
3. Structure-round result
4. Mode-round comparison
5. Pre-production result
6. expected/discovered/downloaded/completed/review/failed counts
7. Sample name/detail/image triples
8. Missing fields and known risks
9. Recommended mode and fallback condition
10. Statement that formal archive writes were not performed
