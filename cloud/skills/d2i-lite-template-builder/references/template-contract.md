# D2I Cloud Template Contract

这是当前 Cloud 的最小模板合同。旧 Skill 或旧文档与此冲突时，以本合同和
`docs/d2i_cloud_template_extract_contract.md` 为准。

```json
{
  "site_name": "stable_site_name",
  "template_version": 1,
  "start_urls": ["https://example.com/people"],
  "allowed_domains": ["example.com"],
  "selectors": {
    "list_item": "xpath://...",
    "name": ["xpath:normalize-space(...)"],
    "detail_link": ["xpath:./@href"],
    "next_page": [],
    "detail_name": ["xpath:..."],
    "detail_image": ["xpath:.../@src"],
    "detail_summary": ["xpath:..."],
    "detail_full_text": ["xpath:..."],
    "detail_fields": {},
    "detail_field_labels": {}
  },
  "rules": {
    "obey_robots_txt": true,
    "snapshot_html": true,
    "extract_images": true,
    "write_metadata": true,
    "image_download_mode": "requests_jsl",
    "auto_fallback_to_browser": true,
    "required_fields": ["name", "detail_url"],
    "field_map": {
      "person": ["name"],
      "image_url": ["image_url"],
      "summary": ["summary"],
      "full_content": ["full_content"]
    },
    "image_naming": "name",
    "same_name_dedup": true
  },
  "crawl": {
    "speed_tier": "safe",
    "speed_tier_reason": "新站默认安全档",
    "concurrent_requests": 1,
    "download_delay": 5,
    "retry_times": 3,
    "timeout_seconds": 30
  }
}
```

## Required

- `site_name`
- 非空 `start_urls`
- 非空 `allowed_domains`
- `selectors.list_item`
- `selectors.name`
- `selectors.detail_link`
- `rules.required_fields` 必须包含 `name`、`detail_url`
- `rules.required_fields` 不得包含 `image_url`
- `crawl.speed_tier`: `safe`、`standard` 或 `turbo`

## Useful but optional

- `detail_image`：尽量填写；没有头像的人仍保留。
- `next_page`：只有真实分页时才需要。
- `gender`、`summary`、`full_content`：有来源就提取，不凭空补。
- `rules.admin`、`unit_name`：政府类模板应填写，保证最终路径正确。

## Fast acceptance

- 默认只跑一个小样队列。
- 样本通常 5–10 人；整个页面少于 10 人时直接全跑。
- 只在当前模式失败时尝试另一个模式。
- 成品至少抽查一张图片的 TITI 元数据回读。
- 抓错可修模板后重抓，不为追求形式完整反复跑空测试。

