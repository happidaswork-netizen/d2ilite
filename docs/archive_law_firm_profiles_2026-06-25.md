# 律所公开人物抓取归档（2026-06-25）

## 状态

本轮律所公开人物头像抓取已暂停，不再继续启动新的律所任务。

自动回看 `继续律所头像抓取` 已暂停。当前未发现仍在运行的律所抓取 Python 进程。

## 已完成目录

| 来源 | 输出目录 | profiles | images | metadata | failures | 男 | 女 | 未知 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 天同律师事务所 | `data/public_archive/天同律师事务所_专业团队` | 14 | 14 | 14 | 0 | 0 | 0 | 14 |
| 海问律师事务所 | `data/public_archive/海问律师事务所_专业人员` | 39 | 39 | 78 | 0 | 4 | 2 | 33 |
| 君合律师事务所 | `data/public_archive/君合律师事务所_专业人员` | 431 | 431 | 431 | 0 | 63 | 51 | 317 |

合计图片：484 张。

## 配置与日志

- 天同配置：`scraper/config.law.tiantong.fast.json`
- 海问配置：`scraper/config.law.haiwen.fast.json`
- 君合配置：`scraper/config.law.junhe.fast.json`
- 君合模板：`scraper/templates/君合律师事务所_专业人员.json`
- 天同日志：`data/public_archive/law_tiantong_run.log`
- 海问日志：`data/public_archive/law_haiwen_run.log`、`data/public_archive/law_haiwen_fast_resume.log`
- 君合日志：`data/public_archive/law_junhe_run.log`

## 注意事项

- `review_queue.jsonl` 主要是 `gender` 缺失，不是下载失败。
- 海问 `metadata=78` 是因为旧节奏抓取后用无等待配置续跑，元数据写入结果存在重复记录；最终图片数以 `images=39` 为准。
- 未知性别只按官网文本证据处理；未使用头像外貌推断。
