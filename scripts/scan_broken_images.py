#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量扫描目录中的图片损坏情况。

分级：
- broken: 图片无法完整解码（确定损坏）
- suspect: 可解码，但检测到明显横向断层特征（疑似损坏）
- ok: 未发现异常

说明：
- suspect 是启发式检测，不是 100% 定论；
- 建议先用 --since 只扫“出问题时间段”改动过的文件。
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

from PIL import Image, ImageStat

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}


@dataclass
class ScanResult:
    path: str
    mtime: str
    size_bytes: int
    status: str
    reason: str


def _iter_images(root_dir: str, recursive: bool) -> Iterable[str]:
    root = os.path.abspath(root_dir)
    if recursive:
        for base, _dirs, files in os.walk(root):
            for name in files:
                if os.path.splitext(name)[1].lower() in IMAGE_EXTS:
                    yield os.path.join(base, name)
    else:
        for name in os.listdir(root):
            path = os.path.join(root, name)
            if (not os.path.isfile(path)) or (os.path.splitext(name)[1].lower() not in IMAGE_EXTS):
                continue
            yield path


def _parse_since(value: str) -> Optional[datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        # 支持 2026-02-26 / 2026-02-26T08:30:00
        return datetime.fromisoformat(s)
    except Exception:
        raise ValueError(f"invalid --since value: {value}")


def _mtime_dt(path: str) -> datetime:
    return datetime.fromtimestamp(float(os.path.getmtime(path)))


def _safe_open_rgb(path: str) -> Image.Image:
    with Image.open(path) as im:
        im.load()
        rgb = im.convert("RGB")
    return rgb


def _decode_check(path: str) -> Tuple[bool, str]:
    try:
        # 第一次 verify 检查文件结构
        with Image.open(path) as im:
            im.verify()
        # 第二次 load 触发完整像素解码
        with Image.open(path) as im2:
            im2.load()
        return True, ""
    except Exception as e:
        return False, f"decode_error:{e}"


def _row_means(rgb: Image.Image) -> List[Tuple[float, float, float]]:
    w, h = rgb.size
    arr: List[Tuple[float, float, float]] = []
    for y in range(h):
        row = rgb.crop((0, y, w, y + 1))
        stat = ImageStat.Stat(row)
        m = stat.mean
        arr.append((float(m[0]), float(m[1]), float(m[2])))
    return arr


def _avg_rgb(rows: List[Tuple[float, float, float]]) -> Tuple[float, float, float]:
    if not rows:
        return (0.0, 0.0, 0.0)
    n = float(len(rows))
    r = sum(x[0] for x in rows) / n
    g = sum(x[1] for x in rows) / n
    b = sum(x[2] for x in rows) / n
    return (r, g, b)


def _visual_seam_suspect(path: str) -> str:
    """
    返回空字符串表示未命中；否则返回 suspect 原因。
    """
    try:
        rgb = _safe_open_rgb(path)
    except Exception:
        return ""

    w, h = rgb.size
    if (w < 32) or (h < 32):
        return ""

    # 降采样到较小宽度，提升扫描速度并降低噪声。
    target_w = min(256, w)
    if target_w != w:
        target_h = max(32, int(round(h * (target_w / float(w)))))
        try:
            resample = Image.Resampling.BILINEAR
        except Exception:
            resample = Image.BILINEAR  # type: ignore[attr-defined]
        rgb = rgb.resize((target_w, target_h), resample)
        w, h = rgb.size

    means = _row_means(rgb)
    if len(means) < 24:
        return ""

    deltas: List[float] = []
    for i in range(1, len(means)):
        a = means[i - 1]
        b = means[i]
        d = abs(a[0] - b[0]) + abs(a[1] - b[1]) + abs(a[2] - b[2])
        deltas.append(float(d))

    if len(deltas) < 12:
        return ""

    med = float(statistics.median(deltas))
    max_delta = float(max(deltas))
    if max_delta <= 0.0:
        return ""

    # 自适应阈值：兼顾自然图边缘与异常断层。
    threshold = max(96.0, med * 12.0)
    if max_delta < threshold:
        return ""

    seam_idx = int(deltas.index(max_delta) + 1)  # 行号（0-based 的下一行）

    # 检测断层前后短窗口平均色偏是否持续显著。
    pre_from = max(0, seam_idx - 6)
    pre_to = max(pre_from + 1, seam_idx - 1)
    post_from = min(len(means) - 1, seam_idx + 1)
    post_to = min(len(means), seam_idx + 7)
    if post_from >= post_to:
        return ""

    pre_mean = _avg_rgb(means[pre_from:pre_to])
    post_mean = _avg_rgb(means[post_from:post_to])
    shift = (
        abs(pre_mean[0] - post_mean[0])
        + abs(pre_mean[1] - post_mean[1])
        + abs(pre_mean[2] - post_mean[2])
    )

    # 双条件：瞬时突变 + 前后平均明显偏移，减少误报。
    if (max_delta >= threshold) and (shift >= 52.0):
        return f"suspect_visual_seam:y={seam_idx},delta={max_delta:.1f},shift={shift:.1f}"
    return ""


def _scan_one(path: str) -> ScanResult:
    stat = os.stat(path)
    mtime = datetime.fromtimestamp(float(stat.st_mtime)).isoformat(sep=" ", timespec="seconds")

    ok, reason = _decode_check(path)
    if not ok:
        return ScanResult(path=path, mtime=mtime, size_bytes=int(stat.st_size), status="broken", reason=reason)

    suspect = _visual_seam_suspect(path)
    if suspect:
        return ScanResult(path=path, mtime=mtime, size_bytes=int(stat.st_size), status="suspect", reason=suspect)

    return ScanResult(path=path, mtime=mtime, size_bytes=int(stat.st_size), status="ok", reason="")


def _write_csv(path: str, rows: List[ScanResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["status", "path", "mtime", "size_bytes", "reason"])
        for r in rows:
            writer.writerow([r.status, r.path, r.mtime, r.size_bytes, r.reason])


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan broken/suspect images in a folder")
    parser.add_argument("root_dir", help="image root directory")
    parser.add_argument("--since", default="", help="only scan files modified after this datetime (ISO format)")
    parser.add_argument("--no-recursive", action="store_true", help="scan current folder only")
    parser.add_argument("--output", default="", help="output csv path")
    parser.add_argument("--only-problems", action="store_true", help="print only broken/suspect rows")
    args = parser.parse_args()

    root = os.path.abspath(args.root_dir)
    if not os.path.isdir(root):
        print(f"[error] root_dir_not_found: {root}")
        return 2

    since_dt = _parse_since(args.since) if args.since else None
    recursive = not bool(args.no_recursive)

    paths: List[str] = []
    for p in _iter_images(root, recursive=recursive):
        if since_dt is not None:
            if _mtime_dt(p) < since_dt:
                continue
        paths.append(p)

    total = len(paths)
    print(f"[scan] root={root}")
    print(f"[scan] recursive={recursive}, total={total}, since={since_dt.isoformat(sep=' ', timespec='seconds') if since_dt else '-'}")

    results: List[ScanResult] = []
    broken = 0
    suspect = 0
    for idx, p in enumerate(paths, start=1):
        r = _scan_one(p)
        results.append(r)
        if r.status == "broken":
            broken += 1
        elif r.status == "suspect":
            suspect += 1

        if (idx % 50 == 0) or (r.status != "ok"):
            print(f"[{idx}/{total}] {r.status}: {p}")

    print("")
    print(f"[summary] total={total}, broken={broken}, suspect={suspect}, ok={total - broken - suspect}")

    problem_rows = [r for r in results if r.status in {"broken", "suspect"}]
    display_rows = problem_rows if args.only_problems else results
    if display_rows:
        print("")
        print("status,path,reason")
        for r in display_rows:
            print(f"{r.status},{r.path},{r.reason}")

    out = str(args.output or "").strip()
    if out:
        out_path = os.path.abspath(out)
        _write_csv(out_path, results)
        print("")
        print(f"[output] {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

