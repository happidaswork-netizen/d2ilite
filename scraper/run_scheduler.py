import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler


def parse_time_window(value: str) -> tuple[int, int]:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError("time must be HH:MM")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("time out of range")
    return hour, minute


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Schedule public scraper at a fixed local time.")
    parser.add_argument("--config", required=True, help="Path to scraper config JSON.")
    parser.add_argument(
        "--time",
        default="02:30",
        help="Local run time in HH:MM, default 02:30.",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Pass through to run_public_scraper.py.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    hour, minute = parse_time_window(args.time)

    script_dir = Path(__file__).resolve().parent
    runner_path = script_dir / "run_public_scraper.py"

    def job() -> None:
        cmd = [sys.executable, str(runner_path), "--config", args.config]
        if args.skip_images:
            cmd.append("--skip-images")
        print(f"[{datetime.now().isoformat(timespec='seconds')}] run: {' '.join(cmd)}")
        subprocess.run(cmd, check=False)

    scheduler = BlockingScheduler()
    scheduler.add_job(job, "cron", hour=hour, minute=minute, id="public_scraper_daily")

    print(f"Scheduler started, daily at {hour:02d}:{minute:02d}")
    print("Press Ctrl+C to stop")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("Scheduler stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
