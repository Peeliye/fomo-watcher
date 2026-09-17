from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from fomo.app import FomoClient
from fomo.intelligence.leaderboard import LeaderboardArchive


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture and merge one hourly Fomo leaderboard cycle")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    project = PROJECT
    cfg = yaml.safe_load((project / args.config).read_text(encoding="utf-8"))
    settings = cfg.get("leaderboard_monitor", {})
    window = str(settings.get("window", "24h"))
    endpoint = "/v2/leaderboard" if window == "all" else f"/v2/leaderboard/{window}"
    database = Path(str(settings.get("database", "data/leaderboard.sqlite3")))
    archive_dir = Path(str(settings.get("archive_dir", "data/leaderboard")))
    if not database.is_absolute():
        database = project / database
    if not archive_dir.is_absolute():
        archive_dir = project / archive_dir
    archive = LeaderboardArchive(database, archive_dir, str(cfg.get("timezone", "Asia/Shanghai")))
    try:
        try:
            response = FomoClient().get(endpoint)
            items = response.get("leaderboard", []) if isinstance(response, dict) else []
            result = archive.capture(items, window=window)
        except Exception as exc:
            result = archive.record_failure(str(exc))
    finally:
        archive.close()
    # Escaped JSON is portable across Windows Task Scheduler/GBK consoles.
    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    main()
