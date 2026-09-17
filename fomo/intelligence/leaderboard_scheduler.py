"""In-process hourly 50Rank scheduler; safe across restarts via DB uniqueness."""
from __future__ import annotations
import logging, threading, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from .leaderboard import LeaderboardArchive

class LeaderboardScheduler:
    def __init__(self,project:Path,cfg:dict[str,Any],client:Any):
        settings=cfg.get("leaderboard_monitor",{});self.enabled=bool(settings.get("enabled",True));self.client=client
        db=Path(str(settings.get("database","data/leaderboard.sqlite3")));archive=Path(str(settings.get("archive_dir","data/leaderboard")))
        self.database=db if db.is_absolute() else project/db;self.archive_dir=archive if archive.is_absolute() else project/archive
        self.timezone=str(cfg.get("timezone","Asia/Shanghai"));self.window=str(settings.get("window","24h"));self.minute=int(settings.get("minute",1));self._stop=threading.Event();self._thread=None
    def start(self):
        if not self.enabled:return self
        self._thread=threading.Thread(target=self._run,name="fomo-50rank-scheduler",daemon=True);self._thread.start();return self
    def stop(self):self._stop.set()
    def _capture(self):
        endpoint="/v2/leaderboard" if self.window=="all" else f"/v2/leaderboard/{self.window}"
        store=LeaderboardArchive(self.database,self.archive_dir,self.timezone)
        try:
            try:
                response=self.client.get(endpoint);items=response.get("leaderboard",[]) if isinstance(response,dict) else []
                result=store.capture(items,window=self.window);logging.info("50Rank snapshot: %s",result)
            except Exception as exc:
                store.record_failure(str(exc));logging.exception("50Rank hourly snapshot failed")
        finally:store.close()
    def _run(self):
        last_slot=None
        while not self._stop.is_set():
            now=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo(self.timezone));slot=(now.date().isoformat(),now.hour)
            if now.minute>=self.minute and slot!=last_slot:
                self._capture();last_slot=slot
            self._stop.wait(min(20,max(2,60-now.second)))
