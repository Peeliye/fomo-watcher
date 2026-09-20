"""Transactional hourly 50Rank snapshots and UTC+8 natural-day analytics."""
from __future__ import annotations
import json, os, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

SCHEMA="""
CREATE TABLE IF NOT EXISTS cycles(cycle_date TEXT PRIMARY KEY,timezone_name TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots(snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,cycle_date TEXT NOT NULL,hour_index INTEGER NOT NULL CHECK(hour_index BETWEEN 0 AND 23),scheduled_for TEXT NOT NULL,captured_at TEXT,status TEXT NOT NULL CHECK(status IN('success','failed')),source_count INTEGER NOT NULL DEFAULT 0,error_message TEXT,FOREIGN KEY(cycle_date) REFERENCES cycles(cycle_date),UNIQUE(cycle_date,hour_index));
CREATE TABLE IF NOT EXISTS ranking_records(snapshot_id INTEGER NOT NULL,user_id TEXT NOT NULL,rank INTEGER NOT NULL,pnl_usd REAL NOT NULL,user_handle TEXT NOT NULL,display_name TEXT NOT NULL,address TEXT NOT NULL,evm_address TEXT NOT NULL,followers INTEGER NOT NULL,num_trades INTEGER NOT NULL,total_volume REAL NOT NULL,total_holdings INTEGER NOT NULL,payload_json TEXT NOT NULL,PRIMARY KEY(snapshot_id,user_id),UNIQUE(snapshot_id,rank),FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS idx_snapshots_cycle_status ON snapshots(cycle_date,status,hour_index DESC);
CREATE INDEX IF NOT EXISTS idx_records_user ON ranking_records(user_id,snapshot_id);
"""
SCHEMA_VERSION=3
def _number(v:Any)->float:
    try:
        n=float(v or 0); return n if n==n and abs(n)!=float("inf") else 0.0
    except(TypeError,ValueError): return 0.0
def _atomic_json(path:Path,payload:Any)->None:
    path.parent.mkdir(parents=True,exist_ok=True); temp=path.with_suffix(path.suffix+".tmp")
    temp.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8"); os.replace(temp,path)
def _public_entry(item:dict[str,Any],position:int,window:str)->dict[str,Any]:
    field={"24h":"pnl24h","7d":"pnl7d","30d":"pnl30d","all":"totalPnL"}[window]
    uid=str(item.get("id") or item.get("userId") or item.get("address") or item.get("evmAddress") or "").strip()
    if not uid: raise ValueError("leaderboard entry has no stable user id")
    return {"userId":uid,"rank":int(_number(item.get("rank")) or position),"pnlUsd":round(_number(item.get(field,item.get("pnlUsd"))),6),"userHandle":str(item.get("userHandle") or "unknown"),"displayName":str(item.get("displayName") or ""),"address":str(item.get("address") or ""),"evmAddress":str(item.get("evmAddress") or ""),"followers":int(_number(item.get("followers"))),"numTrades":int(_number(item.get("numTrades"))),"totalVolume":round(_number(item.get("totalVolume")),6),"totalHoldings":int(_number(item.get("totalHoldings"))),"clan":item.get("clan") if isinstance(item.get("clan"),dict) else None,"topHoldings":item.get("topHoldings") if isinstance(item.get("topHoldings"),list) else []}

class LeaderboardArchive:
    def __init__(self,database:str|Path,archive_dir:str|Path,timezone_name:str="Asia/Shanghai",readonly:bool=False):
        self.database,self.archive_dir=Path(database),Path(archive_dir); self.timezone=ZoneInfo(timezone_name)
        self.readonly=readonly
        if readonly:
            # A normal read-only connection participates in WAL visibility;
            # immutable=1 would silently ignore newly committed WAL frames.
            self.db=sqlite3.connect(f"file:{self.database.as_posix()}?mode=ro",uri=True,timeout=5)
            self.db.row_factory=sqlite3.Row;self.db.execute("PRAGMA query_only=ON")
            return
        self.database.parent.mkdir(parents=True,exist_ok=True); self.archive_dir.mkdir(parents=True,exist_ok=True)
        existed=self.database.exists() and self.database.stat().st_size>0
        self.db=sqlite3.connect(self.database,timeout=10); self.db.row_factory=sqlite3.Row
        version=int(self.db.execute("PRAGMA user_version").fetchone()[0])
        if version<SCHEMA_VERSION and existed:self._backup_before_migration(version)
        self.db.execute("PRAGMA journal_mode=WAL"); self.db.execute("PRAGMA synchronous=FULL"); self.db.execute("PRAGMA foreign_keys=ON"); self.db.execute("PRAGMA busy_timeout=10000")
        if version<SCHEMA_VERSION:
            self.db.executescript(SCHEMA);self._migrate_legacy();self.db.execute(f"PRAGMA user_version={SCHEMA_VERSION}");self.db.commit()
    def close(self)->None:self.db.close()
    def _backup_before_migration(self,version:int)->None:
        stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");directory=self.database.parent/"backups";directory.mkdir(parents=True,exist_ok=True)
        target=sqlite3.connect(directory/f"{self.database.stem}.pre-v{SCHEMA_VERSION}.from-v{version}.{stamp}.sqlite3")
        try:self.db.backup(target)
        finally:target.close()
    def _migrate_legacy(self)->None:
        tables={r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"leaderboard_runs","leaderboard_entries"}.issubset(tables):return
        runs=self.db.execute("SELECT * FROM leaderboard_runs WHERE window='24h' ORDER BY run_id").fetchall();latest={}
        for run in runs:
            parsed=datetime.fromisoformat(str(run["captured_at"]).replace("Z","+00:00"));local=(parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(self.timezone);latest[(local.strftime("%Y-%m-%d"),local.hour)]=run
        for (day,hour),run in latest.items():
            if self.db.execute("SELECT 1 FROM snapshots WHERE cycle_date=? AND hour_index=?",(day,hour)).fetchone():continue
            captured=str(run["captured_at"]);scheduled=f"{day}T{hour:02d}:01:00+08:00";self.db.execute("INSERT OR IGNORE INTO cycles VALUES(?,?,?)",(day,str(self.timezone),captured));cur=self.db.execute("INSERT INTO snapshots(cycle_date,hour_index,scheduled_for,captured_at,status,source_count)VALUES(?,?,?,?, 'success',?)",(day,hour,scheduled,captured,min(50,int(run["source_count"]))));sid=int(cur.lastrowid or 0)
            for old in self.db.execute("SELECT * FROM leaderboard_entries WHERE run_id=? ORDER BY rank",(run["run_id"],)):
                if int(old["rank"])>50:continue
                payload=json.loads(old["payload_json"]);e=_public_entry(payload,int(old["rank"]),"24h");e["pnlUsd"]=_number(old["pnl_usd"])
                self.db.execute("INSERT OR IGNORE INTO ranking_records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(sid,e["userId"],e["rank"],e["pnlUsd"],e["userHandle"],e["displayName"],e["address"],e["evmAddress"],e["followers"],e["numTrades"],e["totalVolume"],e["totalHoldings"],json.dumps(e,ensure_ascii=False,separators=(",",":"))))
        self.db.execute("DELETE FROM ranking_records WHERE rank>50");self.db.execute("UPDATE snapshots SET source_count=(SELECT COUNT(*) FROM ranking_records WHERE ranking_records.snapshot_id=snapshots.snapshot_id) WHERE status='success'");self.db.commit()
    def _slot(self,at:datetime|None)->tuple[datetime,str,int,str]:
        now=at or datetime.now(timezone.utc); now=(now if now.tzinfo else now.replace(tzinfo=timezone.utc)).astimezone(timezone.utc); local=now.astimezone(self.timezone)
        return now,local.strftime("%Y-%m-%d"),local.hour,local.replace(minute=1,second=0,microsecond=0).isoformat()
    def _entries(self,items:Iterable[dict[str,Any]],window:str)->list[dict[str,Any]]:
        result=[]; users=set(); ranks=set()
        for pos,item in enumerate(items,1):
            try:e=_public_entry(item,pos,window)
            except ValueError:continue
            if e["userId"] in users or e["rank"] in ranks:continue
            users.add(e["userId"]);ranks.add(e["rank"]);result.append(e)
        return sorted(result,key=lambda e:e["rank"])[:50]
    def capture(self,items:Iterable[dict[str,Any]],*,captured_at:datetime|None=None,window:str="24h")->dict[str,Any]:
        if window not in {"24h","7d","30d","all"}:raise ValueError("unsupported leaderboard window")
        now,day,hour,scheduled=self._slot(captured_at); entries=self._entries(items,window)
        if not entries:raise ValueError("leaderboard response contained no valid entries")
        captured=now.isoformat(); previous=self.current_ranking(day); previous_ids={x["userId"] for x in previous};previous_ranks={x["userId"]:x["rank"] for x in previous};previous_cycle=self.db.execute("SELECT cycle_date FROM snapshots WHERE status='success' ORDER BY snapshot_id DESC LIMIT 1").fetchone()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("INSERT OR IGNORE INTO cycles VALUES(?,?,?)",(day,str(self.timezone),captured))
            old=self.db.execute("SELECT * FROM snapshots WHERE cycle_date=? AND hour_index=?",(day,hour)).fetchone()
            if old is not None and old["status"]=="success":self.db.rollback();return self._summary(day,hour,int(old["snapshot_id"]),True)
            if old is None:
                cur=self.db.execute("INSERT INTO snapshots(cycle_date,hour_index,scheduled_for,captured_at,status,source_count)VALUES(?,?,?,?,?,?)",(day,hour,scheduled,captured,"success",len(entries))); sid=int(cur.lastrowid or 0)
            else:
                sid=int(old["snapshot_id"]);self.db.execute("DELETE FROM ranking_records WHERE snapshot_id=?",(sid,));self.db.execute("UPDATE snapshots SET captured_at=?,status='success',source_count=?,error_message=NULL WHERE snapshot_id=?",(captured,len(entries),sid))
            self.db.executemany("INSERT INTO ranking_records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",[(sid,e["userId"],e["rank"],e["pnlUsd"],e["userHandle"],e["displayName"],e["address"],e["evmAddress"],e["followers"],e["numTrades"],e["totalVolume"],e["totalHoldings"],json.dumps(e,ensure_ascii=False,separators=(",",":"))) for e in entries]);self.db.commit()
        except Exception:self.db.rollback();raise
        path=self.archive_dir/"hourly"/day/f"{hour:02d}01.json";_atomic_json(path,{"capturedAt":captured,"cycleDate":day,"hourIndex":hour,"window":window,"leaderboard":entries})
        finalized_day=str(previous_cycle[0]) if previous_cycle is not None and str(previous_cycle[0])!=day else None;finalized_path=str(self.export_day(finalized_day,True)) if finalized_day else None
        out=self._summary(day,hour,sid);ids={e["userId"] for e in entries};rank_changes=sum(e["userId"] in previous_ranks and previous_ranks[e["userId"]]!=e["rank"] for e in entries);out.update({"hasPrevious":bool(previous),"newCount":len(ids-previous_ids),"removedCount":len(previous_ids-ids),"rankChangedCount":rank_changes,"changed":bool(ids!=previous_ids or rank_changes),"hourlyArchive":str(path),"dailyArchive":str(self.export_day(day,False)),"finalizedDay":finalized_day,"finalizedArchive":finalized_path,"finalizedUniqueCount":len(self.participants(finalized_day)) if finalized_day else None});_atomic_json(self.archive_dir/"latest-comparison.json",out);return out
    def record_failure(self,message:str,*,captured_at:datetime|None=None)->dict[str,Any]:
        now,day,hour,scheduled=self._slot(captured_at);captured=now.isoformat();self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("INSERT OR IGNORE INTO cycles VALUES(?,?,?)",(day,str(self.timezone),captured));self.db.execute("INSERT INTO snapshots(cycle_date,hour_index,scheduled_for,captured_at,status,source_count,error_message)VALUES(?,?,?,?, 'failed',0,?) ON CONFLICT(cycle_date,hour_index) DO UPDATE SET captured_at=CASE WHEN snapshots.status='success' THEN snapshots.captured_at ELSE excluded.captured_at END,error_message=CASE WHEN snapshots.status='success' THEN snapshots.error_message ELSE excluded.error_message END",(day,hour,scheduled,captured,str(message)[:1000]));self.db.commit()
        except Exception:self.db.rollback();raise
        return {"status":"failed","cycleDate":day,"hourIndex":hour,"error":str(message)}
    def _summary(self,day:str,hour:int,sid:int,duplicate:bool=False)->dict[str,Any]:
        row=self.db.execute("SELECT * FROM snapshots WHERE snapshot_id=?",(sid,)).fetchone();unique=self.db.execute("SELECT COUNT(DISTINCT r.user_id) FROM ranking_records r JOIN snapshots s USING(snapshot_id) WHERE s.cycle_date=? AND s.status='success'",(day,)).fetchone()[0]
        return {"status":"duplicate" if duplicate else "ok","snapshotId":sid,"capturedAt":row["captured_at"],"localDay":day,"cycleDate":day,"hourIndex":hour,"sourceCount":int(row["source_count"]),"dailyUniqueCount":int(unique),"duplicate":duplicate}
    def overview(self,day:str|None=None)->dict[str,Any]:
        day=day or datetime.now(timezone.utc).astimezone(self.timezone).strftime("%Y-%m-%d");rows=self.db.execute("SELECT * FROM snapshots WHERE cycle_date=? ORDER BY hour_index",(day,)).fetchall();success=[r for r in rows if r["status"]=="success"];latest=success[-1] if success else None;unique=self.db.execute("SELECT COUNT(DISTINCT r.user_id) FROM ranking_records r JOIN snapshots s USING(snapshot_id) WHERE s.cycle_date=? AND s.status='success'",(day,)).fetchone()[0]
        return {"cycleDate":day,"timezone":str(self.timezone),"progress":f"{len(success)}/24","successfulSnapshots":len(success),"failedSnapshots":sum(r["status"]=="failed" for r in rows),"currentRanked":int(latest["source_count"]) if latest else 0,"uniqueParticipants":int(unique),"lastSnapshot":latest["captured_at"] if latest else None,"hours":[{"hourIndex":r["hour_index"],"status":r["status"],"capturedAt":r["captured_at"],"count":r["source_count"]} for r in rows]}
    def _records(self,sid:int)->list[dict[str,Any]]:return [json.loads(r["payload_json"]) for r in self.db.execute("SELECT * FROM ranking_records WHERE snapshot_id=? ORDER BY rank",(sid,))]
    def current_ranking(self,day:str|None=None)->list[dict[str,Any]]:
        day=day or datetime.now(timezone.utc).astimezone(self.timezone).strftime("%Y-%m-%d");row=self.db.execute("SELECT snapshot_id FROM snapshots WHERE cycle_date=? AND status='success' ORDER BY hour_index DESC LIMIT 1",(day,)).fetchone();return self._records(int(row[0])) if row else []
    def history(self,day:str,hour:int)->dict[str,Any]:
        row=self.db.execute("SELECT * FROM snapshots WHERE cycle_date=? AND hour_index=?",(day,int(hour))).fetchone()
        if row is None:return {"cycleDate":day,"hourIndex":int(hour),"status":"missing","records":[]}
        return {"cycleDate":day,"hourIndex":int(hour),"status":row["status"],"capturedAt":row["captured_at"],"error":row["error_message"],"records":self._records(int(row["snapshot_id"])) if row["status"]=="success" else []}
    def participants(self,day:str|None=None)->list[dict[str,Any]]:
        day=day or datetime.now(timezone.utc).astimezone(self.timezone).strftime("%Y-%m-%d")
        rows=self.db.execute("""SELECT s.snapshot_id,s.hour_index,s.captured_at,r.*,
          (SELECT MAX(snapshot_id) FROM snapshots WHERE cycle_date=? AND status='success') AS current_snapshot
          FROM snapshots s JOIN ranking_records r USING(snapshot_id)
          WHERE s.cycle_date=? AND s.status='success' ORDER BY s.hour_index,r.rank""",(day,day)).fetchall();agg={}
        for row in rows:
            x=agg.get(row["user_id"])
            if x is None:x={"userId":row["user_id"],"userHandle":row["user_handle"],"displayName":row["display_name"],"address":row["address"],"evmAddress":row["evm_address"],"firstSeen":row["captured_at"],"firstPnl":row["pnl_usd"],"firstRank":row["rank"],"maxPnl":row["pnl_usd"],"minPnl":row["pnl_usd"],"bestRank":row["rank"],"appearCount":0};agg[row["user_id"]]=x
            x.update({"lastSeen":row["captured_at"],"latestRank":row["rank"],"latestRankedPnl":row["pnl_usd"],"userHandle":row["user_handle"],"isCurrentRanked":int(row["snapshot_id"])==int(row["current_snapshot"])});x["appearCount"]+=1;x["bestRank"]=min(x["bestRank"],row["rank"]);x["maxPnl"]=max(x["maxPnl"],row["pnl_usd"]);x["minPnl"]=min(x["minPnl"],row["pnl_usd"])
        for x in agg.values():x["pnlChange"]=round(x["latestRankedPnl"]-x["firstPnl"],6);x["appearances"]=x["appearCount"];x["lastRank"]=x["latestRank"];x["lastPnlUsd"]=x["latestRankedPnl"];x["presentInLatest"]=x["isCurrentRanked"]
        return sorted(agg.values(),key=lambda x:(not x["isCurrentRanked"],x["latestRank"],-x["latestRankedPnl"]))
    def participant_detail(self,day:str,user_id:str)->dict[str,Any]:
        rows=self.db.execute("SELECT s.hour_index,s.captured_at,r.rank,r.pnl_usd FROM snapshots s LEFT JOIN ranking_records r ON r.snapshot_id=s.snapshot_id AND r.user_id=? WHERE s.cycle_date=? AND s.status='success' ORDER BY s.hour_index",(user_id,day)).fetchall();by={int(r["hour_index"]):r for r in rows}
        return {"cycleDate":day,"userId":user_id,"history":[{"hourIndex":h,"rank":by[h]["rank"] if h in by else None,"pnlUsd":by[h]["pnl_usd"] if h in by else None,"status":"ranked" if h in by and by[h]["rank"] is not None else "not_ranked"} for h in range(24)]}
    def export_day(self,day:str,finalized:bool)->Path:
        path=self.archive_dir/"daily"/f"{day}.json";members=self.participants(day);_atomic_json(path,{**self.overview(day),"uniqueMembers":len(members),"finalized":finalized,"members":members});return path
