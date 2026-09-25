from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile

from scripts.execution_service import _sidecar_healthy


def test_one_shot_requires_fresh_connected_sidecar() -> None:
    now = datetime.now(timezone.utc)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "status.json"
        status = {"running": True, "connected": True, "authenticated": True,
                  "subscribed": True, "updatedAt": now.isoformat()}
        path.write_text(json.dumps(status), encoding="utf-8")
        assert _sidecar_healthy(path, now=now)
        status["updatedAt"] = (now - timedelta(seconds=46)).isoformat()
        path.write_text(json.dumps(status), encoding="utf-8")
        assert not _sidecar_healthy(path, now=now)
        status["updatedAt"] = now.isoformat()
        status["subscribed"] = False
        path.write_text(json.dumps(status), encoding="utf-8")
        assert not _sidecar_healthy(path, now=now)
        path.unlink()
        assert not _sidecar_healthy(path, now=now)
