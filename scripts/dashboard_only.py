"""Run only the local dashboard, without starting monitors or notifications."""

import threading
import sys
from pathlib import Path

import yaml

project_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_dir))
from fomo.web.server import start_dashboard

config = yaml.safe_load((project_dir / "config.yaml").read_text(encoding="utf-8"))
server = start_dashboard(project_dir, config)
if server is None:
    raise SystemExit("Dashboard could not start")
try:
    threading.Event().wait()
finally:
    server.shutdown()
    server.server_close()
