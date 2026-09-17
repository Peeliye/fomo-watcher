"""Compatibility entry point; implementation lives in fomo.risk.engine."""

from fomo.risk.engine import *  # noqa: F401,F403
from fomo.risk.engine import main

if __name__ == "__main__":
    raise SystemExit(main())
