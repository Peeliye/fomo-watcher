from __future__ import annotations

import pytest
import json
from pathlib import Path

from fomo.signals.freshness import decide_freshness


@pytest.mark.parametrize("source,observed,consume,accepted,upstream,queue,skew",
                         json.loads((Path(__file__).parent / "fixtures" / "freshness-v1.json")
                                    .read_text(encoding="utf-8")))
def test_upstream_is_independent_of_local_execution(source, observed, consume,
                                                     accepted, upstream, queue, skew):
    result = decide_freshness(source, observed, consume, 5_000)
    assert (result.accepted, result.upstream_delay_ms, result.local_queue_delay_ms,
            result.clock_skew_ms) == (accepted, upstream, queue, skew)
