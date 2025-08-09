from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from tap_smart_open.tap import TapSmartOpen


def _load_config() -> Dict[str, Any]:
    cfg_path = Path(__file__).parent / "acceptance" / "config.json"
    return json.loads(cfg_path.read_text())


def test_discover_streams() -> None:
    cfg = _load_config()
    tap = TapSmartOpen(config=cfg)
    streams = tap.discover_streams()
    names = [s.name for s in streams]
    assert "example" in names
    # schema should be inferred and include some known fields
    s = {s.name: s for s in streams}["example"]
    schema_props = s.schema.get("properties", {})
    for key in ["id", "updated_at", "name", "amount", "active"]:
        assert key in schema_props


def test_sync_basic() -> None:
    cfg = _load_config()
    tap = TapSmartOpen(config=cfg)
    streams = {s.name: s for s in tap.discover_streams()}
    s = streams["example"]
    recs = list(s.get_records(context=None))
    # Expect all 3 rows since no incoming state is set
    assert len(recs) == 3
    # Check a couple of types
    assert isinstance(recs[0]["name"], str)
