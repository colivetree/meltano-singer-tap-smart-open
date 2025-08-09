"""Tests for tap-smart-open."""

import json
import tempfile
from pathlib import Path

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_smart_open.tap import TapSmartOpen

# Create test class using Singer SDK testing framework
TestTapSmartOpen = get_tap_test_class(
    tap_class=TapSmartOpen,
    config={
        "streams": [
            {
                "name": "test_csv",
                "uri": "tests/fixtures/sample.csv",
                "format": "csv",
                "keys": ["id"],
                "replication_method": "FULL_TABLE",
                "csv": {
                    "sep": ",",
                    "header": 0,
                    "encoding": "utf-8"
                }
            }
        ]
    }
)


class TestTapSmartOpenUnit:
    """Unit tests for tap-smart-open."""

    def test_tap_initialization(self):
        """Test tap can be initialized."""
        config = {
            "streams": [
                {
                    "name": "test",
                    "uri": "test.csv",
                    "format": "csv",
                    "keys": ["id"]
                }
            ]
        }
        tap = TapSmartOpen(config=config)
        assert tap.name == "tap-smart-open"
        assert len(tap.config["streams"]) == 1

    def test_stream_discovery(self):
        """Test stream discovery."""
        config = {
            "streams": [
                {
                    "name": "stream1",
                    "uri": "test1.csv",
                    "format": "csv",
                    "keys": ["id"]
                },
                {
                    "name": "stream2", 
                    "uri": "test2.jsonl",
                    "format": "jsonl",
                    "keys": ["id"]
                }
            ]
        }
        tap = TapSmartOpen(config=config)
        streams = tap.discover_streams()
        assert len(streams) == 2
        assert streams[0].name == "stream1"
        assert streams[1].name == "stream2"

    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        csv_content = """id,name,value,timestamp
1,Alice,100,2024-01-01T10:00:00Z
2,Bob,200,2024-01-01T11:00:00Z
3,Charlie,300,2024-01-01T12:00:00Z
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()
            yield f.name
        Path(f.name).unlink()

    @pytest.fixture
    def sample_jsonl_file(self):
        """Create a temporary JSONL file for testing."""
        jsonl_content = """{"id": 1, "name": "Alice", "value": 100, "timestamp": "2024-01-01T10:00:00Z"}
{"id": 2, "name": "Bob", "value": 200, "timestamp": "2024-01-01T11:00:00Z"}
{"id": 3, "name": "Charlie", "value": 300, "timestamp": "2024-01-01T12:00:00Z"}
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(jsonl_content)
            f.flush()
            yield f.name
        Path(f.name).unlink()

    def test_csv_stream_processing(self, sample_csv_file):
        """Test CSV stream processing."""
        config = {
            "streams": [
                {
                    "name": "test_csv",
                    "uri": sample_csv_file,
                    "format": "csv",
                    "keys": ["id"],
                    "replication_method": "FULL_TABLE",
                    "csv": {
                        "sep": ",",
                        "header": 0,
                        "encoding": "utf-8"
                    }
                }
            ]
        }
        tap = TapSmartOpen(config=config)
        streams = tap.discover_streams()
        assert len(streams) == 1
        
        stream = streams[0]
        records = list(stream.get_records(None))
        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["value"] == 100

    def test_jsonl_stream_processing(self, sample_jsonl_file):
        """Test JSONL stream processing."""
        config = {
            "streams": [
                {
                    "name": "test_jsonl",
                    "uri": sample_jsonl_file,
                    "format": "jsonl",
                    "keys": ["id"],
                    "replication_method": "FULL_TABLE"
                }
            ]
        }
        tap = TapSmartOpen(config=config)
        streams = tap.discover_streams()
        assert len(streams) == 1
        
        stream = streams[0]
        records = list(stream.get_records(None))
        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["value"] == 100

    def test_schema_inference(self, sample_csv_file):
        """Test schema inference."""
        config = {
            "streams": [
                {
                    "name": "test_csv",
                    "uri": sample_csv_file,
                    "format": "csv",
                    "keys": ["id"],
                    "infer_samples": 10,
                    "csv": {
                        "sep": ",",
                        "header": 0,
                        "encoding": "utf-8"
                    }
                }
            ]
        }
        tap = TapSmartOpen(config=config)
        streams = tap.discover_streams()
        stream = streams[0]
        
        schema = stream.schema
        assert schema["type"] == "object"
        assert "properties" in schema
        
        properties = schema["properties"]
        assert "id" in properties
        assert "name" in properties
        assert "value" in properties
        assert "timestamp" in properties
        
        # Check inferred types
        assert "integer" in properties["id"]["type"]
        assert "string" in properties["name"]["type"]
        assert "integer" in properties["value"]["type"]
        assert properties["timestamp"]["format"] == "date-time"

    def test_explicit_schema_override(self, sample_csv_file):
        """Test explicit schema override."""
        explicit_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["string"]},
                "name": {"type": ["string"]},
                "custom_field": {"type": ["string"]}
            }
        }
        
        config = {
            "streams": [
                {
                    "name": "test_csv",
                    "uri": sample_csv_file,
                    "format": "csv",
                    "keys": ["id"],
                    "schema": explicit_schema,
                    "csv": {
                        "sep": ",",
                        "header": 0,
                        "encoding": "utf-8"
                    }
                }
            ]
        }
        tap = TapSmartOpen(config=config)
        streams = tap.discover_streams()
        stream = streams[0]
        
        schema = stream.schema
        assert schema == explicit_schema