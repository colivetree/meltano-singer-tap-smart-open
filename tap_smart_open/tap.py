from __future__ import annotations

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from .streams import SmartOpenStream


class TapSmartOpen(Tap):
    name = "tap-smart-open"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "streams",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("uri", th.StringType, description="Single URI, optionally with glob pattern."),
                    th.Property(
                        "uris",
                        th.ArrayType(th.StringType),
                        description="Explicit list of URIs; overrides 'uri' if provided.",
                    ),
                    th.Property(
                        "format",
                        th.StringType,
                        description="One of: csv, jsonl, json, parquet",
                        default="csv",
                        allowed_values=["csv", "jsonl", "json", "parquet"],
                    ),
                    th.Property(
                        "keys",
                        th.ArrayType(th.StringType),
                        description="Primary key fields.",
                        default=[],
                    ),
                    th.Property(
                        "replication_method",
                        th.StringType,
                        allowed_values=["FULL_TABLE", "INCREMENTAL"],
                        default="FULL_TABLE",
                    ),
                    th.Property("replication_key", th.StringType),
                    th.Property(
                        "chunksize",
                        th.IntegerType,
                        description="Chunk size for CSV/JSONL reading.",
                        default=50000,
                    ),
                    th.Property(
                        "infer_samples",
                        th.IntegerType,
                        description="Number of rows/items to sample during schema inference.",
                        default=2000,
                    ),
                    th.Property(
                        "schema",
                        th.ObjectType(additional_properties=True),
                        description="Explicit JSON Schema override (skip inference).",
                    ),
                    th.Property(
                        "csv",
                        th.ObjectType(
                            additional_properties=True,
                        ),
                    ),
                    th.Property(
                        "json",
                        th.ObjectType(
                            additional_properties=True,
                        ),
                    ),
                )
            ),
            required=True,
        ),
        th.Property(
            "auth",
            th.ObjectType(
                th.Property("aws_profile", th.StringType),
                th.Property("aws_access_key_id", th.StringType),
                th.Property("aws_secret_access_key", th.StringType),
                th.Property("aws_session_token", th.StringType),
                th.Property("aws_region", th.StringType),
            ),
            description="Authentication hints. Providers also respect standard environment credentials.",
        ),
        th.Property(
            "state_checkpoint_interval",
            th.IntegerType,
            description="Records per state checkpoint emission.",
            default=10000,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        streams: List[Stream] = []
        for s_cfg in self.config.get("streams", []):
            streams.append(SmartOpenStream(tap=self, stream_config=s_cfg))
        return streams


if __name__ == "__main__":
    TapSmartOpen.cli()
