from __future__ import annotations

import json
import math
import re
from datetime import datetime
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional

import fsspec
import ijson
import pandas as pd
from dateutil import parser as dateparser
from singer_sdk import Stream
# JSONSchema import removed - not needed for this implementation
from smart_open import open as smart_open


def _looks_like_glob(uri: str) -> bool:
    return any(ch in uri for ch in ["*", "?", "[", "]"])


def _is_iso_datetime(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        dateparser.isoparse(value)
        return True
    except Exception:
        return False


def _coerce_replication_value(value: Any) -> Any:
    # Try datetime first, then numeric, then string
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, str):
        try:
            return dateparser.isoparse(value)
        except Exception:
            # try numeric
            try:
                if "." in value:
                    return float(value)
                return int(value)
            except Exception:
                return value
    return value


def _merge_types(types: set[str], new_type: str) -> set[str]:
    # prefer 'number' over 'integer' if both present
    if new_type == "number" and "integer" in types:
        types.discard("integer")
    types.add(new_type)
    return types


def _infer_type_from_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, str):
        if _is_iso_datetime(value):
            return "date-time"
        return "string"
    return "string"


class SmartOpenStream(Stream):
    """Generic stream reading via Smart Open + fsspec across formats."""

    def __init__(self, tap, stream_config: Dict[str, Any]) -> None:
        self.stream_config = stream_config
        self.tap = tap  # Store tap reference first
        name = stream_config["name"]
        
        # Store stream config BEFORE calling super().__init__() because schema property is accessed during init
        self._stream_replication_method = stream_config.get("replication_method", "FULL_TABLE")
        self._stream_replication_key = stream_config.get("replication_key")
        self._stream_primary_keys = stream_config.get("keys", [])
        self._checkpoint_interval = int(
            tap.config.get("state_checkpoint_interval", 10000)
        )
        
        # Cache resolved paths to avoid repeated SFTP connections
        self._cached_paths: Optional[List[str]] = None
        # Cache file contents to avoid repeated SFTP file reads
        self._cached_file_contents: Dict[str, bytes] = {}
        
        super().__init__(tap=tap, name=name)

    @property
    def primary_keys(self) -> List[str]:
        return self._stream_primary_keys

    @property
    def replication_key(self) -> Optional[str]:
        return self._stream_replication_key

    @property
    def replication_method(self) -> str:
        return self._stream_replication_method

    @property
    def schema(self) -> Dict[str, Any]:
        explicit = self.stream_config.get("schema")
        if explicit:
            return explicit
        return self._infer_schema()

    # --------------- Schema inference ---------------

    def _infer_schema(self) -> Dict[str, Any]:
        limit = int(self.stream_config.get("infer_samples", 2000))
        samples: List[Dict[str, Any]] = []
        for rec in self._iter_records_raw(limit=limit):
            samples.append(rec)
            if len(samples) >= limit:
                break

        properties: Dict[str, Any] = {}
        type_map: Dict[str, set[str]] = {}

        for rec in samples:
            for k, v in rec.items():
                t = _infer_type_from_value(v)
                type_map.setdefault(k, set())
                type_map[k] = _merge_types(type_map[k], t)

        for k, types in type_map.items():
            # handle date-time (string with format)
            if "date-time" in types:
                if "null" in types:
                    properties[k] = {"type": ["string", "null"], "format": "date-time"}
                else:
                    properties[k] = {"type": "string", "format": "date-time"}
                continue

            # general case
            if "integer" in types and "number" in types:
                types.discard("integer")
            if len(types) == 0:
                properties[k] = {"type": ["string", "null"]}
            else:
                # If null is in types, keep as array; otherwise use single type
                if "null" in types:
                    non_null_types = [t for t in sorted(types) if t != "null"]
                    if len(non_null_types) == 1:
                        properties[k] = {"type": [non_null_types[0], "null"]}
                    else:
                        properties[k] = {"type": sorted(list(types))}
                else:
                    # No null values seen - make it required (single type)
                    if len(types) == 1:
                        properties[k] = {"type": list(types)[0]}
                    else:
                        properties[k] = {"type": sorted(list(types))}

        # Ensure replication and keys are in schema
        if self.replication_key and self.replication_key not in properties:
            if self.replication_key == "_sdc_extracted_at":
                # _sdc_extracted_at is always added by the tap, so it's optional in schema
                # but will always be present in records
                properties[self.replication_key] = {
                    "type": ["string", "null"],
                    "format": "date-time"
                }
            else:
                properties[self.replication_key] = {"type": ["string", "null"]}
        for pk in self.primary_keys:
            if pk not in properties:
                properties[pk] = {"type": ["string", "null"]}

        # Build required fields list - fields without null in their type
        # Exclude _sdc_extracted_at from required fields since it's added by the tap
        required_fields = []
        for field_name, field_schema in properties.items():
            if field_name == "_sdc_extracted_at":
                continue  # Skip _sdc_extracted_at - it's added by tap, not required in schema
            field_type = field_schema.get("type")
            if isinstance(field_type, str):
                # Single type means required
                required_fields.append(field_name)
            elif isinstance(field_type, list) and "null" not in field_type:
                # Array of types without null means required
                required_fields.append(field_name)

        schema = {
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }
        
        if required_fields:
            schema["required"] = required_fields
            
        return schema

    # --------------- File iteration ---------------

    def _storage_options(self, uri: str) -> Dict[str, Any]:
        auth = self.tap.config.get("auth") or {}
        scheme = uri.split(":", 1)[0].lower()
        opts: Dict[str, Any] = {}
        if scheme == "s3":
            if auth.get("aws_profile"):
                opts["profile"] = auth["aws_profile"]
            if auth.get("aws_access_key_id"):
                opts["key"] = auth["aws_access_key_id"]
            if auth.get("aws_secret_access_key"):
                opts["secret"] = auth["aws_secret_access_key"]
            if auth.get("aws_session_token"):
                opts["token"] = auth["aws_session_token"]
            if auth.get("aws_region"):
                opts.setdefault("client_kwargs", {})["region_name"] = auth["aws_region"]
        elif scheme == "sftp":
            # Parse SFTP URI to extract connection details
            # Format: sftp://user:pass@host:port/path
            import urllib.parse
            parsed = urllib.parse.urlparse(uri)
            if parsed.hostname:
                opts["host"] = parsed.hostname
            if parsed.port:
                opts["port"] = parsed.port
            if parsed.username:
                opts["username"] = parsed.username
            if parsed.password:
                # URL decode the password to handle special characters
                opts["password"] = urllib.parse.unquote(parsed.password)
        # Other providers typically rely on their own env setup.
        return opts

    def _iter_paths(self) -> List[str]:
        # Return cached paths if available
        if self._cached_paths is not None:
            return self._cached_paths
            
        uris = self.stream_config.get("uris")
        if uris:
            self._cached_paths = list(uris)
            return self._cached_paths

        uri = self.stream_config.get("uri")
        if not uri:
            raise ValueError(f"Stream '{self.name}' missing 'uri' or 'uris'")

        # Check if we have a pattern to filter files
        pattern = self.stream_config.get("pattern")
        
        if _looks_like_glob(uri):
            files = fsspec.open_files(uri, mode="rb", **self._storage_options(uri))
            # Use .path attribute for stable, deterministic iteration
            paths = sorted([f.path for f in files])  # type: ignore[attr-defined]
        else:
            # If no glob pattern in URI but we have a pattern config, treat URI as directory
            if pattern:
                # Ensure URI ends with / for directory listing
                if not uri.endswith('/'):
                    uri += '/'
                # Use fsspec to list files in directory (SFTP-aware)
                try:
                    import urllib.parse
                    parsed = urllib.parse.urlparse(uri)
                    scheme = parsed.scheme
                    netloc = parsed.netloc
                    remote_dir = parsed.path or "/"
                    if remote_dir.endswith("/"):
                        remote_dir = remote_dir[:-1]
                    # Instantiate FS using connection opts (host/port/username/password, already parsed in _storage_options)
                    _fs = fsspec.filesystem(scheme, **self._storage_options(uri))
                    # List with detail=True so we can use absolute names directly
                    all_files_info = _fs.listdir(remote_dir, detail=True)
                    self.logger.info(f"Raw listdir result for {remote_dir}: {all_files_info}")
                    # Extract absolute paths of files
                    if isinstance(all_files_info, list) and all_files_info and isinstance(all_files_info[0], dict):
                        names = [info["name"] for info in all_files_info if info.get("type") == "file"]
                    else:
                        # Fallback: simple list of basenames; make them absolute under remote_dir
                        basenames = _fs.listdir(remote_dir)
                        names = [f"{remote_dir}/{bn}" for bn in basenames if bn]
                    # Ensure leading slash for each path
                    names = [n if n.startswith("/") else f"/{n}" for n in names]
                    # Reconstruct full URIs: sftp://user@host:port + absolute path
                    protocol = f"{scheme}://"
                    full_netloc = netloc  # already includes user:pass@host:port per config
                    paths = [f"{protocol}{full_netloc}{n}" for n in names]
                    self.logger.info(f"Found {len(paths)} files in directory {uri}: {paths}")
                except Exception as e:
                    self.logger.warning(f"Failed to list directory {uri}: {e}")
                    import traceback
                    self.logger.warning(f"Traceback: {traceback.format_exc()}")
                    paths = [uri]
            else:
                paths = [uri]
        
        # Apply regex pattern filter if specified
        if pattern:
            regex = re.compile(pattern)
            filtered_paths = []
            self.logger.info(f"Applying pattern filter '{pattern}' to {len(paths)} files")
            for path in paths:
                # Extract filename from path for pattern matching
                filename = path.split('/')[-1]
                if regex.search(filename):
                    filtered_paths.append(path)
                    self.logger.info(f"File '{filename}' matches pattern")
                else:
                    self.logger.info(f"File '{filename}' does not match pattern")
            paths = filtered_paths
            self.logger.info(f"After pattern filtering: {len(paths)} files remain")
        
        # Cache the resolved paths to avoid repeated SFTP connections
        self._cached_paths = paths
        return paths

    # --------------- Record iterators ---------------

    def _iter_csv(self, path: str) -> Iterator[Dict[str, Any]]:
        opts = dict(self.stream_config.get("csv") or {})
        chunksize = int(self.stream_config.get("chunksize", 50000))
        
        # Convert boolean header values to pandas-compatible format
        if "header" in opts:
            if opts["header"] is True:
                opts["header"] = 0  # First row as header
            elif opts["header"] is False:
                opts["header"] = None  # No header

        # Check cache first to avoid repeated SFTP file reads
        if path in self._cached_file_contents:
            self.logger.info(f"Using cached content for CSV file: {path}")
            file_content = self._cached_file_contents[path]
        else:
            # Read entire file content and cache it
            self.logger.info(f"Reading and caching CSV file: {path}")
            with smart_open(path, "rb") as fh:
                file_content = fh.read()
            self._cached_file_contents[path] = file_content
        
        # Process content in memory using StringIO to avoid multiple connections
        import io
        if chunksize and chunksize > 0:
            # Use pandas chunked reading on in-memory content
            for chunk in pd.read_csv(io.BytesIO(file_content), chunksize=chunksize, **opts):  # type: ignore[arg-type]
                for rec in chunk.to_dict(orient="records"):
                    # Sanitize non-finite float values to None for JSON compliance
                    sanitized_rec = {}
                    for k, v in rec.items():
                        if isinstance(v, float) and (pd.isna(v) or not math.isfinite(v)):
                            sanitized_rec[k] = None
                        else:
                            sanitized_rec[k] = v
                    yield sanitized_rec
        else:
            # Read entire file at once from in-memory content
            df = pd.read_csv(io.BytesIO(file_content), **opts)  # type: ignore[arg-type]
            for rec in df.to_dict(orient="records"):
                # Sanitize non-finite float values to None for JSON compliance
                sanitized_rec = {}
                for k, v in rec.items():
                    if isinstance(v, float) and (pd.isna(v) or not math.isfinite(v)):
                        sanitized_rec[k] = None
                    else:
                        sanitized_rec[k] = v
                yield sanitized_rec

    def _iter_jsonl(self, path: str) -> Iterator[Dict[str, Any]]:
        # JSON Lines, one JSON per line
        encoding = (self.stream_config.get("json") or {}).get("encoding", "utf-8")
        with smart_open(path, "r", encoding=encoding) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

    def _iter_json_array(self, path: str) -> Iterator[Dict[str, Any]]:
        # Stream array items with ijson
        # json_path is prefix to the items, default "item" for array at root
        json_opts = self.stream_config.get("json") or {}
        json_path = json_opts.get("json_path", "item")
        encoding = json_opts.get("encoding", "utf-8")
        with smart_open(path, "r", encoding=encoding) as fh:
            for obj in ijson.items(fh, json_path):
                if isinstance(obj, dict):
                    yield obj
                else:
                    yield {"value": obj}

    def _iter_parquet(self, path: str) -> Iterator[Dict[str, Any]]:
        # Pandas can read remote parquet paths via fsspec with storage_options
        storage_opts = self._storage_options(path)
        df = pd.read_parquet(path, storage_options=storage_opts or None)  # type: ignore[arg-type]
        for rec in df.to_dict(orient="records"):
            yield rec

    def _iter_records_raw(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        fmt = self.stream_config.get("format", "csv").lower()
        count = 0
        for p in self._iter_paths():
            if fmt == "csv":
                iterator = self._iter_csv(p)
            elif fmt == "jsonl":
                iterator = self._iter_jsonl(p)
            elif fmt == "json":
                iterator = self._iter_json_array(p)
            elif fmt == "parquet":
                iterator = self._iter_parquet(p)
            else:
                raise ValueError(f"Unsupported format '{fmt}' for stream '{self.name}'")

            for rec in iterator:
                yield rec
                count += 1
                if limit is not None and count >= limit:
                    return

    # --------------- Singer-SDK integration ---------------

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        start_val = None
        if self.replication_key:
            start_val = self.get_starting_replication_key_value(context)

        sent = 0
        extraction_time = datetime.utcnow().isoformat() + "Z"
        
        for rec in self._iter_records_raw():
            # Add extraction timestamp if using _sdc_extracted_at as replication key
            if self.replication_key == "_sdc_extracted_at":
                rec["_sdc_extracted_at"] = extraction_time
            
            # Incremental filtering
            if self.replication_key and start_val is not None:
                rv = _coerce_replication_value(rec.get(self.replication_key))
                if rv is None:
                    # If missing, allow through to not stall future progress
                    pass
                else:
                    # If both datetime, or both numeric/string, rely on python comparison
                    if isinstance(start_val, str):
                        try:
                            start_val_cmp = dateparser.isoparse(start_val)
                        except Exception:
                            start_val_cmp = start_val
                    else:
                        start_val_cmp = start_val
                    if rv <= start_val_cmp:  # type: ignore[operator]
                        continue

            yield rec

            sent += 1
            if self._checkpoint_interval and sent % self._checkpoint_interval == 0:
                # The SDK manages state writes; yielding records with replication_key
                # is sufficient. Explicit state writes are typically not needed here.
                pass
