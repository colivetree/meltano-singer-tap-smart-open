# tap-smart-open

A Singer tap for extracting data from files using Smart Open, supporting multiple file formats and cloud storage providers.

## Features

- **Multi-format support**: CSV, JSONL, JSON (array), Parquet
- **Multi-protocol support**: Local files, S3, GCS, Azure Blob Storage, HTTP(S)
- **Glob pattern support**: Process multiple files with wildcards
- **Automatic schema inference**: Sample-based type detection
- **Chunked processing**: Memory-efficient handling of large files
- **Incremental replication**: State-based incremental sync support
- **Cloud authentication**: Built-in support for AWS, GCP, and Azure credentials

## Installation

```bash
pip install tap-smart-open
```

Or install from source:

```bash
git clone https://github.com/your-org/tap-smart-open.git
cd tap-smart-open
pip install -e .
```

## Configuration

The tap is configured using a JSON configuration file or environment variables. Here's the configuration schema:

### Stream Configuration

Each stream represents a set of files to process:

```yaml
streams:
  - name: "my_csv_data"
    uri: "s3://my-bucket/data/*.csv"
    format: "csv"
    keys: ["id"]
    replication_method: "INCREMENTAL"
    replication_key: "updated_at"
    chunksize: 10000
    infer_samples: 1000
    csv:
      sep: ","
      header: 0
      encoding: "utf-8"
```

### Configuration Parameters

#### Stream-level Configuration

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | Yes | - | Unique name for the stream |
| `uri` | string | No* | - | Single file path/URL (supports glob patterns) |
| `uris` | array | No* | - | List of explicit file paths/URLs |
| `format` | string | No | `csv` | File format: `csv`, `jsonl`, `json`, `parquet` |
| `keys` | array | No | `[]` | Primary key field names |
| `replication_method` | string | No | `FULL_TABLE` | `FULL_TABLE` or `INCREMENTAL` |
| `replication_key` | string | No | - | Field name for incremental replication |
| `chunksize` | integer | No | `50000` | Records per chunk for CSV/JSONL |
| `infer_samples` | integer | No | `2000` | Records to sample for schema inference |
| `schema` | object | No | - | Explicit JSON Schema (skips inference) |

*Either `uri` or `uris` must be provided.

#### Format-specific Configuration

**CSV Options (`csv` object):**
```yaml
csv:
  sep: ","           # Delimiter character
  header: 0          # Header row (0=first row, null=no header)
  encoding: "utf-8"  # File encoding
  # Any pandas.read_csv() parameter
```

**JSON Options (`json` object):**
```yaml
json:
  json_path: "item"    # JSONPath for array items (default: "item")
  encoding: "utf-8"    # File encoding
```

#### Authentication Configuration

```yaml
auth:
  # AWS S3
  aws_profile: "default"
  aws_access_key_id: "AKIA..."
  aws_secret_access_key: "..."
  aws_session_token: "..."
  aws_region: "us-east-1"
  
  # GCP (uses standard environment variables)
  # Set GOOGLE_APPLICATION_CREDENTIALS or use service account
  
  # Azure (uses standard environment variables)
  # Set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY
```

#### Global Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `state_checkpoint_interval` | integer | `10000` | Records between state checkpoints |

## Usage Examples

### Local CSV Files

```yaml
streams:
  - name: "sales_data"
    uri: "./data/sales_*.csv"
    format: "csv"
    keys: ["transaction_id"]
    replication_method: "INCREMENTAL"
    replication_key: "created_at"
    csv:
      sep: ","
      header: 0
```

### S3 Parquet Files

```yaml
streams:
  - name: "events"
    uri: "s3://data-lake/events/year=2024/*/*.parquet"
    format: "parquet"
    keys: ["event_id"]
    replication_method: "FULL_TABLE"
auth:
  aws_profile: "data-pipeline"
```

### JSONL from HTTP

```yaml
streams:
  - name: "api_logs"
    uri: "https://api.example.com/logs/export.jsonl"
    format: "jsonl"
    keys: ["log_id"]
    replication_method: "INCREMENTAL"
    replication_key: "timestamp"
```

### JSON Array with Custom Path

```yaml
streams:
  - name: "users"
    uri: "gs://my-bucket/users.json"
    format: "json"
    keys: ["user_id"]
    json:
      json_path: "data.users.item"  # For nested arrays
```

### Multiple File Sources

```yaml
streams:
  - name: "combined_data"
    uris:
      - "s3://bucket1/data.csv"
      - "gs://bucket2/data.csv"
      - "./local/data.csv"
    format: "csv"
    keys: ["id"]
```

## Meltano Integration

Add to your `meltano.yml`:

```yaml
extractors:
  - name: tap-smart-open
    namespace: tap_smart_open
    pip_url: tap-smart-open
    executable: tap-smart-open
    config:
      streams:
        - name: my_data
          uri: s3://my-bucket/data/*.csv
          format: csv
          keys: ["id"]
          replication_method: INCREMENTAL
          replication_key: updated_at
      auth:
        aws_profile: default
```

Run with Meltano:

```bash
# Discovery
meltano invoke tap-smart-open --discover

# Extract and load
meltano run tap-smart-open target-jsonl
```

## Supported Protocols

| Protocol | Example | Authentication |
|----------|---------|----------------|
| Local files | `./data/file.csv` | None |
| HTTP/HTTPS | `https://example.com/data.csv` | None |
| S3 | `s3://bucket/path/file.csv` | AWS credentials |
| GCS | `gs://bucket/path/file.csv` | GCP service account |
| Azure Blob | `abfs://container/path/file.csv` | Azure storage key |

## File Format Support

### CSV
- Pandas-powered CSV parsing
- Configurable delimiters, headers, encoding
- Chunked reading for memory efficiency
- Automatic type inference

### JSONL (JSON Lines)
- One JSON object per line
- Streaming processing
- Configurable encoding

### JSON (Array)
- JSON arrays with configurable JSONPath
- Uses ijson for streaming
- Supports nested array extraction

### Parquet
- Native Parquet support via PyArrow
- Efficient columnar processing
- Automatic schema detection

## Schema Inference

The tap automatically infers JSON Schema from sample data:

- **Sampling**: Configurable number of records (`infer_samples`)
- **Type detection**: Automatic detection of strings, integers, numbers, booleans, dates
- **Date/time handling**: ISO 8601 datetime string detection
- **Nullable fields**: Automatic null type inclusion
- **Primary keys**: Automatic inclusion in schema
- **Replication keys**: Automatic inclusion with proper metadata

## Incremental Replication

For incremental sync:

1. Set `replication_method: "INCREMENTAL"`
2. Specify `replication_key` (timestamp/numeric field)
3. The tap will filter records based on the last saved state
4. State is automatically managed by the Singer framework

## Performance Considerations

- **Chunking**: Use `chunksize` for large CSV/JSONL files
- **Sampling**: Reduce `infer_samples` for faster discovery on large datasets
- **Glob patterns**: Be specific to avoid processing unnecessary files
- **Cloud storage**: Use appropriate regions and authentication for best performance

## Development

### Setup

```bash
git clone https://github.com/your-org/tap-smart-open.git
cd tap-smart-open
pip install -e ".[dev]"
```

### Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=tap_smart_open

# Run Singer SDK tests
pytest --singer
```

### Code Quality

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type checking
mypy tap_smart_open/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Changelog

### v0.1.0
- Initial release
- Support for CSV, JSONL, JSON, Parquet formats
- Multi-cloud storage support (S3, GCS, Azure)
- Automatic schema inference
- Incremental replication support
- Meltano integration
