# Satellite Imagery Tile Processing Pipeline

A serverless pipeline for detecting tyre pyrolysis activity from satellite imagery using AWS Lambda and OpenAI vision models.

## Overview

This system ingests tile coordinates from CSV files, fetches corresponding satellite imagery from Mapbox or Google Maps APIs, and analyzes each tile using an AI vision model to detect pyrolysis activity. The pipeline is designed for high throughput with strong guarantees around idempotency, observability, and fault tolerance.

## Architecture

```
S3 (CSV) --> Ingestion Lambda --> SQS --> Tile Worker Lambda --> DynamoDB
                                              |
                                              +--> S3 (Images)
                                              +--> OpenAI Vision API
```

**Components:**

- **Ingestion Lambda** - Streams CSV from S3, auto-detects coordinate schema, dispatches tile jobs to SQS
- **Tile Worker Lambda** - Claims jobs atomically, fetches imagery, uploads to S3, runs AI analysis, records results
- **DynamoDB** - Stores run metadata and tile job state with lock-based idempotency
- **S3** - Stores source CSVs and fetched satellite imagery
- **SQS** - Decouples ingestion from processing with configurable concurrency

## Project Structure

```
aws/
├── Dockerfile
├── requirements.txt
├── scripts/
│   └── start_run.py              # Ingestion entry point
└── src/pyrolysis_aws/
    ├── core/
    │   ├── schema/               # Pydantic data models
    │   ├── config/               # Environment configuration
    │   ├── aws_clients/          # boto3 clients and secrets
    │   ├── ddb/                  # DynamoDB operations
    │   ├── imagery/              # Mapbox and Google API clients
    │   ├── openai/               # Vision model integration
    │   ├── io/                   # S3 and HTTP utilities
    │   ├── errors/               # Error code taxonomy
    │   ├── logging/              # Structured JSON logging
    │   └── pipeline/             # Core processing orchestration
    └── lambdas/
        └── tile_worker/          # Lambda handler
```

## Requirements

- Python 3.12
- AWS Account with Lambda, DynamoDB, S3, SQS, and Secrets Manager
- API keys for Mapbox, Google Maps Static API, and OpenAI

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_REGION` | AWS region | `us-east-1` |
| `S3_BUCKET` | Bucket for artifacts | Required |
| `DDB_RUNS_TABLE` | DynamoDB runs table | Required |
| `DDB_TILEJOBS_TABLE` | DynamoDB tile jobs table | Required |
| `PIPELINE_SECRETS_ID` | Secrets Manager secret ID | Required |
| `TILE_JOBS_QUEUE_URL` | SQS queue URL | Required |
| `JOB_STALE_LOCK_SECONDS` | Job lock TTL in seconds | `900` |
| `PIPELINE_MAX_RETRIES` | Maximum retry attempts | `3` |
| `PIPELINE_REQUEST_TIMEOUT` | HTTP request timeout | `10` |
| `LOG_LEVEL` | Logging level | `INFO` |

### AWS Secrets Manager

Store API keys in Secrets Manager with the following structure:

```json
{
  "MAPBOX_TOKEN": "pk.xxx",
  "GOOGLE_MAPS_API_KEY": "xxx",
  "OPENAI_API_KEY": "sk-xxx"
}
```

### DynamoDB Tables

**Runs Table** (Partition Key: `run_id`)
- Tracks run metadata, status, and tile counts

**TileJobs Table** (Partition Key: `run_id`, Sort Key: `tile_id`)
- Stores job state, coordinates, AI results, and error information

## Deployment

1. Build the container image:
   ```bash
   docker build -t pyrolysis-lambda:latest -f aws/Dockerfile aws/
   ```

2. Push to Amazon ECR and update Lambda function configuration

3. Configure environment variables and SQS event source mapping with `batch_size=1`

4. Attach IAM policy with permissions for:
   - `s3:GetObject`, `s3:PutObject`
   - `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:UpdateItem`
   - `sqs:SendMessage`, `sqs:SendMessageBatch`
   - `secretsmanager:GetSecretValue`

## Input Format

The pipeline accepts CSV files with either Mapbox tile coordinates or Google Maps coordinates.

**Mapbox format:**
```csv
z,x,y
18,12345,67890
```

**Google Maps format:**
```csv
lat,lon,zoom
40.7128,-74.0060,18
```

## Lambda Configuration

| Setting | Recommended Value |
|---------|-------------------|
| Memory | 512-1024 MB |
| Timeout | 120 seconds |
| Architecture | arm64 or x86_64 |

