# Feed Script API (Python)

Flask API for feed generation and validation with file-based storage backend. The API manages feed processing jobs, handles retries, and provides comprehensive audit logging.

## Features

- **Feed Generation & Validation**: Separate endpoints for generation and validation workflows
- **Job Management**: Complete job lifecycle with status tracking and retry logic
- **File-Based Storage**: JSON file storage for jobs and activities (no database required)
- **Callback Handling**: Single callback endpoint for job status updates
- **EC2 Integration**: Local script execution on same EC2 instance
- **Activity Logging**: Comprehensive audit trail for all operations

## Storage Structure

### File-Based Storage

1. **Jobs Directory** (`./jobs/`): Main job tracking files
   - Job status management (generating → generated → validating → validated)
   - Retry counting and failure tracking
   - JSON metadata storage

2. **Activities Directory** (`./activities/`): Audit trail for all operations
   - Request/response logging
   - Callback tracking
   - User/service action history

## Setup

### 1. Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Environment Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
# Edit .env with your settings
```

Required variables:
- `LOGS_DIR`: Directory for script logs
- `CALLBACK_URL`: Callback endpoint URL

## API Endpoints

### 1. Health Check
```
GET /
```
**Purpose:** API health verification
**Response:** `{"ok": true}`

### 2. Feed Validation
```
POST /api/feed/validate
```
**Purpose:** Trigger validation job
**Request:**
```json
{
  "job_id": 1,
  "affiliate_merchant_id": "AM123",
  "source_s3_path": "s3://bucket/feed.csv",
  "destination_s3_path": "s3://bucket/result.json",
  "callback_url": "http://localhost:3000/api/callback/feed",
  "partner_id": 456
}
```
**Response:** `{"message": "Validation job accepted", "job_id": "uuid"}`

### 3. Job Status Check
```
GET /api/feed/status/{job_id}
```
**Purpose:** Retrieve current job status and details
**Response:**
```json
{
  "id": "uuid",
  "affiliate_merchant_id": "AM123",
  "partner_id": "456",
  "status": "validating",
  "retry_count": 0,
  "job_data": {...},
  "created_at": "2024-01-01T00:00:00",
  "updated_at": "2024-01-01T00:00:00"
}
```

### 4. Callback Handler
```
POST /api/callback/feed
```
**Purpose:** Receive validation script results and update job status
**Request:**
```json
{
  "job_id": "uuid",
  "type": "feed_validation",
  "status": "success" | "failed",
  "result": {
    "destination_s3_path": "s3://bucket/result.json"
  },
  "err": ""
}
```
**Response:** `{"received": true}`

## Job State Machine

```
validating → validated
     ↓          ↓
validation_failed
```

**Status Flow:**
1. **`validating`** → Validation script running
2. **`validated`** → Validation completed successfully
3. **`validation_failed`** → Validation failed

## Key Features

### Retry Logic:
- Same endpoint handles both new jobs and retries
- Automatic retry count increment
- Preserves failure history

### Background Execution:
- All scripts run in daemon threads
- Non-blocking API responses
- Comprehensive error handling

### Audit Trail:
- Every API call logged as activity
- Every callback recorded
- Complete request/response history

### Local Execution:
- No SSH dependencies
- Runs on same EC2 instance
- Direct subprocess calls

## Run

```bash
export FLASK_APP=app.py
flask run --host=0.0.0.0 --port=3000
# or
python3 app.py
```

## Testing

### Manual Testing
```bash
# Start server
python3 app.py

# Test API endpoints in another terminal
curl -X POST http://localhost:3000/api/feed/generate \
  -H "Content-Type: application/json" \
  -d '{"job_id": 1, "affiliate_merchant_id": "TEST_MERCHANT_123", "source_s3_path": "s3://test-bucket/input/feed.csv", "destination_s3_path": "s3://test-bucket/output/feed.csv", "callback_url": "http://localhost:3000/api/callback/feed"}'
```

## Notes

- Jobs run in background threads
- Script execution via local subprocess (same EC2 instance)
- Comprehensive logging to files and activities
- Automatic status updates via callbacks
- Full audit trail for compliance
- No database dependencies required
