# Feed Script API (Python)

Small Flask API that copies a bash analysis script to an EC2 host, runs it, parses the output for the uploaded S3 path, and POSTs a callback payload to a configured URL.

Setup

1. Create a virtualenv and install requirements:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy `.env.example` to `.env` and set `EC2_HOST`, `EC2_KEY_PATH`, `CALLBACK_URL`.

Run

```bash
export FLASK_APP=app.py
flask run --host=0.0.0.0 --port=3000
# or
python app.py
```

Trigger a job

POST to `/api/run-feed` with JSON body:

```json
{
  "partner_id": "partner1",
  "s3_feed_file": "s3://bucket/path/feed.xml.gz",
  "s3_output_path": "s3://bucket/path/output",
  "distinguish_id": "run1",
  "ec2_host": "optional-if-not-in-env",
  "ec2_user": "optional",
  "ec2_key_path": "optional-if-not-in-env",
  "callback_url": "optional-if-not-in-env",
  "job_id": 1,
  "type": "feed_generation"
}
```

Notes

- The API runs the job in a background thread and returns 202 Accepted.
- The server uses `scp` and `ssh` to transfer and execute the script; ensure the host accepts key auth and the key is accessible.
- The script logs to `./logs` on the remote host; this service looks for a line matching `Upload completed: s3://...` to populate the callback `destination_s3_path`.
