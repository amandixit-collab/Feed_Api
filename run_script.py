import os
import re
import shlex
import subprocess
import requests
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run_cmd(cmd, timeout=None):
    logger.info('Running: %s', cmd)
    completed = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    logger.info('Returncode: %s', completed.returncode)
    if completed.stdout:
        logger.info('stdout:\n%s', completed.stdout)
    if completed.stderr:
        logger.info('stderr:\n%s', completed.stderr)
    if completed.returncode != 0:
        raise RuntimeError(f'Command failed ({completed.returncode}): {cmd}\n{completed.stderr}')
    return completed.stdout


def send_callback(callback_url, job_id, type_, status, destination_s3_path, err_msg):
    if not callback_url:
        logger.warning('No callback URL configured; skipping callback')
        return

    payload = {
        'job_id': job_id or 0,
        'type': type_ or 'feed_generation',
        'status': status,
        'result': {
            'destination_s3_path': destination_s3_path or ''
        },
        'err': err_msg or ''
    }

    try:
        logger.info('Posting callback to %s with payload %s', callback_url, payload)
        requests.post(callback_url, json=payload, timeout=30)
    except Exception as e:
        logger.exception('Failed to POST callback: %s', e)


def execute_and_notify(opts):
    ec2_host = opts.get('ec2_host')
    ec2_user = opts.get('ec2_user', 'ec2-user')
    key_path = opts.get('key_path')
    # Allow scheduling via opts or environment variable RUN_AT (format: HH:MM or HH:MM:SS)
    run_at = opts.get('run_at') or os.getenv('RUN_AT')
    partner_id = opts.get('partner_id')
    s3_feed_file = opts.get('s3_feed_file')
    s3_output_path = opts.get('s3_output_path')
    distinguish_id = opts.get('distinguish_id')
    callback_url = opts.get('callback_url')
    job_id = opts.get('job_id', 0)
    type_ = opts.get('type', 'feed_generation')

    # Support a mock mode for tests where no real SSH/SCP is available
    if opts.get('mock'):
        stdout = ""
        stdout += "✅ Download completed: /tmp/mock_feed.xml.gz\n"
        stdout += "✅ Stats file created: /tmp/mock_stats.txt\n"
        stdout += "☁️ Uploading stats to S3...\n"
        stdout += "✅ Upload completed: s3://mock-bucket/mock_path/mock_stats.txt\n"
        destination = 's3://mock-bucket/mock_path/mock_stats.txt'
        send_callback(callback_url, job_id, type_, 'success', destination, '')
        return {'stdout': stdout, 'destination': destination}

    print(ec2_host, key_path)

    if not ec2_host or not key_path:
        err = 'Missing ec2_host or key_path'
        send_callback(callback_url, job_id, type_, 'failed', None, err)
        raise RuntimeError(err)

    # If a run_at time is provided, sleep until that scheduled time
    if run_at:
        try:
            now = datetime.now()
            parts = [int(p) for p in run_at.split(':')]
            if len(parts) == 2:
                target = now.replace(hour=parts[0], minute=parts[1], second=0, microsecond=0)
            else:
                target = now.replace(hour=parts[0], minute=parts[1], second=parts[2], microsecond=0)
            if target < now:
                # schedule for next day
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            logger.info('Waiting %.0f seconds until scheduled run_at=%s (target=%s)', wait_seconds, run_at, target)
            time.sleep(wait_seconds)
        except Exception:
            logger.exception('Failed to parse or wait for RUN_AT; proceeding immediately')

    local_script = os.path.join(os.path.dirname(__file__), 'scripts', 'analyze_feed.sh')
    remote_script = f'/tmp/analyze_feed_{partner_id}_{int(os.times()[4])}.sh'

    try:
        scp_cmd = f"scp -i {shlex.quote(key_path)} -o StrictHostKeyChecking=no {shlex.quote(local_script)} {shlex.quote(ec2_user)}@{shlex.quote(ec2_host)}:{shlex.quote(remote_script)}"
        run_cmd(scp_cmd)

        remote_cmd = (
            f"chmod +x {shlex.quote(remote_script)} && bash {shlex.quote(remote_script)} "
            f"{shlex.quote(str(partner_id))} {shlex.quote(s3_feed_file)} {shlex.quote(s3_output_path)} {shlex.quote(str(distinguish_id))}"
        )
        ssh_run = (
            f"ssh -i {shlex.quote(key_path)} -o StrictHostKeyChecking=no "
            f"{shlex.quote(ec2_user)}@{shlex.quote(ec2_host)} '{remote_cmd}'"
        )

        stdout = run_cmd(ssh_run, timeout=60 * 30)

        # Try to find S3 path from output line like: Upload completed: s3://bucket/path
        m = re.search(r"Upload completed:\s*(s3://\S+)", stdout, re.IGNORECASE)
        destination = m.group(1) if m else None

        # After remote execution, fetch the latest log file from the EC2 logs directory
        logs_dir = f"/home/{ec2_user}/Feed_API/Feed_API/scripts/logs"
        # Build remote command safely and quote it for the ssh invocation
        remote_list_cmd = f"ls -t {logs_dir}/* 2>/dev/null | head -n1"
        latest_path_cmd = (
            f"ssh -i {shlex.quote(key_path)} -o StrictHostKeyChecking=no "
            f"{shlex.quote(ec2_user)}@{shlex.quote(ec2_host)} {shlex.quote(remote_list_cmd)}"
        )
        # Poll the latest log until success marker appears or timeout elapses
        timeout_secs = int(opts.get('log_timeout') or os.getenv('LOG_CHECK_TIMEOUT') or 300)
        poll_interval = int(opts.get('log_poll_interval') or os.getenv('LOG_POLL_INTERVAL') or 5)
        deadline = time.time() + timeout_secs
        last_log_tail = ''
        last_latest = None

        while time.time() < deadline:
            try:
                latest_path = run_cmd(latest_path_cmd).strip()
            except Exception:
                latest_path = ''

            if not latest_path:
                logger.info('No log file found yet in %s; will retry', logs_dir)
                time.sleep(poll_interval)
                continue

            # only fetch tail if latest_path changed or on first iteration
            if latest_path != last_latest:
                last_latest = latest_path

            try:
                remote_cat_cmd = f"tail -n 200 {shlex.quote(latest_path)}"
                cat_cmd = (
                    f"ssh -i {shlex.quote(key_path)} -o StrictHostKeyChecking=no "
                    f"{shlex.quote(ec2_user)}@{shlex.quote(ec2_host)} {shlex.quote(remote_cat_cmd)}"
                )
                log_content = run_cmd(cat_cmd)
                last_log_tail = log_content
            except Exception:
                logger.exception('Failed to read latest log; will retry')
                time.sleep(poll_interval)
                continue

            if re.search(r"Script finished successfully", last_log_tail):
                send_callback(callback_url, job_id, type_, 'success', destination, '')
                return {'stdout': stdout, 'destination': destination, 'log': latest_path}

            logger.info('Success marker not found yet in %s; retrying until timeout', latest_path)
            time.sleep(poll_interval)

        # Timeout reached, return failed with last tail
        err_msg = 'Timeout waiting for success marker. Latest log tail:\n' + last_log_tail
        logger.error(err_msg)
        send_callback(callback_url, job_id, type_, 'failed', destination, err_msg)
        return {'stdout': stdout, 'destination': destination, 'log': last_latest}

    except Exception as e:
        logger.exception('Execution failed')
        send_callback(callback_url, job_id, type_, 'failed', None, str(e))
        raise
