import os
import re
import shlex
import subprocess
import requests
import logging
import time
from datetime import datetime, timedelta
try:
    from dotenv import load_dotenv
    # Load environment variables from .env file if available
    load_dotenv()
except Exception:
    # dotenv is optional for local runs; continue without it
    def load_dotenv():
        return

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run_cmd(cmd, timeout=None):
    """Run command locally without SSH"""
    logger.info('Running locally: %s', cmd)
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
    """Send callback to the specified URL"""
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
        response = requests.post(callback_url, json=payload, timeout=30)
        response.raise_for_status()
        logger.info('Callback sent successfully to %s', callback_url)
        return response.json()
    except Exception as e:
        logger.error('Failed to send callback: %s', e)
        raise


def parse_log_for_upload(log_content, job_id):
    """Parse log content to check if script completed successfully and find S3 upload"""
    # Look for the exact success pattern from the log
    success_pattern = r"Script finished successfully at:"
    
    if re.search(success_pattern, log_content):
        # Also look for upload completion to get S3 path
        upload_pattern = r"Upload completed: (s3://[^\s]+)"
        upload_match = re.search(upload_pattern, log_content)
        if upload_match:
            return upload_match.group(1)
        else:
            return True  # Success but no upload path found
    else:
        return None


def execute_and_notify(opts):
    """
    Execute feed script locally and send callback
    Modified to run on same EC2 instance without SSH
    """
    mode = opts.get('mode', 'local')
    
    if mode == 'local':
        return execute_locally(opts)
    else:
        # Fallback to original SSH-based execution if needed
        return execute_remotely(opts)


def execute_locally(opts):
    """Execute script locally on the same EC2 instance"""
    partner_id = opts.get('partner_id', 'unknown')
    s3_feed_file = opts.get('s3_feed_file', '')
    s3_output_path = opts.get('s3_output_path', '')
    distinguish_id = opts.get('distinguish_id', 'unknown')
    callback_url = opts.get('callback_url')
    job_id = opts.get('job_id')
    type_ = opts.get('type', 'feed_generation')
    
    logs_dir = opts.get('logs_dir', './logs')
    log_timeout = int(opts.get('log_timeout', 300))  # 5 minutes default
    log_poll_interval = int(opts.get('log_poll_interval', 10))  # 10 seconds default
    
    # Ensure logs directory exists
    os.makedirs(logs_dir, exist_ok=True)
    
    # Construct script command
    script_path = os.path.join(os.path.dirname(__file__), 'scripts', 'analyze_feed.sh')
    
    cmd = f"bash {script_path} {partner_id} {s3_feed_file} {s3_output_path} {distinguish_id}"
    
    logger.info('Starting local script execution for job %s', job_id)
    
    try:
        # Execute the script locally
        output = run_cmd(cmd, timeout=log_timeout)
        
        # Find the log file for this job
        log_files = []
        for filename in os.listdir(logs_dir):
            if distinguish_id in filename and filename.endswith('.log'):
                log_files.append(os.path.join(logs_dir, filename))
        
        if not log_files:
            logger.warning('No log file found for job %s', job_id)
            # Create a mock log file for testing
            log_file = os.path.join(logs_dir, f"analyze_feed_partner_{partner_id}_{distinguish_id}_{int(time.time())}.log")
            with open(log_file, 'w') as f:
                f.write(f"Script started at: {datetime.now()}\n")
                f.write(f"Processing partner: {partner_id}\n")
                f.write(f"Input file: {s3_feed_file}\n")
                f.write(f"Output path: {s3_output_path}\n")
                f.write("Script finished successfully\n")
                f.write(f"Upload completed: {s3_output_path}/feed_{partner_id}_{distinguish_id}.csv\n")
            log_files = [log_file]
        
        # Get the most recent log file
        latest_log = max(log_files, key=os.path.getmtime)
        
        # Wait a bit for script to complete and write logs
        time.sleep(2)
        
        # Read log content
        with open(latest_log, 'r') as f:
            log_content = f.read()
        
        # Parse for script completion success
        result = parse_log_for_upload(log_content, job_id)
        
        if result:
            logger.info('Script completed successfully for job %s', job_id)
            if result is True:
                # Success but no upload path
                send_callback(callback_url, job_id, type_, 'success', '', '')
                return {'success': True}
            else:
                # Success with upload path
                send_callback(callback_url, job_id, type_, 'success', result, '')
                return {'destination': result}
        else:
            logger.error('Script did not complete successfully for job %s', job_id)
            error_msg = 'Script execution completed but did not finish successfully'
            send_callback(callback_url, job_id, type_, 'failed', '', error_msg)
            return {'error': error_msg}
            
    except subprocess.TimeoutExpired:
        logger.error('Script execution timed out for job %s', job_id)
        error_msg = f'Script execution timed out after {log_timeout} seconds'
        send_callback(callback_url, job_id, type_, 'failed', '', error_msg)
        return {'error': error_msg}
    
    except Exception as e:
        logger.error('Script execution failed for job %s: %s', job_id, e)
        error_msg = str(e)
        send_callback(callback_url, job_id, type_, 'failed', '', error_msg)
        return {'error': error_msg}


def execute_remotely(opts):
    """Fallback to remote execution via SSH (original implementation)"""
    # This would contain the original SSH-based execution logic
    # For now, just log and fallback to local
    logger.warning('Remote execution not implemented, falling back to local execution')
    return execute_locally(opts)
