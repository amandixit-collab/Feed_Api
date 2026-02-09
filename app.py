import os
import time
import threading
from flask import Flask, request, jsonify
from flask_cors import CORS
from run_script_local import execute_and_notify
from dotenv import load_dotenv
from file_based_job_manager import FileBasedJobManager
from datetime import datetime

load_dotenv()

app = Flask(__name__)
CORS(app)

# Initialize job manager and job ID mapping
job_manager = FileBasedJobManager()
job_id_mapping = {}  # frontend_job_id -> backend_job_id
reverse_job_id_mapping = {}  # backend_job_id -> frontend_job_id

# Status enums (as strings)
class JobStatus:
    VALIDATING = 'validating'
    VALIDATED = 'validated'
    VALIDATION_FAILED = 'validation_failed'

class ActivitySource:
    UI = 'ui'
    CALLBACK = 'callback'

@app.route('/')
def health():
    return jsonify({'ok': True})



@app.route('/api/feed/validate', methods=['POST'])
def trigger_feed_validation():
    """API: Trigger Validation - Creates new job or retries failed job"""
    data = request.get_json(force=True)
    
    # Extract frontend job_id
    frontend_job_id = data.get('job_id')
    
    required_fields = ['job_id', 'source_s3_path', 'destination_s3_path', 'callback_url', 'affiliate_merchant_id']
    missing = [k for k in required_fields if k not in data or not data.get(k)]
    
    if missing:
        return jsonify({
            "job_id": 0,
            "type": "feed_validation",
            "status": "failed",
            "response_status": 400,
            "result": {
                "destination_s3_path": ""
            },
            "err": f'Missing fields: {missing}'
        }), 400
    
    # Validate S3 path format
    for field in ['source_s3_path', 'destination_s3_path']:
        if not data[field].startswith('s3://'):
            return jsonify({
                "job_id": 0,
                "type": "feed_validation",
                "status": "failed",
                "response_status": 400,
                "result": {
                    "destination_s3_path": ""
                },
                "err": f'{field} must start with s3://'
            }), 400
    
    # Check if frontend job_id already exists (for retry scenarios)
    backend_job_id = job_id_mapping.get(frontend_job_id)
    existing_job = job_manager.get_job(backend_job_id) if backend_job_id else None
    
    if existing_job:
        # Handle retry logic
        if existing_job['status'] == JobStatus.VALIDATION_FAILED:
            max_retry_count = int(os.getenv('MAX_RETRY_COUNT', 3))
            
            if existing_job['retry_count'] >= max_retry_count:
                return jsonify({
                    "job_id": 0,
                    "type": "feed_validation",
                    "status": "failed",
                    "response_status": 400,
                    "result": {
                        "destination_s3_path": ""
                    },
                    "err": f'Max retry count ({max_retry_count}) exceeded'
                }), 400
            
            retry_count = existing_job['retry_count'] + 1
            
            # Update job_data with new request
            job_data = existing_job['job_data'] or {}
            job_data.update({
                'validation_source_s3_path': data['source_s3_path'],
                'validation_destination_s3_path': data['destination_s3_path'],
                'validation_callback_url': data['callback_url']
            })
            
            # Update job
            job_manager.update_job(existing_job['id'], {
                'status': JobStatus.VALIDATING,
                'retry_count': retry_count,
                'job_data': job_data,
                'updated_at': datetime.utcnow().isoformat()
            })
            
            # Create activity for retry
            job_manager.create_activity(
                entity='feed_generation_job',
                entity_id=existing_job['id'],
                source=ActivitySource.UI,
                requested_by=data.get('requested_by', 'system'),
                activity_data={
                    'request_path': '/api/feed/validate',
                    'request_body': data,
                    'response': {'err': '', 'result': {'job_id': existing_job['id']}},
                    'action': 'retry'
                }
            )
            
            # Execute validation script in background with delay
            retry_delay_ms = int(os.getenv('RETRY_DELAY_MS', 300))
            time.sleep(retry_delay_ms / 1000)  # Convert to seconds
            execute_validation_script(existing_job['id'], f"{data['callback_url']}?webhook={existing_job['id']}")
            
            return jsonify({
                "job_id": frontend_job_id,
                "type": "feed_validation",
                "status": "success",
                "response_status": 200,
                "result": {
                    "destination_s3_path": data['destination_s3_path']
                },
                "err": ""
            }), 200
        else:
            return jsonify({
                "job_id": 0,
                "type": "feed_validation",
                "status": "failed",
                "response_status": 400,
                "result": {
                    "destination_s3_path": ""
                },
                "err": f'Job cannot be retried in current status: {existing_job["status"]}'
            }), 400
    else:
        # Create new validation job
        job_data = {
            'validation_source_s3_path': data['source_s3_path'],
            'validation_destination_s3_path': data['destination_s3_path'],
            'validation_callback_url': data['callback_url'],
            'failure': {}
        }
        
        # Create backend job with UUID
        backend_job_id = job_manager.create_job(
            affiliate_merchant_id=data['affiliate_merchant_id'],
            partner_id=str(data.get('partner_id', '')),
            job_data=job_data
        )
        
        # Map frontend job_id to backend job_id
        job_id_mapping[frontend_job_id] = backend_job_id
        reverse_job_id_mapping[backend_job_id] = frontend_job_id
        
        job_manager.create_activity(
            entity='feed_generation_job',
            entity_id=backend_job_id,
            source=ActivitySource.UI,
            requested_by=data.get('requested_by', 'system'),
            activity_data={
                'request_path': '/api/feed/validate',
                'request_body': data,
                'response': {'err': '', 'result': {'job_id': frontend_job_id}},
                'action': 'validation_trigger'
            }
        )
        
        # Execute validation script in background
        execute_validation_script(backend_job_id, f"{data['callback_url']}?webhook={backend_job_id}")
        
        return jsonify({
                "job_id": frontend_job_id,
                "type": "feed_validation",
                "status": "success",
                "response_status": 200,
                "result": {
                    "destination_s3_path": data['destination_s3_path']
                },
                "err": ""
            }), 200

@app.route('/api/feed/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """GET status endpoint"""
    # Check if job_id is frontend job_id, map to backend job_id
    backend_job_id = job_id_mapping.get(job_id, job_id)
    
    job = job_manager.get_job(backend_job_id)
    if not job:
        return jsonify({
            "job_id": 0,
            "type": "feed_validation",
            "status": "failed",
            "response_status": 404,
            "result": {
                "destination_s3_path": ""
            },
            "err": 'Job not found'
        }), 404
    
    # Return job status with frontend job_id
    job_response = job.copy()
    job_response['id'] = job_id  # Return frontend job_id instead of backend UUID
    
    return jsonify(job_response), 200



def execute_validation_script(job_id, callback_url):
    """Execute validation script in background (local execution)"""
    def background():
        try:
            job = job_manager.get_job(job_id)
            if not job:
                app.logger.error('Job not found for validation: %s', job_id)
                return
            
            job_data = job['job_data'] or {}
            
            opts = {
                'mode': 'local',  # Always local since we're on same EC2
                'ec2_host': None,  # Not needed for local execution
                'ec2_user': None,  # Not needed for local execution
                'key_path': None,   # Not needed for local execution
                'logs_dir': os.getenv('LOGS_DIR', './logs'),
                'run_at': None,    # Not needed for local execution
                'partner_id': job['partner_id'],
                's3_feed_file': job_data.get('validation_source_s3_path'),
                's3_output_path': job_data.get('validation_destination_s3_path'),
                'distinguish_id': f"{job_id}_validation",
                'callback_url': callback_url,
                'job_id': job_id,
                'type': 'feed_validation',
                'log_timeout': os.getenv('LOG_CHECK_TIMEOUT', '300'),
                'log_poll_interval': os.getenv('LOG_POLL_INTERVAL', '10'),
            }
            
            # Execute script
            result = execute_and_notify(opts)
            
            # Update job status based on result
            if result and isinstance(result, dict) and result.get('destination'):
                # Script succeeded, callback will update status
                app.logger.info('Validation script completed successfully for job %s', job_id)
            else:
                # Script failed, update status immediately
                job_manager.update_job(job_id, {
                    'status': JobStatus.VALIDATION_FAILED,
                    'updated_at': datetime.utcnow().isoformat()
                })
                app.logger.error('Validation script failed for job %s', job_id)
            
        except Exception as e:
            app.logger.error('Validation job error: %s', e)
            # Update job status to failed
            job_manager.update_job(job_id, {
                'status': JobStatus.VALIDATION_FAILED,
                'updated_at': datetime.utcnow().isoformat()
            })
    
    thread = threading.Thread(target=background, daemon=True)
    thread.start()


@app.route('/api/callback/feed', methods=['POST'])
def callback_receiver():
    """Callback Contract - Handles validation callbacks only"""
    payload = request.get_json(force=True)
    app.logger.info('Received callback: %s', payload)
    
    # Validate required fields
    required_fields = ['job_id', 'type', 'status']
    missing = [k for k in required_fields if k not in payload]
    if missing:
        app.logger.error('Callback missing required fields: %s', missing)
        return jsonify({
            "job_id": 0,
            "type": "feed_validation",
            "status": "failed",
            "response_status": 400,
            "result": {
                "destination_s3_path": ""
            },
            "err": f'Missing fields: {missing}'
        }), 400
    
    job_id = payload['job_id']
    callback_type = payload['type']
    status = payload['status']
    result = payload.get('result', {})
    err_msg = payload.get('err', '')
    
    # Find job
    job = job_manager.get_job(job_id)
    if not job:
        app.logger.error('Job not found for callback: %s', job_id)
        return jsonify({
            "job_id": 0,
            "type": "feed_validation",
            "status": "failed",
            "response_status": 404,
            "result": {
                "destination_s3_path": ""
            },
            "err": 'Job not found'
        }), 404
    
    # Get frontend job_id for response
    frontend_job_id = reverse_job_id_mapping.get(job_id, job_id)
    
    # Create activity entry (source = callback)
    job_manager.create_activity(
        entity='feed_generation_job',
        entity_id=job['id'],
        source=ActivitySource.CALLBACK,
        requested_by='system',
        activity_data={
            'callback_type': callback_type,
            'status': status,
            'result': result,
            'err': err_msg,
            'callback_received_at': datetime.utcnow().isoformat()
        }
    )
    
    # Update job status based on callback type and status
    job_data = job['job_data'] or {}
    
    if callback_type == 'feed_validation':
        if status == 'success':
            new_status = JobStatus.VALIDATED
            # Update destination_s3_path in job_data if provided
            if result and 'destination_s3_path' in result:
                job_data['validation_destination_s3_path'] = result['destination_s3_path']
        elif status == 'failed':
            new_status = JobStatus.VALIDATION_FAILED
            # Update failure data
            if 'failure' not in job_data:
                job_data['failure'] = {}
            if 'validation_failed' not in job_data['failure']:
                job_data['failure']['validation_failed'] = []
            
            if err_msg:
                job_data['failure']['validation_failed'].append({
                    'code': 'VALIDATION_ERROR',
                    'message': err_msg,
                    'timestamp': datetime.utcnow().isoformat()
                })
    
    job_manager.update_job(job_id, {
        'status': new_status,
        'retry_count': 0,
        'job_data': job_data,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    app.logger.info('Callback processed successfully for job %s', job_id)
    return jsonify({
        "job_id": frontend_job_id,
        "type": "feed_validation",
        "status": "success",
        "response_status": 200,
        "result": {
            "destination_s3_path": result.get('destination_s3_path', '')
        },
        "err": ""
    })


if __name__ == '__main__':
    port = int(os.getenv('PORT', 3000))
    app.run(host='0.0.0.0', port=port)
