import os
import threading
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from run_script import execute_and_notify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

@app.route('/')
def health():
    # serve simple UI at /ui; keep health check here
    return jsonify({'ok': True})


@app.route('/ui')
def ui():
    return send_from_directory('static', 'index.html')

@app.route('/api/run-feed', methods=['POST'])
def run_feed():
    data = request.get_json(force=True)
    required = ['partner_id', 's3_feed_file', 's3_output_path', 'distinguish_id']
    missing = [k for k in required if k not in data or not data.get(k)]




    if missing:
        return jsonify({'error': f'Missing fields: {missing}'}), 400

    opts = {
        'ec2_host': os.getenv('EC2_HOST') or data.get('ec2_host'),
        'ec2_user': os.getenv('EC2_USER') or data.get('ec2_user') or 'ec2-user',
        'key_path': os.getenv('EC2_KEY_PATH') or data.get('ec2_key_path'),
        'run_at': os.getenv('RUN_AT') or data.get('run_at'),
        'partner_id': data['partner_id'],
        's3_feed_file': data['s3_feed_file'],
        's3_output_path': data['s3_output_path'],
        'distinguish_id': data['distinguish_id'],
        'callback_url': os.getenv('CALLBACK_URL') or data.get('callback_url'),
        'job_id': data.get('job_id', 0),
        'type': data.get('type', 'feed_generation')
    }

    def background():
        try:
            execute_and_notify(opts)
        except Exception as e:
            app.logger.error('Background job error: %s', e)

    thread = threading.Thread(target=background, daemon=True)
    thread.start()

    return jsonify({'message': 'Job accepted', 'job_id': opts['job_id']}), 200


@app.route('/api/callback/feed', methods=['POST'])
def callback_receiver():
    payload = request.get_json(force=True)
    app.logger.info('Received callback: %s', payload)
    return jsonify({'received': True})


if __name__ == '__main__':
    port = int(os.getenv('PORT', 3000))
    app.run(host='0.0.0.0', port=port)
