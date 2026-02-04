import threading
import json
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
import time

# ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from run_script import execute_and_notify


class CallbackHandler(BaseHTTPRequestHandler):
    received = None

    def do_POST(self):
        length = int(self.headers.get('content-length', 0))
        body = self.rfile.read(length)
        CallbackHandler.received = json.loads(body.decode('utf-8'))
        self.send_response(200)
        self.end_headers()


def start_server(port):
    server = HTTPServer(('localhost', port), CallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_execute_and_notify_mock():
    port = 50123
    server = start_server(port)
    time.sleep(0.1)

    opts = {
        'mock': True,
        'callback_url': f'http://localhost:{port}/api/callback/feed',
        'job_id': 42,
        'type': 'feed_generation',
        'partner_id': 'testpartner',
        's3_feed_file': 's3://bucket/mock.xml.gz',
        's3_output_path': 's3://bucket/out',
        'distinguish_id': 'run1'
    }

    result = execute_and_notify(opts)

    # give server a moment
    time.sleep(0.2)

    assert result['destination'] == 's3://mock-bucket/mock_path/mock_stats.txt'
    assert CallbackHandler.received is not None
    assert CallbackHandler.received['job_id'] == 42
    assert CallbackHandler.received['status'] == 'success'
    assert CallbackHandler.received['result']['destination_s3_path'] == result['destination']

    server.shutdown()


if __name__ == '__main__':
    # Run the test directly without pytest
    test_execute_and_notify_mock()
    print('OK')
