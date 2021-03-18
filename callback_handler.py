
import logging
import threading
import json
from flask import Flask, jsonify, request
# from flask import request
from gevent.pywsgi import WSGIServer


class MockSnowHandler(threading.Thread):
    def __init__(self, props, resp_dict):
        threading.Thread.__init__(self)
        self.props = props
        self.log = logging.getLogger('MTATester.MockSnowHandler')
        self.responses = resp_dict  # global, but want local ref
        self.http_server = None

    def run(self):
        app = Flask(__name__)

        @app.route('/stresstest/cb', methods=['POST'])
        def handle_cb_data():
            response_object = json.loads(request.data)
            rid = response_object['ExecutionId']
            print(f'got reply for: {rid}')
            self.responses[rid] = request.data
            return jsonify({'accepted': 'OK'})

        self.http_server = WSGIServer(('', 5000), app)
        self.http_server.serve_forever()
        print('CB server started')
