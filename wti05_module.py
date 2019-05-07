from flask import Flask


class EndpointAction(object):
    def __init__(self, action):
        self.action = action

    def __call__(self, **kwargs):
        return self.action(**kwargs)


class FlaskAppWrapper(object):
    def __init__(self, name):
        self.app = Flask(name)

    def run(self, host='0.0.0.0', port='8888'):
        self.app.run(host=host, port=port)

    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None, methods=[]):
        self.app.add_url_rule(endpoint, endpoint_name, EndpointAction(handler), methods=methods)
