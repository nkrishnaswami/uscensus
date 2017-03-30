try:
    import ujson as json
except ImportError:
    import json

class NopCache(object):
    def get(self, url):
        return None
    def put(self, url, doc):
        return json.loads(doc)
