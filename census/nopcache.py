try:
    import ujson as json
except ImportError:
    import json


class NopCache(object):
    """Dummy cache implementation for fetchjson that does not store data"""
    def get(self, url):
        return None

    def put(self, url, doc):
        return json.loads(doc)
