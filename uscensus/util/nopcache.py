from __future__ import print_function
from __future__ import unicode_literals


try:
    import ujson as json
except ImportError:
    import json


class NopCache(object):
    """Dummy cache implementation for fetchjson that does not store data"""
    def get(self, url):
        return None, None

    def delete(self, url):
        pass

    def touch(self, url, date):
        pass

    def put(self, url, doc, date):
        return json.loads(doc)
