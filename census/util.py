import requests

def fetchjson(url, cache, **kwargs):
    """
    Very thin wrapper around requests.py, to get a URL, check for errors, and return the parsed JSON reponse.
    This will cache queried data and try to avoid the call on hit.
    """
    doc=cache.get(url)
    if not doc:
        r=requests.get(url, **kwargs)
        r.raise_for_status()
        return cache.put(url, r.text)
    return doc
