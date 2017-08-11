from __future__ import unicode_literals
from builtins import str

def ensuretext(val):
    """Make sure strings/lists of strings are unicode."""
    if isinstance(val, list):
        return [ensuretext(elt) for elt in val]
    elif isinstance(val, str):
        return val
    elif isinstance(val, dict):
        return [ensuretext(key) for key in val]
    else:
        return str(val)
