from __future__ import print_function
from __future__ import unicode_literals
from builtins import str


def ensuretext(val):
    """Turn strings/lists of strings into unicode strings."""
    if isinstance(val, list):
        return ' '.join([ensuretext(elt) for elt in val])
    elif isinstance(val, str):
        return val
    elif isinstance(val, dict):
        return ' '.join(ensuretext(key) for key in val.keys())
    else:
        # n.b. in Python 2, the builtins import makes this mean `unicode`.
        return str(val)
