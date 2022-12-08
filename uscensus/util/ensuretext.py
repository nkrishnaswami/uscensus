def ensuretext(val):
    """Turn strings/lists of strings into unicode strings."""
    if isinstance(val, list):
        return ' '.join([ensuretext(elt) for elt in val])
    elif isinstance(val, str):
        return val
    elif isinstance(val, dict):
        return ' '.join(ensuretext(key) for key in val.keys())
    else:
        return str(val)
