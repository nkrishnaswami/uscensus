def ensuretext(val):
    """Turn strings/lists of strings into unicode strings."""
    if isinstance(val, list):
        return ' '.join([ensuretext(elt) for elt in val])
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        return ' '.join(ensuretext(key) for key in val)
    return str(val)
