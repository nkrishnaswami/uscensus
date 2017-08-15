from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import requests
import sys


# out of alphabetical order since there's fallback logic
try:
    from email.utils import format_datetime, parsedate_to_datetime
except ImportError:
    # these were introduced in 3.3; quick hack:
    from email.utils import formatdate, parsedate
    import time

    def format_datetime(dt):
        return formatdate(time.mktime(dt.timetuple()))

    def parsedate_to_datetime(date):
        return datetime.datetime(*parsedate(date)[:6])


def condget(url, date, session, **kwargs):
    """Conditionally get `url` using `session` if it has been modified
    since `date`.

    Returns: the modified document or None.

    Exceptions:
      * requests.exceptions.HTTPError on HTTP failure

    """
    headers = {}
    if date:
        headers['If-Modified-Since'] = format_datetime(date)
    r = session.get(url, headers=headers, **kwargs)
    r.raise_for_status()
    if r.status_code == 304:
        return None
    return r.text


def fetchjson(url, cache, session, **kwargs):
    """Caching wrapper around requests.py, to get a URL, check for
    errors, and return the parsed JSON reponse.

    The document will be retrieved and stored in the cache if not
    already present or if `cache.timeout` has elapsed and the document
    has been modified since last store.

    Arguments:
      * url: URL from which to fetch JSON resonse
      * cache: Cache in which to store response
      * session: optional requests.Session for making API call
      * kwargs: additional arguments to `requests.get`

    Exceptions:
      * requests.exceptions.HTTPError on HTTP failure
      * ValueError on JSON parse failure

    """
    doc, date = None, None
    # check for a cached document
    try:
        doc, date = cache.get(url)
    except Exception as e:
        logging.debug('cache error: {}: {}'.format(
            type(e), e), exc_info=sys.exc_info())

    # check if the cached document is stale
    stale = False
    if date:
        stale = (datetime.datetime.now().timestamp() -
                 date.timestamp()) > cache.timeout.total_seconds()
    logging.debug('hit={} expiry={} stale={} url={}'.format(
        not not doc, date, stale, url))

    # see if we need to re-fetch
    if not doc or stale:
        # try to get the doc if we don't have it or if it's changed
        text = condget(
            url,
            date,
            session or requests,
            **kwargs
        )
        if text is None:
            # unchanged; update cache timestamp so we don't do
            # more cond. gets till `cache.timeout` passes again.
            cache.touch(url, datetime.datetime.now())
        else:
            # changed
            if stale:
                cache.delete(url)
            doc = cache.put(
                url,
                text,
                datetime.datetime.now())
    return doc
