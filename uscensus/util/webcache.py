import datetime
from email.utils import format_datetime
import logging
import sys
import time

import requests


_logger = logging.getLogger(__name__)


def condget(req, date, session, retries):
    """Conditionally get `req` using `session` if it has been modified
    since `date`, retrying failures `retries` times.

    Returns: the modified document or None.

    Exceptions:
      * requests.exceptions.HTTPError on HTTP failure

    """
    headers = {}
    if date:
        headers['If-Modified-Since'] = format_datetime(date)
    req.headers = headers
    for retry in range(retries):
        r = session.send(req)
        if r.status_code < 400:
            break
        time.sleep(1)
    r.raise_for_status()
    if r.status_code == 304:
        return None
    return r.text


def fetchjson(url, cache, session, *, retries=3, **kwargs):
    """Caching wrapper around requests.py, to get a URL, check for
    errors, and return the parsed JSON reponse.

    The document will be retrieved and stored in the cache if not
    already present or if `cache.timeout` has elapsed and the document
    has been modified since last store.

    Arguments:
      * url: URL from which to fetch JSON resonse
      * cache: Cache in which to store response
      * session: optional requests.Session for making API call
      * retries: number of times to retry failed GETs
      * kwargs: additional arguments to `requests.get`

    Exceptions:
      * requests.exceptions.HTTPError on HTTP failure
      * ValueError on JSON parse failure

    """
    req = requests.Request('GET', url, **kwargs).prepare()
    url = req.url
    doc, date = None, None
    # check for a cached document
    try:
        doc, date = cache.get(url)
    except Exception as e:
        _logger.debug(f'cache error: {type(e)}: {e}',
                      exc_info=sys.exc_info())

    # check if the cached document is stale
    stale = False
    if date:
        stale = (datetime.datetime.now().timestamp() -
                 date.timestamp()) > cache.timeout.total_seconds()
    _logger.debug(f'hit={bool(doc)} expiry={date} stale={stale} url={url}')

    # see if we need to re-fetch
    if not doc or stale:
        # try to get the doc if we don't have it or if it's changed
        text = condget(
            req,
            date,
            session or requests.Session(),
            retries
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
