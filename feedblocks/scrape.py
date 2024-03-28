import functools
import json
import logging
import os
import time

import requests

# META ########################################################################

ETH_API_KEY = os.environ.get('ETH_API_KEY', '')
ETH_API_URL = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}'

RATE_LIMIT_ETHERSCAN = 5. # Hz / calls per second
RATE_LIMIT_INFURA = 10. # Hz / calls per second

# RATE LIMIT ##################################################################

def pace(freq: float) -> callable:
    """Creates a decorator thats throttles a function to a specified frequency."""
    __ms = 1. / freq
    __prev = 0.

    def __decorator(func: callable) -> callable:
        @functools.wraps(func)
        def __wrapper(*args, **kwargs):
            """Space calls to this functions to satisfy a rate limit."""
            nonlocal __prev
            __now = time.time() / 1000.
            # sleep until next time slot
            time.sleep(max(0., __prev + __ms - __now))
            # update the time of the previous call
            __prev = __now
            # actually output the original function's result
            return func(*args, **kwargs)
        return __wrapper

    return __decorator

# SOURCE CODE #################################################################

def get_source(address: str, url: str=ETH_API_URL, key: str=ETH_API_KEY) -> bytes:
    __b = b''
    try:
        # query etherscan
        __r = requests.get(url.format(address=address, key=key))
        __j = json.loads(__r.text)
        __b = __j['result'][0]['SourceCode'].encode('utf-8')
    except Exception:
        logging.debug(str(__j))
        return None
    return __b

get_source_from_etherscan = pace(freq=RATE_LIMIT_ETHERSCAN)(functools.partial(get_source, url=ETH_API_URL, key=ETH_API_KEY))
