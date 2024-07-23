import functools
import json
import logging
import os
import time
import urllib.request

# META ########################################################################

ETH_API_KEY = os.environ.get('ETH_API_KEY', '')
ETH_API_URL = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}'

RATE_LIMIT_ETHERSCAN = 4. # Hz / calls per second
RATE_LIMIT_INFURA = 9. # Hz / calls per second

# RATE LIMIT ##################################################################

def pace(freq: float, backoff: float=1.0, limit: int=4) -> callable:
    """Creates a decorator thats throttles a function to a specified frequency."""
    __wait = 1. / freq
    __prev = 0.

    def __decorator(func: callable) -> callable:
        @functools.wraps(func)
        def __wrapper(*args, **kwargs):
            """Space calls to this functions to satisfy a rate limit."""
            nonlocal __prev
            # init
            __attempts = 0
            __result = None
            # attempt until success or max retries is reach
            while __attempts < max(1, limit):
                # sleep until next time slot
                time.sleep(max(0., __prev + __wait - 0.001 * time.time()))
                # ready
                try:
                    # actually call the original function
                    __result = func(*args, **kwargs)
                    # play it safe and take the time of return as ref for the next call
                    __prev = 0.001 * time.time()
                    # get out of the retry loop
                    return __result
                except Exception as __e:
                    __attempts += 1
                    # increase waiting time
                    __prev += __attempts * backoff
                    # log args + error
                    logging.debug(f'attempt {__attempts} failed: f({args} + {kwargs}) => error "{str(__e)}"')
            # run out of attempts
            logging.warning(f'all attempts failed: f({args} + {kwargs})')
            return None
        # return the wrapped function
        return __wrapper

    return __decorator

# SOURCE CODE #################################################################

def get_source(address: str, url: str=ETH_API_URL, key: str=ETH_API_KEY) -> bytes:
    __r = urllib.request.urlopen(url.format(address=address, key=key))
    __j = json.loads(__r.read().decode('utf-8'))
    return __j['result'][0]['SourceCode'].encode('utf-8')

get_source_from_etherscan = pace(freq=RATE_LIMIT_ETHERSCAN)(functools.partial(get_source, url=ETH_API_URL, key=ETH_API_KEY))
