import functools
import json
import logging
import os
import time

import pyarrow.lib as pl
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

def get_source(address: str, key: str=ETH_API_KEY) -> bytes:
    __b = b''
    try:
        # query etherscan
        __r = requests.get(ETH_API_URL.format(address=address, key=ETH_API_KEY))
        __j = json.loads(__r.text)
        __b = __j['result'][0]['SourceCode'].encode('utf-8')
    except Exception:
        return None
    return __b

def scrape_source(batch: pl.RecordBatch, get: callable=pace(freq=RATE_LIMIT_ETHERSCAN)(get_source), log: callable=logging.info) -> list:
    # output
    __batch = []
    # split
    __rows = batch.to_pylist()
    # batch stats
    __error_count = 0
    __skipped_count = 0
    __missing_count = 0
    __ok_count = 0
    # iterate
    for __r in __rows:
        if __r and __r.get('contract_address', b'') and not __r.get('source_code', b''):
            # query API
            __a = '0x' + __r.get('contract_address', b'').hex()
            __r['source_code'] = get(address=__a)
            # stats
            if __r['source_code'] is None:
                __error_count +=1
            elif not __r['source_code']:
                __missing_count += 1
            else:
                __ok_count +=1
        else:
            __skipped_count +=1
        # save data
        __batch.append(__r)
    # log
    log('ok: {}'.format(__ok_count))
    log('missing: {}'.format(__missing_count))
    log('skipped: {}'.format(__skipped_count))
    log('errors: {}'.format(__error_count))
    # format as pyarrow table
    return __batch

# RUNTIME BYTECODE ############################################################

# iterate over dataset row by row

# get bytecode

# disassemble

# tokenize

# get source code from explorer

# save it in DB

# recreate project

# compile & compare

# CREATION BYTECODE ###########################################################
