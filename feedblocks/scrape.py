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
FREQUENCY = 9. # Hz / calls per second

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

def scrape_source(table: pl.Table, key: str=ETH_API_KEY, freq: float=FREQUENCY) -> pl.Table:
    # global stats
    __batch_count = 0
    # output
    __table = []
    # rate limited get
    __get = pace(freq=freq)(get_source) # rate limit on API queries
    for __batch in table.to_batches(max_chunksize=128):
        # split
        __rows = __batch.to_pylist()
        # batch stats
        logging.info('batch {}...'.format(__batch_count))
        __error_count = 0
        __skipped_count = 0
        __not_found_count = 0
        __ok_count = 0
        for __r in __rows:
            if __r and __r.get('contract_address', b'') and not __r.get('source_code', b''):
                # query API
                __a = '0x' + __r.get('contract_address', b'').hex()
                __r['source_code'] = __get(address=__a, key=key)
                # stats
                if __r['source_code'] is None:
                    __error_count +=1
                elif not __r['source_code']:
                    __not_found_count += 1
                else:
                    __ok_count +=1
            else:
                __skipped_count +=1
            # save data
            __table.append(__r)
        # log
        logging.info('stats:\nsuccessful: {ok}\nnot found: {found}\nskipped: {skipped}\nerrors: {errors}\n'.format(ok=__ok_count, found=__not_found_count, skipped=__skipped_count, errors=__error_count))
        __batch_count += 1
    return pl.Table.from_pylist(mapping=__table, schema=table.schema)

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
