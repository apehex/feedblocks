import json
import os

import pyarrow
import pyarrow.lib as pl
import pyarrow.parquet as pq
import requests
import web3

# SCHEMA ######################################################################

BASE_SCHEMA = pyarrow.schema(fields=[
    pyarrow.lib.field('chain_id', pyarrow.uint64()),
    pyarrow.lib.field('block_number', pyarrow.uint32()),
    pyarrow.lib.field('block_hash', pyarrow.binary()),
    pyarrow.lib.field('transaction_hash', pyarrow.binary()),
    pyarrow.lib.field('deployer', pyarrow.binary()),
    pyarrow.lib.field('factory', pyarrow.binary()),
    pyarrow.lib.field('contract_address', pyarrow.binary()),
    pyarrow.lib.field('create_index', pyarrow.uint32()),
    pyarrow.lib.field('init_code', pyarrow.binary()),
    pyarrow.lib.field('init_code_hash', pyarrow.binary()),
    pyarrow.lib.field('code', pyarrow.binary()),
    pyarrow.lib.field('code_hash', pyarrow.binary()),
    pyarrow.lib.field('n_init_code_bytes', pyarrow.uint32()),
    pyarrow.lib.field('n_code_bytes', pyarrow.uint32()),])

EXTRA_SCHEMA = pyarrow.schema(fields=[
    pyarrow.lib.field('source_code', pyarrow.binary()),])

SCHEMA = pyarrow.unify_schemas(schemas=[BASE_SCHEMA, EXTRA_SCHEMA])

# DATASET #####################################################################



# TARGETS #####################################################################

# SOURCE CODE #################################################################

ETH_API_KEY = os.environ.get('ETH_API_KEY', '')
ETH_API_URL = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}'

__a = '0xc62bdA6F3f7158Bc263Ebc188155F398c6167cE2'

__d = pq.ParquetDataset('data/', schema=SCHEMA)
__t = __d.read()
__c = pyarrow.compute.field('contract_address') == bytes.fromhex('001BD7EF3B7424A18DA412C7AAA3B6534BA7816E')
__t.filter(__c)

def get_source(address: str, key: str=ETH_API_KEY) -> bytes:
    __b = b''
    try:
        # query etherscan
        __r = requests.get(ETH_API_URL.format(address=address, key=ETH_API_KEY))
        __j = json.loads(__r.text)
        __b = __j['result'][0]['SourceCode'].encode('utf-8')
    except Exception:
        pass
    return __b

def scrape_source(table: pl.Table, schema: pl.Schema=SCHEMA, key: str=ETH_API_KEY) -> pl.Table:
    __i = 0
    __table = []
    for __batch in table.to_batches(max_chunksize=128):
        __rows = __batch.to_pylist()
        for __r in __rows:
            if __r and __r.get('contract_address', b'') and not __r.get('source_code', b''):
                __r['source_code'] = get_source(address=__r.get('contract_address', b''))
            __table.append(__r)
    return pl.Table.from_pylist(mapping=__table, schema=schema, key=key)

# RUNTIME BYTECODE ############################################################

# pq.write_to_dataset(__t, root_path='data/')

# iterate over dataset row by row

# get bytecode

# disassemble

# tokenize

# get source code from explorer

# save it in DB

# recreate project

# compile & compare

# CREATION BYTECODE ###########################################################
