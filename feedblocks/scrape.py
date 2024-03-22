import json
import os

import pyarrow
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
    pyarrow.lib.field('code_source', pyarrow.binary()),
    pyarrow.lib.field('code_assembly', pyarrow.binary()),
    pyarrow.lib.field('init_code_assembly', pyarrow.binary()),])

SCHEMA = pyarrow.unify_schemas(schemas=[BASE_SCHEMA, EXTRA_SCHEMA])

# TARGETS #####################################################################

# SOURCE CODE #################################################################

ETH_API_KEY = os.environ.get('ETH_API_KEY', '')
ETH_API_URL = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}'

__a = '0x2ea46070dCD7C7DDD2114b4508548551d8630471'
__r = requests.get(ETH_API_URL.format(address=__a, key=ETH_API_KEY))
__t = __r.text

# RUNTIME BYTECODE ############################################################

__d = pq.ParquetDataset('data/', schema=SCHEMA)
__t = __d.read(columns=['contract_address', 'code', 'init_code'])
__t.to_batches(max_chunksize=128)
__c = pyarrow.compute.field('contract_address') == bytes.fromhex('001BD7EF3B7424A18DA412C7AAA3B6534BA7816E')
__t.filter(__c)

# iterate over dataset row by row
for __b in __t.to_batches():
	__r = __b.to_pylist()
	list(__r[0].get('code'))

# get bytecode

# disassemble

# tokenize

# get source code from explorer

# save it in DB

# recreate project

# compile & compare

# CREATION BYTECODE ###########################################################
