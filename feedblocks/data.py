import os

import pyarrow
import pyarrow.lib as pl
import pyarrow.parquet as pq

# SCHEMA ######################################################################

SCHEMAS ={
    'contracts': pyarrow.schema(fields=[
        pl.field('chain_id', pyarrow.uint64()),
        pl.field('block_number', pyarrow.uint32()),
        pl.field('block_hash', pyarrow.binary()),
        pl.field('transaction_hash', pyarrow.binary()),
        pl.field('deployer', pyarrow.binary()),
        pl.field('factory', pyarrow.binary()),
        pl.field('contract_address', pyarrow.binary()),
        pl.field('create_index', pyarrow.uint32()),
        pl.field('init_code', pyarrow.binary()),
        pl.field('init_code_hash', pyarrow.binary()),
        pl.field('code', pyarrow.binary()),
        pl.field('code_hash', pyarrow.binary()),
        pl.field('n_init_code_bytes', pyarrow.uint32()),
        pl.field('n_code_bytes', pyarrow.uint32()),
        pl.field('source_code', pyarrow.binary()),]),}

# DATASET #####################################################################

DATA_PATH = 'data/{chain}/{dataset}/'

def tidy(path: str='data') -> None:
    __files = [__p.split('__') for __p in os.listdir(path) if os.path.isfile(os.path.join(path, __p))]
    for __f in __files:
        __old = os.path.join(path, '__'.join(__f))
        __new = os.path.join(path, *__f)
        os.rename(__old, __new)

# __c = pyarrow.compute.field('contract_address') == bytes.fromhex('001BD7EF3B7424A18DA412C7AAA3B6534BA7816E')
