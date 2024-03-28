import logging
import os

import pyarrow
import pyarrow.lib as pl
import pyarrow.parquet as pq

import feedblocks.scrape as fs

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

# PARTITION ###################################################################

def tidy(path: str='data') -> None:
    __files = [__p.split('__') for __p in os.listdir(path) if os.path.isfile(os.path.join(path, __p))]
    for __f in __files:
        __old = os.path.join(path, '__'.join(__f))
        __new = os.path.join(path, *__f)
        try:
            os.rename(__old, __new)
            logging.info('{} => {}'.format(__old, __new))
        except Exception:
            logging.debug('failed to move {}'.format(__old))

# IO ##########################################################################

DATA_PATH = 'data/{chain}/{dataset}/'

def load(chain: str='ethereum', dataset: str='contracts', path: str=DATA_PATH, schemas: dict=SCHEMAS) -> pq.ParquetDataset:
    __path = path.format(chain=chain, dataset=dataset)
    __schema = schemas.get(dataset, None)
    return pq.ParquetDataset(__path, schema=__schema)

def save(table: pl.Table, chain: str='ethereum', dataset: str='contracts', path: str=DATA_PATH, schemas: dict=SCHEMAS) -> None:
    __path = path.format(chain=chain, dataset=dataset)
    __schema = schemas.get(dataset, None)
    return pq.write_to_dataset(table, root_path=__path, schema=__schema)

# __c = pyarrow.compute.field('contract_address') == bytes.fromhex('001BD7EF3B7424A18DA412C7AAA3B6534BA7816E')

# SOURCE CODE #################################################################

def add_solidity_sources_to_batch(batch: pl.RecordBatch, get: callable=fs.get_source_from_etherscan) -> list:
    # output
    __batch = []
    # split
    __rows = batch.to_pylist()
    # batch stats
    __stats = {'ok': 0, 'missing': 0, 'skipped': 0, 'errors': 0}
    # iterate
    for __r in __rows:
        # query API
        if __r and __r.get('contract_address', b'') and not __r.get('source_code', b''):
            __a = '0x' + __r.get('contract_address', b'').hex()
            __r['source_code'] = get(address=__a)
            # stats
            if __r['source_code'] is None:
                __stats['errors'] += 1
            elif not __r['source_code']:
                __stats['missing'] += 1
            else:
                __stats['ok'] += 1
        # already fetched
        else:
            __stats['skipped'] += 1
        # save data
        __batch.append(__r)
    # log
    for __k, __v, in __stats.items():
        logging.info('{label}: {count}'.format(label=__k, count=__v))
    # format as pyarrow table
    return __batch

def add_solidity_sources_to_table(table: pl.Table, get: callable=fs.get_source_from_etherscan) -> pl.Table:
    # iterate on parquet files / table
    __table = []
    __batches = list(table.to_batches(max_chunksize=128))
    __total = len(__batches)
    # iterate
    for __i, __b in enumerate(__batches):
        logging.info('batch {} / {}'.format(__i, __total))
        __table.extend(add_solidity_sources_to_batch(batch=__b, get=get))
    # convert to pyarrow
    return pl.Table.from_pylist(mapping=__table, schema=table.schema)

def add_solidity_sources_to_dataset(dataset: pq.ParquetDataset, get: callable=fs.get_source_from_etherscan) -> None:
    # scrape the solidity source code
    for __fragment in dataset.fragments[1:]:
        # current file
        logging.info(__fragment.path)
        # fetch solidity sources
        __table = add_solidity_sources_to_table(table=__fragment.to_table(), get=get)
        # save to disk
        pq.write_table(table=__table, where=__fragment.path + '_')
        # if successful replace the original table
        if os.path.isfile(__fragment.path + '_'):
            try:
                os.replace(__fragment.path + '_', __fragment.path)
                logging.info('updated solidity sources')
            except Exception:
                logging.debug('failed to replace {}'.format(__fragment.path))
