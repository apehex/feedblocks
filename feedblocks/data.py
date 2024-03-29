import logging
import os

import pyarrow
import pyarrow.compute as pc
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

def _update_record(
    record: dict,
    force_update_empty_records: bool=False,
    force_update_filled_records: bool=False
) -> bool:
    __address = record.get('contract_address', None)
    __sources = record.get('source_code', None)
    return (
        (bool(record) and bool(__address)) # must be feasible
        and (
            __sources is None # still not queried
            or (__sources == b'' and force_update_empty_records) # was not found on previous update
            or (bool(__sources) and force_update_filled_records))) # has already been queried & saved but update is requested

def _update_table(
    table: pl.Table,
    force_update_empty_records: bool=False,
    force_update_filled_records: bool=False
) -> bool:
    __c = pc.field('source_code') != b''
    return (
        'source_code' not in table.column_names # table has no source code yet
        or force_update_empty_records or force_update_filled_records # update older data
        or not bool(table.filter(__c))) # column is in schema but still empty

def _update_stats(
    record: dict,
    stats: dict,
    updated: bool
) -> None:
    __s = record.get('source_code', None)
    stats['ok'] += int(updated and bool(__s))
    stats['missing'] += int(updated and (__s == b''))
    stats['skipped'] += int(not updated)
    stats['errors'] += int(updated and (__s is None))

def add_solidity_sources_to_batch(
    batch: pl.RecordBatch,
    get: callable=fs.get_source_from_etherscan,
    force_update_empty_records: bool=False,
    force_update_filled_records: bool=False
) -> list:
    # output
    __batch = []
    # split
    __rows = batch.to_pylist()
    # batch stats
    __stats = {'ok': 0, 'missing': 0, 'skipped': 0, 'errors': 0}
    # iterate
    for __r in __rows:
        __update_required = _update_record(record=__r, force_update_empty_records=force_update_empty_records, force_update_filled_records=force_update_filled_records)
        # query API
        if __update_required:
            __a = '0x' + __r.get('contract_address', b'').hex()
            __r['source_code'] = get(address=__a)
        # stats
        _update_stats(record=__r, stats=__stats, updated=__update_required)
        # save data
        __batch.append(__r)
    # log
    for __k, __v, in __stats.items():
        logging.info('{label}: {count}'.format(label=__k, count=__v))
    # format as pyarrow table
    return __batch

def add_solidity_sources_to_table(
    table: pl.Table,
    get: callable=fs.get_source_from_etherscan,
    force_update_empty_records: bool=False,
    force_update_filled_records: bool=False
) -> pl.Table:
    # iterate on parquet files / table
    __table = []
    __batches = list(table.to_batches(max_chunksize=128))
    __total = max(0, len(__batches) - 1)
    # iterate
    for __i, __b in enumerate(__batches):
        logging.info('batch {} / {}'.format(__i, __total))
        __table.extend(add_solidity_sources_to_batch(
            batch=__b,
            get=get,
            force_update_empty_records=force_update_empty_records,
            force_update_filled_records=force_update_filled_records))
    # convert to pyarrow
    return pl.Table.from_pylist(mapping=__table, schema=table.schema)

def add_solidity_sources_to_dataset(
    dataset: pq.ParquetDataset,
    get: callable=fs.get_source_from_etherscan,
    force_update_empty_records: bool=False,
    force_update_filled_records: bool=False
) -> None:
    # scrape the solidity source code
    for __fragment in dataset.fragments:
        # current file
        logging.info(__fragment.path)
        __table = __fragment.to_table(schema=dataset.schema)
        # fetch solidity sources
        if _update_table(table=__table, force_update_empty_records=force_update_empty_records, force_update_filled_records=force_update_filled_records):
            __table = add_solidity_sources_to_table(
                table=__table,
                get=get,
                force_update_empty_records=force_update_empty_records,
                force_update_filled_records=force_update_filled_records)
        # save to disk
        pq.write_table(table=__table, where=__fragment.path + '_')
        # if successful replace the original table
        if os.path.isfile(__fragment.path + '_'):
            try:
                os.replace(__fragment.path + '_', __fragment.path)
                logging.info('updated solidity sources')
            except Exception:
                logging.debug('failed to replace {}'.format(__fragment.path))
