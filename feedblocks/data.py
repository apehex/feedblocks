import logging
import os
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.lib as pl
import pyarrow.parquet as pq

import feedblocks.scrape as fs

# SCHEMA ######################################################################

MAPPING = {
    pl.field('chain_id', pa.uint64()): pl.field('chain_id', pa.uint64()),
    pl.field('block_number', pa.uint32()): pl.field('block_number', pa.uint32()),
    pl.field('block_hash', pa.large_binary()): pl.field('block_hash', pa.large_binary()),
    pl.field('transaction_hash', pa.large_binary()): pl.field('transaction_hash', pa.large_binary()),
    pl.field('deployer', pa.large_binary()): pl.field('deployer_address', pa.large_binary()),
    pl.field('factory', pa.large_binary()): pl.field('factory_address', pa.large_binary()),
    pl.field('contract_address', pa.large_binary()): pl.field('contract_address', pa.large_binary()),
    pl.field('init_code', pa.large_binary()): pl.field('creation_bytecode', pa.large_binary()),
    pl.field('code', pa.large_binary()): pl.field('runtime_bytecode', pa.large_binary()),
    None: pl.field('creation_sourcecode', pa.large_binary()),}

INPUT_SCHEMA = pa.schema(fields=[__k for __k in MAPPING.keys() if __k is not None])
OUTPUT_SCHEMA = pa.schema(fields=[__k for __k in MAPPING.values() if __k is not None])

# FORMAT ######################################################################

def reformat_table(table: pa.Table, mapping: dict=MAPPING) -> pa.Table:
    # don't rename columns that will be removed
    __rename = {__k.name: __v.name for __k, __v in mapping.items() if __k is not None}
    __remove = [table.field(__i).name for __i in range(table.num_columns) if table.field(__i) not in mapping]
    __add = pa.nulls(size=table.num_rows, type=pa.large_binary())
    # actually rename
    __table = table.rename_columns(names=__rename)
    # remove the columns
    __table = __table.drop_columns(columns=__remove)
    # add the column for source code data
    return __table.append_column(field_=mapping[None], column=__add)

def reformat_dataset(dataset: pq.ParquetDataset, tempdir: str, mapping: dict=MAPPING) -> pq.ParquetDataset:
    logging.info('reformatting...')
    for __fragment in dataset.fragments:
        # extract the meta parameters
        __parsed = parse_fragment_path(__fragment.path)
        __name = compose_fragment_path(first_block=__parsed['first_block'], last_block=__parsed['last_block'], dataset_path=tempdir)
        # load the data
        __table = __fragment.to_table()
        # reformat the table
        __table = reformat_table(table=__table, mapping=mapping)
        # write to disk
        pq.write_table(table=__table, where=os.path.join(tempdir, __name))
        # log
        logging.info('.... {}'.format(__name))
    # log
    logging.info('exported to {}'.format(tempdir))
    # reload the resulting dataset
    return pq.ParquetDataset(tempdir, schema=__table.schema)

# MERGE #######################################################################

def merge_datasets(source: pq.ParquetDataset, destination: pq.ParquetDataset) -> pq.ParquetDataset:
    logging.info('merging...')
    # destination path
    __dest_dir = destination._base_dir
    # list fragments in both datasets
    __dest_files = [os.path.basename(__f.path) for __f in destination.fragments]
    # iterate over the source dataset
    for __fragment in source.fragments:
        __src_name = os.path.basename(__fragment.path)
        # do not overwrite
        if __src_name not in __dest_files:
            pq.write_table(table=__fragment.to_table(), where=os.path.join(__dest_dir, __src_name))
            # log
            logging.info('.... {} => {}'.format(__fragment.path, os.path.join(__dest_dir, __src_name)))
    # load the resulting dataset
    return pq.ParquetDataset(__dest_dir, schema=destination.schema)

# PARTITION ###################################################################

def parse_fragment_path(path: str) -> dict:
    __result = {}
    __regex = re.compile(r'(\w+)__(\w+)__(\d+)_to_(\d+).parquet')
    __match = re.findall(pattern=__regex, string=os.path.basename(path))
    if __match and len(__match[0]) == 4:
        __result = {'chain': __match[0][0], 'dataset': __match[0][1], 'first_block': int(__match[0][2]), 'last_block': int(__match[0][-1])}
    return __result

def compose_dataset_path(root: str='data', chain: str='ethereum', dataset: str='contracts') -> str:
    return os.path.join(root, chain, dataset)

def compose_fragment_path(first_block: int, last_block: int, dataset_path: str='data/ethereum/contracts/') -> str:
    return os.path.join(dataset_path, '{}_to_{}.parquet'.format(first_block, last_block))

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

def load(chain: str='ethereum', dataset: str='contracts', path: str=DATA_PATH, schema: dict=OUTPUT_SCHEMA) -> pq.ParquetDataset:
    __path = path.format(chain=chain, dataset=dataset)
    return pq.ParquetDataset(__path, schema=schema)

def save(table: pl.Table, chain: str='ethereum', dataset: str='contracts', path: str=DATA_PATH, schema: dict=OUTPUT_SCHEMA) -> None:
    __path = path.format(chain=chain, dataset=dataset)
    return pq.write_to_dataset(table, root_path=__path, schema=schema)

# __c = pa.compute.field('contract_address') == bytes.fromhex('001BD7EF3B7424A18DA412C7AAA3B6534BA7816E')

# SOURCE CODE #################################################################

def _is_record_update_required(
    record: dict,
    force_update_empty_records: bool=False,
    force_update_filled_records: bool=False
) -> bool:
    __address = record.get('contract_address', None)
    __sources = record.get('creation_sourcecode', None)
    return (
        (bool(record) and bool(__address)) # must be feasible
        and (
            __sources is None # still not queried
            or (__sources == b'' and force_update_empty_records) # was not found on previous update
            or (bool(__sources) and force_update_filled_records))) # has already been queried & saved but update is requested

def _is_table_update_required(
    table: pl.Table,
    force_update_empty_records: bool=False,
    force_update_filled_records: bool=False
) -> bool:
    __c = pc.field('creation_sourcecode').is_null()
    return (
        'creation_sourcecode' not in table.column_names # table has no source code yet
        or force_update_empty_records or force_update_filled_records # update older data
        or bool(table.filter(__c).num_rows)) # column is in schema but has missing values

def _update_stats(
    record: dict,
    stats: dict,
    updated: bool
) -> None:
    __s = record.get('creation_sourcecode', None)
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
        __update_required = _is_record_update_required(record=__r, force_update_empty_records=force_update_empty_records, force_update_filled_records=force_update_filled_records)
        # query API
        if __update_required:
            __a = '0x' + __r.get('contract_address', b'').hex()
            __r['creation_sourcecode'] = get(address=__a)
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
        logging.info('scraping {}...'.format(os.path.basename(__fragment.path)))
        __table = __fragment.to_table(schema=dataset.schema)
        # fetch solidity sources
        if _is_table_update_required(table=__table, force_update_empty_records=force_update_empty_records, force_update_filled_records=force_update_filled_records):
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
                logging.info('=> updated {}'.format(__fragment.path))
            except Exception:
                logging.warning('failed to replace {}'.format(__fragment.path))
