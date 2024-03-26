import pyarrow
import pyarrow.lib as pl
import pyarrow.parquet as pq

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

__a = '0xc62bdA6F3f7158Bc263Ebc188155F398c6167cE2'

__d = pq.ParquetDataset('data/', schema=SCHEMA)
__t = __d.read()
__c = pyarrow.compute.field('contract_address') == bytes.fromhex('001BD7EF3B7424A18DA412C7AAA3B6534BA7816E')
__t.filter(__c)

# pq.write_to_dataset(__t, root_path='data/')
