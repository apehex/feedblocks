import argparse
import functools
import os
import tempfile

import pyarrow.compute as pc
import pyarrow.lib as pl
import pyarrow.parquet as pq

import feedblocks._logging as fl
import feedblocks.data as fd
import feedblocks.scrape as fs

# CLI #########################################################################

def main() -> None:
    # remove all the external indent
    fl.setup_logger(level=20, offset=-10, tabs=4)
    # CLI args
    __parser = argparse.ArgumentParser(description='Scrape blockchain data and solidify in a dataset.')
    __parser.add_argument('--path', '-p', action='store', dest='out_path', type=str, default='data/ethereum/contracts/', help='the path to the final dataset')
    __parser.add_argument('--exp-key', '-k', action='store', dest='exp_key', type=str, default=fs.ETH_API_KEY, help='a key for a blockchain explorer API to scrape sources')
    __parser.add_argument('--exp-lim', '-l', action='store', dest='exp_lim', type=float, default=fs.RATE_LIMIT_ETHERSCAN, help='the rate limit (freq) of API requests')
    __parser.add_argument('--exp-url', '-u', action='store', dest='exp_url', type=str, default=fs.ETH_API_URL, help='a URL to the blockchain explorer API endpoint')
    __parser.add_argument('--import', '-i', action='store', dest='in_path', type=str, default='', help='path to newly scraped data')
    __parser.add_argument('--rpc-url', '-r', action='store', dest='rpc_url', type=str, default='', help='the RPC endpoint used to scrape blockchain data')
    __parser.add_argument('--tmp-dir', '-t', action='store', dest='tmp_path', type=str, default='', help='temporary directory to store intermediate results')
    # parse
    __args = vars(__parser.parse_args())
    # make tmp dir
    __tmp = __args.get('tmp_path', '') or tempfile.mkdtemp()
    # setup the function to scrape sources from the block explorer
    __key = __args.get('exp_key', fs.ETH_API_KEY)
    __url = __args.get('exp_url', fs.ETH_API_URL)
    __rate = __args.get('exp_lim', fs.RATE_LIMIT_ETHERSCAN)
    __get = fs.pace(freq=__rate)(functools.partial(fs.get_source, url=__url, key=__key))
    # sort the parquet files
    __input_dataset_path = __args.get('in_path', '')
    __output_dataset_path = __args.get('out_path', 'data/ethereum/contracts/')
    # load the current dataset
    __output_dataset = pq.ParquetDataset(__output_dataset_path, schema=fd.OUTPUT_SCHEMA)
    # option to import newly scraped data (with cryo)
    if __input_dataset_path and os.path.isdir(__input_dataset_path):
        __input_dataset = pq.ParquetDataset(__input_dataset_path, schema=fd.INPUT_SCHEMA)
        # reformat
        __input_dataset = fd.reformat_dataset(dataset=__input_dataset, tempdir=__tmp, mapping=fd.MAPPING)
        # merge the new scraped data with the current dataset
        __output_dataset = fd.merge_datasets(source=__input_dataset, destination=__output_dataset)
    # scrape the solidity sources
    fd.add_solidity_sources_to_dataset(dataset=__output_dataset, get=__get, force_update_empty_records=False, force_update_filled_records=False)
    # reload the dataset with sources
    __dataset = fd.load(chain='ethereum', dataset='contracts')

# MAIN ########################################################################

if __name__ == '__main__':
    main()
