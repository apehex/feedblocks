import functools
import logging
import sys

import pyarrow.compute as pc
import pyarrow.lib as pl
import pyarrow.parquet as pq

import feedblocks.data as fd
import feedblocks.scrape as fs

# INIT ########################################################################

MESSAGE_PATTERN = '[{levelname:>8}] {message}'

def setup_logger(level: int=logging.INFO, pattern: str=MESSAGE_PATTERN) -> None:
    """Configure the default log objects for a specific bot."""
    __formatter = logging.Formatter(pattern, style='{')

    __handler = logging.StreamHandler(sys.stdout)
    __handler.setLevel(level)
    __handler.setFormatter(__formatter)

    __logger = logging.getLogger()
    __logger.setLevel(level)
    __logger.addHandler(__handler)

def info(message: str, indent: int=0) -> None:
    logging.info((indent * '.') + ((indent > 0) * ' ') + message)

# GET SOURCES #################################################################

# MAIN ########################################################################

if __name__ == '__main__':
    # init
    setup_logger()
    # throttle the scraping
    __get = fs.pace(freq=fs.RATE_LIMIT_ETHERSCAN - 1.)(functools.partial(fs.get_source, key=fs.ETH_API_KEY))
    # setup the logger with the right indentation for the inner loop
    __log = functools.partial(info, indent=8)
    # load the current data
    __dataset = fd.load(chain='ethereum', dataset='contracts')
    # iterate on parquet files / table
    for __fragment in __dataset.fragments[:1]:
        info(message=__fragment.path, indent=0)
        __table = []
        __batches = list(__fragment.to_batches(batch_size=128))
        # iterate
        for __i, __b in enumerate(__batches[:1]):
            info(message='batch {}'.format(__i), indent=4)
            __table.extend(fs.scrape_source(batch=__b, get=__get, log=__log))
        # save to disk
        pq.write_table(table=pl.Table.from_pylist(mapping=__table, schema=__dataset.schema), where=__fragment.path + '_')
