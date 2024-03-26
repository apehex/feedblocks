import logging
import sys

import pyarrow.compute as pc

import feedblocks.data as fd
import feedblocks.scrape as fs

# INIT ########################################################################

MESSAGE_PATTERN = '[{levelname}] [{asctime}] {message}'

def setup_logger(level: int=logging.INFO, pattern: str=MESSAGE_PATTERN) -> None:
    """Configure the default log objects for a specific bot."""
    __formatter = logging.Formatter(pattern, style='{')

    __handler = logging.StreamHandler(sys.stdout)
    __handler.setLevel(level)
    __handler.setFormatter(__formatter)

    __logger = logging.getLogger()
    __logger.setLevel(level)
    __logger.addHandler(__handler)

# GET SOURCES #################################################################

setup_logger()

__t = fs.scrape_source(table=fd.__t, key=fs.ETH_API_KEY, freq=fs.FREQUENCY)

# pq.write_to_dataset(__t, root_path='data/')
