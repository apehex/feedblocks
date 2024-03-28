import functools
import logging
import os
import sys

import pyarrow.compute as pc
import pyarrow.lib as pl
import pyarrow.parquet as pq

import feedblocks.data as fd
import feedblocks.scrape as fs

# INDENT / FORMAT #############################################################

def indent(ticks: int=0) -> callable:
    def __decorator(func: callable) -> callable:
        @functools.wraps(func)
        def __wrapper(*args, **kwargs) -> None:
            __msg = kwargs.get('msg', args[0])
            return func((ticks * '.') + ((ticks > 0) * ' ') + __msg)
        return __wrapper

    return __decorator

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

# MAIN ########################################################################

if __name__ == '__main__':
    # setup the loggers with the right indentation for the inner loop
    __info_4 = indent(ticks=4)(logging.info)
    __info_8 = indent(ticks=8)(logging.info)
    __debug_12 = indent(ticks=12)(logging.debug)
    # init
    setup_logger(level=logging.INFO)
    # sort the parquet files
    fd.tidy()
    # load the current data
    __dataset = fd.load(chain='ethereum', dataset='contracts')
    # scrape the solidity sources
    fd.add_solidity_sources_to_dataset(dataset=__dataset, get=fs.get_source_from_etherscan)
    # reload the dataset with sources
    __dataset = fd.load(chain='ethereum', dataset='contracts')

# RUNTIME BYTECODE ############################################################

# iterate over dataset row by row

# get bytecode

# disassemble

# tokenize

# get source code from explorer

# save it in DB

# recreate project

# compile & compare

# CREATION BYTECODE ###########################################################
