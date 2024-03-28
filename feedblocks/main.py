import functools
import logging
import os
import traceback
import sys

import pyarrow.compute as pc
import pyarrow.lib as pl
import pyarrow.parquet as pq

import feedblocks.data as fd
import feedblocks.scrape as fs

# INDENT / FORMAT #############################################################

def indent(msg: str, offset: int=-4, tabs: int=4) -> str:
    # account for logging frames
    __i = tabs * max(0, len(traceback.extract_stack()) + offset)
    return (__i * '.') + ((__i > 0) * ' ') + msg

class IndentedStreamHandler(logging.StreamHandler):
    def __init__(self, stream, offset: int=-4, tabs: int=4) -> None:
        self._offset = offset
        self._tabs = tabs
        super(IndentedStreamHandler, self).__init__(stream)

    def emit(self, record: logging.LogRecord) -> None:
        record.msg = indent(msg=record.msg, offset=self._offset, tabs=self._tabs)
        super(IndentedStreamHandler, self).emit(record)

# INIT ########################################################################

MESSAGE_PATTERN = '[{levelname:>8}] {message}'

def setup_logger(level: int=logging.INFO, pattern: str=MESSAGE_PATTERN, offset: int=-4, tabs: int=4) -> None:
    """Configure the default log objects for a specific bot."""
    __formatter = logging.Formatter(pattern, style='{')

    __handler = IndentedStreamHandler(stream=sys.stdout, offset=offset, tabs=tabs)
    __handler.setLevel(level)
    __handler.setFormatter(__formatter)

    __logger = logging.getLogger()
    __logger.setLevel(level)
    __logger.addHandler(__handler)

# MAIN ########################################################################

if __name__ == '__main__':
    # remove all the external indent
    setup_logger(level=logging.INFO, offset=-10, tabs=4)
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
