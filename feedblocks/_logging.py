import logging
import sys
import traceback

# CONSTANTS ###################################################################

MESSAGE_PATTERN = '[{levelname:>8}] {message}'

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

def setup_logger(level: int=logging.INFO, pattern: str=MESSAGE_PATTERN, offset: int=-4, tabs: int=4) -> None:
    """Configure the default log objects for a specific bot."""
    __formatter = logging.Formatter(pattern, style='{')

    __handler = IndentedStreamHandler(stream=sys.stdout, offset=offset, tabs=tabs)
    __handler.setLevel(level)
    __handler.setFormatter(__formatter)

    __logger = logging.getLogger()
    __logger.setLevel(level)
    __logger.addHandler(__handler)
