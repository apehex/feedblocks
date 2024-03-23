

# OPCODES #####################################################################

STOP = 0x00
EQ = 0x14
EXTCODECOPY = 0x3C
BLOCKHASH = 0x40
COINBASE = 0x41
PREVRANDAO = 0x44
JUMPDEST = 0x5B
PUSH1 = 0x60
PUSH32 = 0x7F
CREATE = 0xF0
CALLCODE = 0xF2
RETURN = 0xF3
DELEGATECALL = 0xF4
CREATE2 = 0xF5
REVERT = 0xFD
INVALID = 0xFE
SELFDESTRUCT = 0xFF

HALTING = [STOP, RETURN, REVERT, INVALID, SELFDESTRUCT]

is_halting = lambda opcode: opcode in HALTING
is_push = lambda opcode: opcode >= PUSH1 and opcode <= PUSH32

# INSTRUCTIONS ################################################################

def instruction_length(opcode: int) -> int:
    return 1 + is_push(opcode) * (1 + opcode - PUSH1) # 1 byte for the opcode + n bytes of data

# TOKENS ######################################################################

def one_hot(index: int, depth: int) -> list:
    __i = index % depth
    return __i * [0] + [1] + (depth - __i - 1) * [0]

# TOKEN > #####################################################################

def _tokenize_data(data: bytes) -> list:
    __bits = '{0:0>256b}'.format(int(data.hex(), 16)) if data else '' # expects at most a 32-byte word of data
    return [int(__b) for __b in __bits[::-1]]

def _tokenize_instruction(chunk: bytes) -> list:
    return one_hot(index=chunk[0], depth=256) + _tokenize_data(data=chunk[1:])

def tokenize(bytecode: bytes) -> iter:
    __i = 0
    while __i < len(bytecode):
        __len = instruction_length(opcode=bytecode[__i])
        yield _tokenize_instruction(chunk=bytecode[__i:__i+__len])
        __i = __i + __len

# TOKEN < #####################################################################

def detokenize(x: list) -> bytes:
    return b''
