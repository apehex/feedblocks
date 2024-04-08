# EVM Bytecode

## Description

EVMB (Ethereum Virtual Machine Bytecode) is a training dataset for tasks involving blockchain contracts.
It can be used to encode & decode EVM bytecode and augment further data analysis and generation on blockchain data.

## Metadata

- homepage: [https://github.com/apehex/feedblocks][github-project]
- source code: [tfds.datasets.evmb.Builder][github-tfds]
- version: 0.1.0
- download size: 1 MiB
- auto-cached: No

## Features

## Preprocessing

The HEX bytecode has been processed as follows:

1. the bytecode is disassembled
2. for each instruction:
    2.1 the bytes of the instruction are parsed as `opcode + data`
    2.2 the opcode byte is tokenized using one-hot encoding (depth 256)
    2.3 the folowing 0 to 32 data bytes:
        2.3.1 are interpreted as uint256 (32 * 8 bits) in little endian
        2.3.2 the list of 256 bits is turned into a tensor
    2.4 the opcode and data vectors are concatenated

This results in a dataset of shape `(B, T, 512)`.

[github-project]: https://github.com/apehex/feedblocks/
[github-tfds]: https://github.com/apehex/feedblocks/tree/main/feedblocks/datasets/bytecode/evmb_dataset_builder.py
