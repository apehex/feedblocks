# EVM Contracts

## Description

EVMC (Ethereum Virtual Machine Contracts) is a training dataset for tasks involving blockchain contracts.
It can be used to encode & decode EVM contracts and augment further data analysis and generation on blockchain data.

## Metadata

- homepage: [https://github.com/apehex/feedblocks][github-project]
- source code: [tfds.datasets.evmc.Builder][github-tfds]
- version: 0.1.0
- download size: 402 MiB
- auto-cached: no

## Features

```python
FeaturesDict({
    'solidity_sourcecode': Tensor(shape=(None,), dtype=int32),
    'creation_bytecode': Tensor(shape=(None,), dtype=int32),
    'runtime_bytecode': Tensor(shape=(None,), dtype=int32),
})
```

## Preprocessing

### Solidity Sources

- the sources are formatted as [standard JSON input][solidity-docs-json] for the solidity compiler
- these text sources are encoded using UTF-8

### EVM Bytecode

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
[github-tfds]: https://github.com/apehex/feedblocks/tree/main/feedblocks/datasets/bytecode/evmc_dataset_builder.py
[solidity-docs-json]: https://docs.soliditylang.org/en/v0.8.26/using-the-compiler.html#compiler-input-and-output-json-description
