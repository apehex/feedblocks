# EVM Contracts

## Description

EVMC (Ethereum Virtual Machine Contracts) is a collection of smart contracts from the ETH blockchain.

In particular, each sample holds the creation and runtime bytecodes.
When available, the sources are also included.

## Metadata

- homepage: [https://github.com/apehex/feedblocks][github-project]
- source code: [feedblocks.datasets.evmc.Evmc][github-tfds]
- version: 0.1.1
- download size: 5 GB
- auto-cached: no

| Split         | Samples   |
| ------------- | --------- |
| 'ethereum'    | 1294252   |

## Features

```python
FeaturesDict({
    'block_number': tfds.features.Tensor(shape=(), dtype=tf.dtypes.int32),
    'block_hash': tfds.features.Text(),
    'transaction_hash': tfds.features.Text(),
    'deployer_address': tfds.features.Text(),
    'factory_address': tfds.features.Text(),
    'contract_address': tfds.features.Text(),
    'creation_bytecode': tfds.features.Text(),
    'runtime_bytecode': tfds.features.Text(),
    'solidity_sourcecode': tfds.features.Text(),})
```

## Features

### Block Number

The block number is the only integer feature.

### Solidity Sources

The sources all have open source licenses.
They were collected from block explorer APIs like [Etherscan][etherscan-api].

The sources are formatted as [standard JSON input][solidity-docs-json] for the solidity compiler.

The resulting JSON is then encoded using UTF-8 into a single string.

### HEX Data

All the other features are HEX encoded into strings, **without** the `0x` prefix.

For example:

```python
{
    'block_number': 20155815,
    'block_hash': b'fcddf33b1b5a728a40588eda60262639201ac0d3f611f08286a9e2ef65576111',
    'transaction_hash': b'ec3723ffb8a3bbb8b83b25481f61cbfc46383fc88ff8eb364186b53aa226e4bf'
    'deployer_address': b'ba57abe375903838b5c19709e96dae12191fa37e',
    'factory_address': b'0000000000b3f879cb30fe243b4dfee438691c04',
    'contract_address': b'eff10e7d4feef60ed9b9e9bb9fee12c2504bd0ba',
    'creation_bytecode': b'756eb3f879cb30fe243b4dfee438691c043318585733ff6000526016600af3',
    'runtime_bytecode': b'6eb3f879cb30fe243b4dfee438691c043318585733ff',
    'solidity_sourcecode': b'',}
```

[etherscan-api]: https://docs.etherscan.io/api-endpoints/contracts
[github-project]: https://github.com/apehex/feedblocks/
[github-tfds]: https://github.com/apehex/feedblocks/tree/main/feedblocks/datasets/evmc
[solidity-docs-json]: https://docs.soliditylang.org/en/v0.8.26/using-the-compiler.html#compiler-input-and-output-json-description
