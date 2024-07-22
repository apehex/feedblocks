# Feedblocks

Scrape, clean and solidify blockchain data.

## Data Collection

Blockchain transactions:

```shell
. .env
cryo contracts -a -r "$ETH_RPC_URL" -l 9 -b 19393000:19494000 -o data/ -s 'block_number' --no-report
```

Source code:

```shell
. .env
python feedblocks/main.py --eth-key 'AAAAAAAA' --eth-url 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}' --path 'data/ethereum/contracts/'
```

## Data Augmentation

```shell
docker run ethereum/solc:stable --standard-json < input.json > output.json
```

## Final Datasets

After processing, the data is available:

- in [Parquet format][pq-dataset]
- as a [Tensorflow dataset][tf-dataset]
- as a [Hugging Face dataset][hf-dataset]

[hf-dataset]: https://huggingface.co/datasets/apehex/evm-contracts
[pq-dataset]: ../data/
[tf-dataset]: ../feedblocks/datasets/evmc
