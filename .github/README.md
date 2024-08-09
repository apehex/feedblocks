# Feedblocks

Scrape, clean and solidify blockchain data.

## Data Collection

Blockchain transactions:

```shell
cryo contracts -a -r "$ETH_RPC_URL" -l 9 -b 19393000:19494000 -o data/ -s 'block_number' --no-report
```

Source code:

```shell
python feedblocks/main.py --scrape --eth-key 'AAAAAAAA' --eth-url 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}' --output 'data/ethereum/contracts/'
python feedblocks/main.py -s -k 'AAAAAAAA' -u 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}' -o 'data/ethereum/contracts/'
```

## Data Indexing

Import the data scraped by `cryo`:

```shell
python feedblocks/main.py --input '/mnt/data/' --output 'data/ethereum/contracts/'
python feedblocks/main.py -i '/mnt/data/' -o 'data/ethereum/contracts/'
```

Split the large table fragments:

```shell
python feedblocks/main.py --chunk
python feedblocks/main.py -c
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
