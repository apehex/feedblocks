# Feedblocks

> pipeline to train NN on blockchain data

## Collection

```shell
. .env
cryo contracts -a -r "$ETH_RPC_URL" -l 9 -b -6000: -o data/ -s 'block_number' --no-report
```

## Augmentation

```shell
docker run ethereum/solc:stable --standard-json < input.json > output.json
```
