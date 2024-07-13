"""Dataset of EVM contracts: metadata, source and bytecode."""

import pyarrow.dataset as pd
import tensorflow as tf
import tensorflow_datasets as tfds

import feedblocks.data as fd

# BUILD #######################################################################

class Evmc(tfds.core.GeneratorBasedBuilder):
    """DatasetBuilder for the EVMC dataset."""

    VERSION = tfds.core.Version('0.1.0')
    RELEASE_NOTES = {'0.1.0': 'Initial release.',}

    def __init__(self, **kwargs) -> None:
        super(Evmc, self).__init__(**kwargs)
        self._datasets = {
            __chain: fd.load(chain=__chain, dataset='contracts', path='../../../data/{chain}/{dataset}/')
            for __chain in ['ethereum']}

    def _info(self) -> tfds.core.DatasetInfo:
        """Returns the dataset metadata."""
        return self.dataset_info_from_configs(
            homepage='https://github.com/apehex/feedblocks/',
            supervised_keys=None,
            disable_shuffling=False,
            features=tfds.features.FeaturesDict({
                'block_number': tfds.features.Tensor(shape=(), dtype=tf.dtypes.int32),
                'block_hash': tfds.features.Text(),
                'transaction_hash': tfds.features.Text(),
                'deployer_address': tfds.features.Text(),
                'factory_address': tfds.features.Text(),
                'contract_address': tfds.features.Text(),
                'creation_bytecode': tfds.features.Text(),
                'runtime_bytecode': tfds.features.Text(),
                'solidity_sourcecode': tfds.features.Text(),}))

    def _format_field(self, record: dict, key: str, default: bytes=b'') -> bytes:
        __value = record.get(key, default)
        return default if __value is None else __value

    def _format_record(self, record: dict) -> dict:
        return {
            'block_number': self._format_field(record=record, key='block_number', default=0),
            'block_hash': self._format_field(record=record, key='block_hash', default=b'').hex(),
            'transaction_hash': self._format_field(record=record, key='transaction_hash', default=b'').hex(),
            'deployer_address': self._format_field(record=record, key='deployer', default=b'').hex(),
            'factory_address': self._format_field(record=record, key='factory', default=b'').hex(),
            'contract_address': self._format_field(record=record, key='contract_address', default=b'').hex(),
            'creation_bytecode': self._format_field(record=record, key='init_code', default=b'').hex(),
            'runtime_bytecode': self._format_field(record=record, key='code', default=b'').hex(),
            'solidity_sourcecode': self._format_field(record=record, key='source_code', default=b'').decode('utf-8'),}

    def _split_generators(self, dl_manager: tfds.download.DownloadManager):
        """Generates the data splits."""
        return {'ethereum': self._generate_examples(chain='ethereum')}

    def _generate_examples(self, chain: str) -> iter:
        """Produces contract samples with runtime and creation bytecode."""
        __i = 0
        for __fragment in self._datasets[chain].fragments:
            for __batch in __fragment.to_batches(batch_size=64):
                for __record in __batch.to_pylist():
                    yield (__i, self._format_record(record=__record))
                    __i += 1