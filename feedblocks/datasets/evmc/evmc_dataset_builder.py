"""Dataset of EVM contracts: source and bytecode."""

import pyarrow.dataset as pd
import tensorflow as tf
import tensorflow_datasets as tfds

class Builder(tfds.core.GeneratorBasedBuilder):
    """DatasetBuilder for the EVMC dataset."""

    VERSION = tfds.core.Version('0.1.0')
    RELEASE_NOTES = {'0.1.0': 'Initial release.',}

    def __init__(self, **kwargs) -> None:
        super(Builder, self).__init__(**kwargs)
        __dataset = pd.dataset('../../../data/ethereum/contracts/', format='parquet')
        self.__batch_iter = __dataset.to_batches(columns=['source_code', 'init_code', 'code'], batch_size=128)

    def _info(self) -> tfds.core.DatasetInfo:
        """Returns the dataset metadata."""
        return self.dataset_info_from_configs(
            homepage='https://github.com/apehex/feedblocks/',
            supervised_keys=None,
            disable_shuffling=False,
            features=tfds.features.FeaturesDict({
                'solidity_sourcecode': tfds.features.Tensor(shape=(None,), dtype=tf.dtypes.int32),
                'creation_bytecode': tfds.features.Tensor(shape=(None,), dtype=tf.dtypes.int32),
                'runtime_bytecode': tfds.features.Tensor(shape=(None,), dtype=tf.dtypes.int32),})) # TODO (None, 512 after tokenization)

    def _split_generators(self, dl_manager: tfds.download.DownloadManager):
        """Generates the data splits."""
        return {'train': self._generate_examples()}

    def _generate_examples(self) -> iter:
        """Produces contract samples with runtime and creation bytecode."""
        __i = -1
        for __batch in self.__batch_iter:
            for __row in __batch.to_pylist():
                __i += 1
                yield (__i, {
                    'solidity_sourcecode': list(__row.get('source_code', b'') or b''),
                    'creation_bytecode': list(__row.get('init_code', b'') or b''),
                    'runtime_bytecode': list(__row.get('code', b'') or b'')})
