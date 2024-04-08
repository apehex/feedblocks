"""Dataset of EVM contract bytecode."""

import pyarrow.parquet as pq
import tensorflow as tf
import tensorflow_datasets as tfds

class Builder(tfds.core.GeneratorBasedBuilder):
    """DatasetBuilder for the EVMB dataset."""

    VERSION = tfds.core.Version('0.1.0')
    RELEASE_NOTES = {'0.1.0': 'Initial release.',}

    def __init__(self, **kwargs) -> None:
        super(Builder, self).__init__(**kwargs)
        __database = pq.ParquetDataset('../../../data/ethereum/contracts/')
        self.__batch_iter = __database._dataset.to_batches(columns=['code'], batch_size=128)

    def _info(self) -> tfds.core.DatasetInfo:
        """Returns the dataset metadata."""
        return self.dataset_info_from_configs(
                features=tfds.features.FeaturesDict({'runtime': tfds.features.Tensor(shape=(None, None,), dtype=tf.dtypes.int32),}), # TODO (None, None, 512 after tokenization)
                supervised_keys=None,
                disable_shuffling=False,
                homepage='https://github.com/apehex/feedblocks/',
        )

    def _split_generators(self, dl_manager: tfds.download.DownloadManager):
        """Produce sample of runtime and creation bytecode of contracts deployed on the ETH blockchain."""
        return {
            'runtime': self._generate_examples(),
        }

    def _generate_examples(self) -> iter:
        """Yields samples."""
        # TODO idem creation bytecode
        # TODO tokenize depending on config
        # TODO split dataset in 2? one for runtime bytecode, the other for creation?
        for __batch in self.__batch_iter:
            __dict = __batch.to_pydict()
            for __bytecode in __dict['code']:
                yield 'test', list(__bytecode)
