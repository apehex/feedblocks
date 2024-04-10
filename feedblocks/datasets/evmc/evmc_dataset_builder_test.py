"""EVMC dataset."""

import tensorflow_datasets as tfds

import evmc_dataset_builder as edb

class BytecodeTest(tfds.testing.DatasetBuilderTestCase):
    """Tests for the EVMC dataset."""
    DATASET_CLASS = edb.Builder
    SPLITS = {'train': 3,}

if __name__ == '__main__':
    tfds.testing.test_main()
