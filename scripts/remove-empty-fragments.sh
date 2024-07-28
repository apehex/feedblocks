#!/bin/bash
find data/ -type f -regex '.*/\([0-9]+\)_to_\1\.parquet' -exec rm {} +
