"""
Automatically track data and artifacts

This package provides an API to automatically track data and artifacts in a machine
learning process without the need to manually think about file names or S3 keys. By
using its API the data is automatically stored and loaded in different versions per
execution which allows to compare the data between different runs.
"""
from .api import load, store, info
from .box import Box
from .box_registry import get_box
from .checksum import ChecksumTransformer, DataChecksumMismatch
from .config import get_config
from .data import DataInfo, DataRef
from .filesystem import FileSystemStorage
from .origin import ORIGIN_FROM_FUNCTION_NAME, ORIGIN_FROM_NAME, ORIGIN_FROM_TAGS
from .run import get_run_id, set_run_id
from .statistics import StatisticsTransformer
from .storage import Storage
from .transform import Transformer
from .value_types import (
    ValueType,
    BytesValueType,
    FileValueType,
    StreamValueType,
    StringValueType,
    JsonValueType,
)

__version__ = '0.1'
