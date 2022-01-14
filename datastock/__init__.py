"""
Automatically track data and artifacts

This package provides an API to automatically track data and artifacts in a machine
learning process without the need to manually think about file names or S3 keys. By
using its API the data is automatically stored and loaded in different versions per
execution which allows to compare the data between different runs.
"""
from .api import load, store, info
from .data import DataRef
from .filesystem import FileSystemStorage
from .origin import ORIGIN_FROM_FUNCTION_NAME, ORIGIN_FROM_NAME, ORIGIN_FROM_TAGS
from .stock_registry import get_stock
from .storage import Storage, Transformer, LoadDataFunction, StoreDataFunction
from .inputs import from_bytes, from_file, from_stream, from_string, from_value_as_json
from .outputs import as_bytes, as_file, as_stream, as_string, as_json_value


__version__ = '0.1.dev'
