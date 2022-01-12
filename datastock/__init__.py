"""
Automatically track data and artifacts

This package provides an API to automatically track data and artifacts in a machine
learning process without the need to manually think about file names or S3 keys. By
using its API the data is automatically stored and loaded in different versions per
execution which allows to compare the data between different runs.
"""
from .api import load, store
from .data import DataRef
from .filesystem import FileSystemStorage
from .origin import ORIGIN_FROM_FUNCTION_NAME, ORIGIN_FROM_NAME, ORIGIN_FROM_TAGS
from .stock import Stock, get_stock
from .storage import Storage, Transformer, LoadDataFunction, StoreDataFunction

__version__ = '0.1.dev'
