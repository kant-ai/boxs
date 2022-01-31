"""Collecting statistics about data"""
import datetime

from .io import DelegatingStream
from .transform import Transformer, DelegatingWriter


class StatisticsTransformer(Transformer):
    """
    Transformer that collects statistics about data items.

    This transformer gathers statistics like size of the data, number of lines in the
    data or time when it was stored and adds those as additional values in the data's
    meta-data. The following meta-data values are set:

    - 'size_in_bytes' as int
    - 'number_of_lines' as int
    - 'store_start' Timestamp in ISO-format when the storing of the data started.
    - 'store_end' Timestamp in ISO-format when the storing of the data finished.

    """

    def transform_writer(self, writer):
        return _StatisticsWriter(writer)


class _CountingStreamWrapper(DelegatingStream):
    def __init__(self, delegate, meta):
        super().__init__(delegate)
        self._linebreaks = 0
        self._bytes_written = 0
        self._start = datetime.datetime.now(datetime.timezone.utc)
        self._end = None
        self._meta = meta

    def close(self):
        super().close()
        self._end = datetime.datetime.now(datetime.timezone.utc)
        self._meta['size_in_bytes'] = self._bytes_written
        self._meta['number_of_lines'] = self._linebreaks
        self._meta['store_start'] = self._start.isoformat(
            timespec='milliseconds',
        )
        self._meta['store_end'] = self._start.isoformat(timespec='milliseconds')

    def write(self, byte_buffer):
        written = super().write(byte_buffer)
        self._bytes_written += written
        self._linebreaks += byte_buffer.count(ord('\n'))


class _StatisticsWriter(DelegatingWriter):
    def as_stream(self):
        return _CountingStreamWrapper(self.delegate.as_stream(), self.delegate.meta)
