"""Value type definitions for pandas specific classes"""
import codecs
import io
import logging


logger = logging.getLogger(__name__)


try:
    import pandas

    from .value_types import StringValueType

    class PandasDataFrameCsvValueType(StringValueType):
        """
        A value type for storing and loading pandas DataFrame.
        """

        def supports(self, value):
            return isinstance(value, pandas.DataFrame)

        def write_value_to_writer(self, value, writer):
            with writer.as_stream() as stream, io.TextIOWrapper(
                stream, encoding=self._default_encoding
            ) as text_writer:
                value.to_csv(text_writer)
            writer.meta['encoding'] = self._default_encoding

        def read_value_from_reader(self, reader):
            encoding = reader.meta.get('encoding', self._default_encoding)
            with reader.as_stream() as stream:
                text_stream = codecs.getreader(encoding)(stream)
                setattr(text_stream, 'mode', 'r')
                result = pandas.read_csv(text_stream, encoding=encoding)
                return result

except ImportError as error:
    logger.warning(
        "Unable to load pandas package %s, pandas specific "
        "value types are not available.",
        error,
    )
