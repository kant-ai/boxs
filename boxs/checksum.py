"""Checksum data to detect errors"""
import hashlib
import logging

from .errors import DataError
from .io import DelegatingStream
from .transform import DelegatingReader, DelegatingWriter, Transformer


logger = logging.getLogger(__name__)


class DataChecksumMismatch(DataError):
    """
    Exception that is raised if a checksum doesn't match.

    Attributes:
        item (boxs.storage.Item): The item where the mismatch occurred.
        expected (str): Checksum that was expected.
        calculated (str): Checksum that was actually calculated.
    """

    def __init__(self, item, expected, calculated):
        self.item = item
        self.expected = expected
        self.calculated = calculated
        super().__init__(
            f"{self.item} has wrong checksum '{self.calculated}'"
            f", expected '{self.expected}'"
        )


class ChecksumTransformer(Transformer):
    """
    Transformer that calculates and verifies the checksums of data.

    The transformer adds three values to the data's meta data:
        - 'checksum_digest': The hex-string representation of the checksum.
        - 'checksum_digest_size': The size in bytes of the checksum (not its
          representation).
        - 'checksum_algorithm': The hashing algorithm which is used for calculating
          the checksum. Currently, only 'blake2b' is supported.
    """

    def __init__(self, digest_size=32):
        """
        Create a new ChecksumTransformer.

        Args:
            digest_size (int): Length of the checksum in bytes.  Defaults to `32`.
                Since a checksum is represented as a hex-string, where a single byte
                is represented by two characters, the length of the resulting checksum
                string will be twice of the `digest_size`.
        """
        self.digest_size = digest_size

    def transform_reader(self, reader):
        return _ChecksumReader(reader, default_digest_size=self.digest_size)

    def transform_writer(self, writer):
        return _ChecksumWriter(writer, digest_size=self.digest_size)


class _ChecksumReader(DelegatingReader):
    def __init__(self, delegate, default_digest_size):
        super().__init__(delegate)
        self._verify = True
        if 'checksum_algorithm' not in self.delegate.meta:
            logger.warning(
                "No checksum algorithm given, disabling checksum verification",
            )
            self._verify = False
        elif self.delegate.meta['checksum_algorithm'] != 'blake2b':
            logger.warning(
                "Unknown checksum algorithm '%s', disabling checksum verification",
                self.delegate.meta['checksum_algorithm'],
            )
            self._verify = False
        self._digest_size = self.delegate.meta.get(
            'checksum_digest_size', default_digest_size
        )
        self._stream = None
        if hasattr(delegate, 'as_file'):
            self.as_file = self._as_file

    def read_value(self, value_type):
        result = value_type.read_value_from_reader(self)
        if self._stream is not None:
            found_checksum = self._stream.checksum
            if self._verify:
                expected_checksum = self.delegate.meta['checksum_digest']
                if expected_checksum != found_checksum:
                    raise DataChecksumMismatch(
                        self.item, expected_checksum, found_checksum
                    )
            logger.info("Checksum when reading %s: %s", self.item, found_checksum)
        else:
            logger.warning("Ignoring checksum when loading from local file.")
        return result

    def as_stream(self):
        self._stream = _ChecksumStream(self.delegate.as_stream(), self._digest_size)
        return self._stream

    def _as_file(self):
        # We don't calculate if we read from a local file.
        # Just return the original file path
        return self.delegate.as_file()


class _ChecksumWriter(DelegatingWriter):
    def __init__(self, delegate, digest_size=None):
        super().__init__(delegate)
        self._digest_size = digest_size
        self._stream = None

    def write_value(self, value, value_type):
        value_type.write_value_to_writer(value, self)
        checksum = self._stream.checksum
        self.meta['checksum_digest'] = checksum
        self.meta['checksum_digest_size'] = self._digest_size
        self.meta['checksum_algorithm'] = 'blake2b'
        logger.info("Checksum when writing %s: %s", self.item, checksum)

    def as_stream(self):
        self._stream = _ChecksumStream(self.delegate.as_stream(), self._digest_size)
        return self._stream


class _ChecksumStream(DelegatingStream):
    def __init__(self, delegate, digest_size):
        super().__init__(delegate)
        self.hash = hashlib.blake2b(digest_size=digest_size)
        self.checksum = None

    def close(self):
        self.checksum = self.hash.digest().hex()
        super().close()

    def read(self, size=-1):
        read_bytes = self.delegate.read(size)
        self.hash.update(read_bytes)
        return read_bytes

    def write(self, byte_buffer):
        written = super().write(byte_buffer)
        self.hash.update(byte_buffer[:written])
        return written
