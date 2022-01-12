"""Functions for I/O of data"""
import io


class DelegatingStream(io.RawIOBase):
    """Stream that delegates to another stream."""

    def __init__(self, delegate):
        """
        Creates a new DelegatingStream.

        Args:
            delegate (io.RawIOBase): The delegate stream.
        """
        self.delegate = delegate
        super().__init__()

    def close(self):
        self.delegate.close()

    @property
    def closed(self):
        """Property that returns if a stream is closed."""
        return self.delegate.closed

    def flush(self):
        self.delegate.flush()

    def seek(self, offset, whence=io.SEEK_SET):
        return self.delegate.seek(offset, whence)

    def seekable(self):
        return self.delegate.seekable()

    def tell(self):
        return self.delegate.tell()

    def truncate(self, size=None):
        return self.delegate.truncate(size)

    def writable(self):
        return self.delegate.writable()

    def readinto(self, byte_buffer):
        return self.delegate.readinto(byte_buffer)

    def write(self, byte_buffer):
        return self.delegate.write(byte_buffer)
