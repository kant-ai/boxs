"""Store data in a local filesystem"""
import io
import json
import pathlib

from .storage import Storage, Reader, Writer


class FileSystemStorage(Storage):
    """Storage implementation that stores data items and meta-data in a directory."""

    def __init__(self, directory):
        """
        Create the storage.

        Args:
            directory (Union[str,pathlib.Path]): The path to the directory where the
                data will be stored.
        """
        self.root_directory = pathlib.Path(directory)

    def _file_paths(self, data_id, run_id):
        base_path = self.root_directory / data_id / run_id
        return base_path.with_suffix('.data'), base_path.with_suffix('.info')

    def exists(self, data_id, run_id):
        _, info_file = self._file_paths(data_id, run_id)
        return info_file.exists()

    def create_writer(self, data_id, run_id):
        data_file, info_file = self._file_paths(data_id, run_id)
        return _FileSystemWriter(data_id, run_id, data_file, info_file)

    def create_reader(self, data_id, run_id):
        data_file, info_file = self._file_paths(data_id, run_id)
        return _FileSystemReader(data_id, run_id, data_file, info_file)


class _FileSystemReader(Reader):
    def __init__(self, data_id, run_id, data_file, info_file):
        super().__init__(data_id, run_id)
        self.data_file = data_file
        self.info_file = info_file
        self._info = None

    @property
    def info(self):
        if self._info is None:
            self._info = json.loads(self.info_file.read_text())
        return self._info

    @property
    def meta(self):
        return self.info['meta']

    def as_stream(self):
        return io.FileIO(self.data_file, 'r')


class _FileSystemWriter(Writer):
    def __init__(self, data_id, run_id, data_file, info_file):
        super().__init__(data_id, run_id)
        self.data_file = data_file
        self.info_file = info_file
        self._meta = {}

    @property
    def meta(self):
        return self._meta

    def as_stream(self):
        self.data_file.parent.mkdir(parents=True, exist_ok=True)
        return io.FileIO(self.data_file, 'w')

    def write_info(self, info):
        self.info_file.parent.mkdir(parents=True, exist_ok=True)
        self.info_file.write_text(json.dumps(info))
