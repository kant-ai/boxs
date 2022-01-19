"""Store data in a local filesystem"""
import datetime
import io
import json
import pathlib

from .data import DataInfo
from .storage import Storage, Reader, Writer, Run, Item


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
        self._runs_directory_path().mkdir(parents=True, exist_ok=True)
        self._runs_names_directory_path().mkdir(parents=True, exist_ok=True)

    def _data_file_paths(self, data_ref):
        base_path = self.root_directory / 'data' / data_ref.data_id / data_ref.run_id
        return base_path.with_suffix('.data'), base_path.with_suffix('.info')

    def _run_file_path(self, data_ref):
        return self._runs_directory_path() / data_ref.run_id / data_ref.data_id

    def _runs_directory_path(self):
        return self.root_directory / 'runs'

    def _runs_names_directory_path(self):
        return self._runs_directory_path() / '_named'

    def _run_directory_path(self, run_id):
        return self._runs_directory_path() / run_id

    def list_runs(self, box_id, limit=None):
        runs_directory = self._runs_directory_path()
        runs = [
            self._create_run_from_run_path(path)
            for path in runs_directory.iterdir()
            if path.is_dir() and path != self._runs_names_directory_path()
        ]
        runs = sorted(runs, key=lambda x: x.time, reverse=True)
        if limit is not None:
            runs = runs[:limit]
        return runs

    def list_items_in_run(self, box_id, run_id):
        run_directory = self._run_directory_path(run_id)
        if not run_directory.exists():
            raise ValueError("Unknown run " + run_id)

        name_directory = run_directory / '_named'
        named_items = {}
        if name_directory.exists():
            for named_link_file in name_directory.iterdir():
                name = named_link_file.name
                resolved_info_file = named_link_file.resolve()
                data_id = resolved_info_file.name
                named_items[data_id] = name

        items = [
            Item(
                path.name,
                run_id,
                named_items.get(path.name, ''),
                datetime.datetime.fromtimestamp(
                    path.stat().st_mtime,
                    tz=datetime.timezone.utc,
                ),
            )
            for path in run_directory.iterdir()
            if path.is_file()
        ]
        items = sorted(items, key=lambda x: x.time)
        return items

    def set_run_name(self, box_id, run_id, name):
        """
        Set the name of a run.

        The name can be updated and removed by providing `None`.

        Args;
            run_id (str): Run id of the run which should be named.
            name (Optional[str]): New name of the run. If `None`, an existing name
                will be removed.

        Returns:
            box.storage.Run: The run with its new name.
        """
        run_path = self._run_directory_path(run_id)

        self._remove_name_for_run(run_id)

        if name is not None:
            self._set_name_for_run_path(name, run_path)

        run = self._create_run_from_run_path(run_path)
        return run

    def exists(self, data_ref):
        _, info_file = self._data_file_paths(data_ref)
        return info_file.exists()

    def create_writer(self, data_ref, name=None, tags=None):
        tags = tags or {}
        data_file, info_file = self._data_file_paths(data_ref)
        run_file = self._run_file_path(data_ref)
        return _FileSystemWriter(data_ref, name, tags, data_file, info_file, run_file)

    def create_reader(self, data_ref):
        data_file, info_file = self._data_file_paths(data_ref)
        return _FileSystemReader(data_ref, data_file, info_file)

    def _get_run_names(self):
        name_directory = self._runs_names_directory_path()
        run_names = {}
        for named_link_file in name_directory.iterdir():
            name = named_link_file.name
            resolved_run_dir = named_link_file.resolve()
            run_id = resolved_run_dir.name
            if run_id in run_names:
                raise KeyError("Multiple names for single run")
            run_names[run_id] = name
        return run_names

    def _set_name_for_run_path(self, name, run_path):
        name_dir = self._runs_names_directory_path()
        name_dir.mkdir(exist_ok=True)
        name_symlink_file = name_dir / name
        name_symlink_file.symlink_to(run_path)

    def _remove_name_for_run(self, run_id):
        run_names = self._get_run_names()
        if run_id in run_names:
            name_dir = self._runs_names_directory_path()
            name_symlink_file = name_dir / run_names[run_id]
            name_symlink_file.unlink()

    def _create_run_from_run_path(self, run_path):
        run_names = self._get_run_names()
        run_id = run_path.name
        return Run(
            run_id,
            run_names.get(run_id),
            datetime.datetime.fromtimestamp(
                run_path.stat().st_mtime,
                tz=datetime.timezone.utc,
            ),
        )


class _FileSystemReader(Reader):
    def __init__(self, data_ref, data_file, info_file):
        super().__init__(data_ref)
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
    def __init__(  # pylint: disable=too-many-arguments
        self, data_ref, name, tags, data_file, info_file, run_file
    ):
        super().__init__(data_ref, name, tags)
        self.data_file = data_file
        self.info_file = info_file
        self.run_file = run_file
        self._meta = {}

    @property
    def meta(self):
        return self._meta

    def as_stream(self):
        self.data_file.parent.mkdir(parents=True, exist_ok=True)
        return io.FileIO(self.data_file, 'w')

    def write_info(self, origin, parents, meta):
        meta = dict(meta)
        meta.update(self.meta)
        data_info = DataInfo(
            self.data_ref,
            origin=origin,
            parents=parents,
            name=self.name,
            tags=self.tags,
            meta=meta,
        )

        self.info_file.parent.mkdir(parents=True, exist_ok=True)
        self.info_file.write_text(json.dumps(data_info.value_info()))
        run_dir = self.run_file.parent
        run_dir.mkdir(parents=True, exist_ok=True)
        self.run_file.touch()
        if self.name:
            name_dir = run_dir / '_named'
            name_dir.mkdir(exist_ok=True)
            name_symlink_file = name_dir / self.name
            name_symlink_file.symlink_to(self.run_file)
        return data_info
