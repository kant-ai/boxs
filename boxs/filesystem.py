"""Store data in a local filesystem"""
import datetime
import io
import logging
import json
import os.path
import pathlib
import shutil

from .errors import BoxNotFound, DataCollision, DataNotFound, NameCollision, RunNotFound
from .storage import Storage, Reader, Writer, Run, Item


logger = logging.getLogger(__name__)


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

    def _data_file_paths(self, item):
        base_path = (
            self.root_directory / item.box_id / 'data' / item.data_id / item.run_id
        )
        return base_path.with_suffix('.data'), base_path.with_suffix('.info')

    def _run_file_path(self, item):
        return self._runs_directory_path(item.box_id) / item.run_id / item.data_id

    def _runs_directory_path(self, box_id):
        path = self.root_directory / box_id / 'runs'
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _runs_names_directory_path(self, box_id):
        path = self._runs_directory_path(box_id) / '_named'
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _run_directory_path(self, box_id, run_id):
        return self._runs_directory_path(box_id) / run_id

    def _box_directory_path(self, box_id):
        return self.root_directory / box_id

    def list_runs(self, box_id, limit=None, name_filter=None):
        box_directory = self._box_directory_path(box_id)
        logger.debug("List runs from directory %s", box_directory)
        if not box_directory.exists():
            raise BoxNotFound(box_id)

        runs = self._list_runs_in_box(box_id)
        runs = sorted(runs, key=lambda x: x.time, reverse=True)
        if name_filter is not None:
            runs = list(filter(lambda x: (x.name or '').startswith(name_filter), runs))
        if limit is not None:
            runs = runs[:limit]
        return runs

    def _list_runs_in_box(self, box_id):
        runs_directory = self._runs_directory_path(box_id)
        runs = [
            self._create_run_from_run_path(box_id, path)
            for path in runs_directory.iterdir()
            if path.is_dir() and path != self._runs_names_directory_path(box_id)
        ]
        return runs

    def list_items(self, item_query):
        box_id = item_query.box
        box_directory = self._box_directory_path(box_id)
        if not box_directory.exists():
            raise BoxNotFound(box_id)

        logger.debug("List items with query %s", item_query)

        runs = self._list_runs_in_box(box_id)
        if item_query.run:
            runs = [
                run
                for run in runs
                if run.run_id.startswith(item_query.run or '')
                or (run.name or '').startswith(item_query.run or '')
            ]
        runs = sorted(runs, key=lambda x: x.time)

        all_items = []
        for run in runs:
            items = self._get_items_in_run(box_id, run.run_id)
            items = sorted(items, key=lambda x: x.time)
            all_items.extend(
                (
                    item
                    for item in items
                    if item.data_id.startswith(item_query.data or '')
                    or (item.name or '').startswith(item_query.data or '')
                )
            )
        return all_items

    def set_run_name(self, box_id, run_id, name):
        logger.debug("Set name of run %s in box %s to %s", run_id, box_id, name)

        box_directory = self._box_directory_path(box_id)
        if not box_directory.exists():
            raise BoxNotFound(box_id)

        run_directory = self._run_directory_path(box_id, run_id)
        if not run_directory.exists():
            raise RunNotFound(box_id, run_id)

        run_path = self._run_directory_path(box_id, run_id)

        self._remove_name_for_run(box_id, run_id)

        if name is not None:
            self._set_name_for_run_path(box_id, name, run_path)

        run = self._create_run_from_run_path(box_id, run_path)
        return run

    def delete_run(self, box_id, run_id):
        run_directory = self._run_directory_path(box_id, run_id)
        if not run_directory.exists():
            raise RunNotFound(box_id, run_id)

        items = self._get_items_in_run(box_id, run_id)
        for item in items:
            data_file, info_file = self._data_file_paths(item)
            data_file.unlink()
            info_file.unlink()
        shutil.rmtree(run_directory)

    def create_writer(self, item, name=None, tags=None):
        logger.debug("Create writer for %s", item)
        tags = tags or {}
        data_file, info_file = self._data_file_paths(item)
        run_file = self._run_file_path(item)
        return _FileSystemWriter(item, name, tags, data_file, info_file, run_file)

    def create_reader(self, item):
        logger.debug("Create reader for %s", item)
        data_file, info_file = self._data_file_paths(item)
        return _FileSystemReader(item, data_file, info_file)

    def _get_run_names(self, box_id):
        name_directory = self._runs_names_directory_path(box_id)
        run_names = {}
        for named_link_file in name_directory.iterdir():
            name = named_link_file.name
            resolved_run_dir = named_link_file.resolve()
            run_id = resolved_run_dir.name
            run_names[run_id] = name
        return run_names

    def _set_name_for_run_path(self, box_id, name, run_path):
        name_dir = self._runs_names_directory_path(box_id)
        name_dir.mkdir(exist_ok=True)
        name_symlink_file = name_dir / name
        symlink_path = os.path.relpath(run_path, name_dir)
        name_symlink_file.symlink_to(symlink_path)

    def _remove_name_for_run(self, box_id, run_id):
        run_names = self._get_run_names(box_id)
        if run_id in run_names:
            name_dir = self._runs_names_directory_path(box_id)
            name_symlink_file = name_dir / run_names[run_id]
            name_symlink_file.unlink()

    def _get_items_in_run(self, box_id, run_id):
        named_items = self._get_item_names_in_run(box_id, run_id)
        items = [
            Item(
                box_id,
                path.name,
                run_id,
                named_items.get(path.name, ''),
                datetime.datetime.fromtimestamp(
                    path.stat().st_mtime,
                    tz=datetime.timezone.utc,
                ),
            )
            for path in self._run_directory_path(box_id, run_id).iterdir()
            if path.is_file()
        ]
        return items

    def _get_item_names_in_run(self, box_id, run_id):
        name_directory = self._run_directory_path(box_id, run_id) / '_named'
        named_items = {}
        if name_directory.exists():
            for named_link_file in name_directory.iterdir():
                name = named_link_file.name
                resolved_info_file = named_link_file.resolve()
                data_id = resolved_info_file.name
                named_items[data_id] = name
        return named_items

    def _create_run_from_run_path(self, box_id, run_path):
        run_names = self._get_run_names(box_id)
        run_id = run_path.name
        return Run(
            box_id,
            run_id,
            run_names.get(run_id),
            datetime.datetime.fromtimestamp(
                run_path.stat().st_mtime,
                tz=datetime.timezone.utc,
            ),
        )


class _FileSystemReader(Reader):
    def __init__(self, item, data_file, info_file):
        super().__init__(item)
        self.data_file = data_file
        self.info_file = info_file
        self._info = None

    @property
    def info(self):
        if not self.info_file.exists():
            raise DataNotFound(self.item.box_id, self.item.data_id, self.item.run_id)
        if self._info is None:
            self._info = json.loads(self.info_file.read_text())
        return self._info

    def as_stream(self):
        if not self.data_file.exists():
            raise DataNotFound(self.item.box_id, self.item.data_id, self.item.run_id)
        return io.FileIO(self.data_file, 'r')

    def as_file(self):
        """
        Returns the file path containing the data.

        Additional method that is especially implemented for the boxs CLI to inspect a
        FileSystemStorage without having to copy files.

        Returns:
            pathlib.Path: The file path to the file containing the data.
        """
        return self.data_file


class _FileSystemWriter(Writer):
    def __init__(  # pylint: disable=too-many-arguments
        self, item, name, tags, data_file, info_file, run_file
    ):
        super().__init__(item, name, tags)
        self.data_file = data_file
        self.info_file = info_file
        self.run_file = run_file

    def as_stream(self):
        self.data_file.parent.mkdir(parents=True, exist_ok=True)
        if self.data_file.exists():
            raise DataCollision(self.item.box_id, self.item.data_id, self.item.run_id)
        return io.FileIO(self.data_file, 'w')

    def write_info(self, info):
        self.info_file.parent.mkdir(parents=True, exist_ok=True)
        if self.info_file.exists():
            raise DataCollision(self.item.box_id, self.item.data_id, self.item.run_id)
        self.info_file.write_text(json.dumps(info))
        run_dir = self.run_file.parent
        run_dir.mkdir(parents=True, exist_ok=True)
        self.run_file.touch()
        if self.name:
            name_dir = run_dir / '_named'
            name_dir.mkdir(exist_ok=True)
            name_symlink_file = name_dir / self.name
            if name_symlink_file.exists():
                raise NameCollision(
                    self.item.box_id,
                    self.item.data_id,
                    self.item.run_id,
                    self.name,
                )
            symlink_path = os.path.relpath(self.run_file, name_dir)
            name_symlink_file.symlink_to(symlink_path)
