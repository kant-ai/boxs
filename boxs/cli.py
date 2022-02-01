"""Command line interface"""
import argparse
import codecs
import collections.abc
import datetime
import io
import json
import logging
import math
import pathlib
import shutil
import subprocess
import sys

from boxs.box_registry import get_box
from boxs.config import get_config
from boxs.data import DataRef
from boxs.errors import BoxsError
from boxs.graph import write_graph_of_refs
from boxs.storage import ItemQuery, Run
from boxs.value_types import FileValueType


logger = logging.getLogger(__name__)


def main(argv=None):
    """
    main() method of our command line interface.

    Args:
        argv (List[str]): Command line arguments given to the function. If `None`, the
            arguments are taken from `sys.argv`.
    """
    argv = argv or sys.argv[1:]

    boxs_home_dir = pathlib.Path.home() / '.boxs'
    boxs_home_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(boxs_home_dir / 'cli.log')
    file_handler.level = logging.DEBUG
    file_handler.setFormatter(
        logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[file_handler],
    )

    logger.debug("Command line arguments: %s", argv)

    parser = argparse.ArgumentParser(
        prog='boxs',
        description="Allows to inspect and manipulate boxes that are used for "
        "storing data items using the python 'boxs' library.",
    )
    parser.set_defaults(command=lambda _: parser.print_help())
    parser.add_argument(
        '-b',
        '--default-box',
        metavar='BOX',
        dest='default_box',
        help="The id of the default box to use. If not set, the default is taken "
        "from the BOXS_DEFAULT_BOX environment variable.",
    )
    parser.add_argument(
        '-i',
        '--init-module',
        dest='init_module',
        help="A python module that should be automatically loaded. If not set, the "
        "default is taken from the BOXS_INIT_MODULE environment variable.",
    )
    parser.add_argument(
        '-j',
        '--json',
        dest='json',
        action='store_true',
        help="Print output as json",
    )

    subparsers = parser.add_subparsers(help="Commands")

    _add_list_runs_command(subparsers)
    _add_name_run_command(subparsers)
    _add_delete_run_command(subparsers)
    _add_clean_runs_command(subparsers)

    _add_list_command(subparsers)
    _add_info_command(subparsers)
    _add_diff_command(subparsers)

    _add_export_command(subparsers)
    _add_graph_command(subparsers)

    args = parser.parse_args(argv)

    config = get_config()

    if args.default_box:
        config.default_box = args.default_box
    if args.init_module:
        config.init_module = args.init_module

    try:
        args.command(args)
    except BoxsError as error:
        _print_error(error, args)


def _add_list_runs_command(subparsers):
    list_runs_parser = subparsers.add_parser("list-runs", help="List runs")
    list_runs_parser.add_argument(
        '-f',
        '--name-filter',
        metavar='FILTER',
        dest='filter',
        help="Only list runs whose name begins with FILTER.",
    )
    list_runs_parser.add_argument(
        '-l',
        '--limit',
        metavar='LIMIT',
        dest='limit',
        type=int,
        help="Only list the <LIMIT> last runs.",
    )
    list_runs_parser.set_defaults(command=list_runs_command)


def list_runs_command(args):
    """
    Function that lists runs.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """

    box = get_box()
    storage = box.storage
    logger.info("Listing all runs in box %s", box.box_id)
    runs = storage.list_runs(box.box_id, name_filter=args.filter, limit=args.limit)
    _print_result("List runs", runs, args)


def _add_name_run_command(subparsers):
    name_run_parser = subparsers.add_parser("name-run", help="Set the name of a run")
    name_run_parser.add_argument(
        metavar='RUN',
        dest='run',
        help="Run id or name, can be just the first characters.",
    )
    name_run_parser.add_argument(
        '-n',
        '--name',
        dest='name',
        default=None,
        help="The new name of the run, if left out, the current name will be removed.",
    )
    name_run_parser.set_defaults(command=name_run_command)


def name_run_command(args):
    """
    Command that allows to set a name for a specific run.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    box = get_box()
    storage = box.storage
    run = _get_run_from_args(args)
    if run is None:
        return
    logger.info(
        "Setting name of run %s in box %s to %s",
        run.run_id,
        box.box_id,
        args.name,
    )
    run = storage.set_run_name(box.box_id, run.run_id, args.name)
    _print_result(f"Run name set {run.run_id}", [run], args)


def _add_delete_run_command(subparsers):
    delete_run_parser = subparsers.add_parser("delete-run", help="Delete a run")
    delete_run_parser.add_argument(
        metavar='RUN',
        dest='run',
        help="Run id or name, can be just the first characters.",
    )
    delete_run_parser.add_argument(
        '-q',
        '--quiet',
        dest='quiet',
        action='store_true',
        help="Don't ask for confirmation.",
    )
    delete_run_parser.set_defaults(command=delete_run_command)


def delete_run_command(args):
    """
    Command that allows to delete a specific run.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    box = get_box()
    storage = box.storage
    run = _get_run_from_args(args)
    if run is None:
        return
    logger.info(
        "Deleting run %s in box %s",
        run.run_id,
        box.box_id,
    )
    if not args.quiet:
        if not _confirm(
            f"Really delete the run {run.run_id}? There might be other "
            f"runs referencing data from it. (y/N)"
        ):
            return
    storage.delete_run(box.box_id, run.run_id)
    _print_result(f"Run {run.run_id} deleted.", [run], args)


def _add_clean_runs_command(subparsers):
    clean_runs_parser = subparsers.add_parser("clean-runs", help="Clean runs")
    clean_runs_parser.add_argument(
        '-n',
        '--remove-named',
        dest='remove_named',
        action='store_true',
        help="Delete runs which have names.",
    )
    clean_runs_parser.add_argument(
        '-r',
        '--preserve-runs',
        metavar='COUNT',
        dest='count',
        default=5,
        type=int,
        help="Preserve the <COUNT> last runs. Defaults to 5.",
    )
    clean_runs_parser.add_argument(
        '-d',
        '--ignore-dependencies',
        dest='ignore_dependencies',
        action='store_true',
        help="Delete runs which contain data items referenced by kept runs.",
    )
    clean_runs_parser.add_argument(
        '-q',
        '--quiet',
        dest='quiet',
        action='store_true',
        help="Don't ask for confirmation.",
    )
    clean_runs_parser.set_defaults(command=clean_runs_command)


def clean_runs_command(args):
    """
    Function that removes old runs.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """

    box = get_box()
    storage = box.storage
    logger.info("Removing runs in box %s", box.box_id)
    runs = storage.list_runs(box.box_id)

    runs_to_keep = set(runs[: args.count])

    if not args.remove_named:
        _keep_runs_with_name(runs, runs_to_keep)

    if not args.ignore_dependencies:
        _keep_runs_that_are_dependencies(runs_to_keep, storage)

    runs_to_delete = [run for run in runs if run not in runs_to_keep]
    _print_result("Delete runs", runs_to_delete, args)

    if runs_to_delete:
        if not args.quiet:
            if not _confirm("Really delete all listed runs? (y/N)"):
                return

        for run in runs_to_delete:
            box.storage.delete_run(run.box_id, run.run_id)


def _keep_runs_that_are_dependencies(runs_to_keep, storage):

    queue = collections.deque(runs_to_keep)

    def _keep_ancestors_of_ref(ref):
        info = ref.info
        for parent in info.parents:
            run = Run(parent.box_id, parent.run_id)
            if run not in runs_to_keep:
                runs_to_keep.add(run)
                queue.append(run)
            _keep_ancestors_of_ref(parent)

    while queue:
        run = queue.popleft()
        query = ItemQuery.from_fields(box=run.box_id, run=run.run_id)
        items = storage.list_items(query)
        for item in items:
            ref = DataRef.from_item(item)
            _keep_ancestors_of_ref(ref)


def _keep_runs_with_name(runs, runs_to_keep):
    for run in runs:
        if run.name is not None:
            runs_to_keep.add(run)


def _add_list_command(subparsers):
    list_parser = subparsers.add_parser("list", help="List runs or items in a run")
    list_parser.add_argument(
        nargs=1,
        metavar='QUERY',
        dest='query',
        default=None,
        help="The query in format [<box>:<data>:<run>] for the items which should be"
        " listed. ",
    )
    list_parser.set_defaults(command=list_command)


def list_command(args):
    """
    Function that lists the data items of a specific run.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    item_query = _parse_query(args.query[0])
    logger.info("Listing items by query %s", item_query)

    box = get_box(item_query.box)
    item_query.box = box.box_id
    items = box.storage.list_items(item_query)

    if len(items) == 0:
        _print_error(f"No items found by query {args.query[0]}", args)
        return
    _print_result(f"List items {item_query}", items, args)


def _add_info_command(subparsers):
    info_parser = subparsers.add_parser("info", help="Info about an item")
    info_parser.add_argument(
        nargs=1,
        metavar='QUERY',
        dest='query',
        default=None,
        help="The query in format [<box>:<data>:<run>] for the item whose info should"
        " be printed.",
    )
    info_parser.set_defaults(command=info_command)


def info_command(args):
    """
    Command that shows the information about a data item.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    item_query = _parse_query(args.query[0])
    box = get_box(item_query.box)
    item_query.box = box.box_id
    items = box.storage.list_items(item_query)

    if len(items) == 0:
        _print_error(f"No item found by query {args.query[0]}", args)
        return
    if len(items) > 1:
        _print_error(f"Multiple items found by query {args.query[0]}", args)
        _print_result('', items, args)
        return
    item = items[0]

    logger.info(
        "Showing info about item %s from run %s in box %s",
        item.data_id,
        item.run_id,
        item.box_id,
    )

    info = box.storage.create_reader(DataRef.from_item(item)).info
    _print_result(f"Info {item.data_id} {item.run_id}", info, args)


def _add_diff_command(subparsers):
    diff_parser = subparsers.add_parser("diff", help="Compare data items")
    diff_parser.add_argument(
        nargs=2,
        metavar='QUERY',
        dest='queries',
        default=None,
        help="The queries in format [<box>:<data>:<run>] describing the items to"
        " compare.",
    )
    diff_parser.add_argument(
        '-d',
        '--diff-command',
        dest='diff',
        default='diff',
        help="The command to use for comparing, defaults to 'diff'.",
    )
    diff_parser.add_argument(
        '-l',
        '--without-labels',
        dest='labels',
        action='store_false',
        help="Disable the labels.",
    )
    diff_parser.add_argument(
        nargs='*',
        metavar='DIFF-ARG',
        dest='diff_args',
        help="Arbitrary arguments for the diff command.",
    )
    diff_parser.set_defaults(command=diff_command)


def diff_command(args):
    """
    Command that compares two runs or data items.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """

    def _get_data_item_as_file(ref):
        return ref.load(value_type=FileValueType())

    results = []
    for obj_string in args.queries:
        item_query = _parse_query(obj_string)
        box = get_box(item_query.box)
        item_query.box = box.box_id
        results.append(box.storage.list_items(item_query))

    if len(results[0]) == 1 and len(results[1]) == 1:
        first_ref = DataRef.from_item(results[0][0])
        second_ref = DataRef.from_item(results[1][0])
        logger.info(
            "Showing diff between items %s and %s",
            first_ref.uri,
            second_ref.uri,
        )
        first_file_path = _get_data_item_as_file(first_ref)
        first_label = args.queries[0]

        second_file_path = _get_data_item_as_file(second_ref)
        second_label = args.queries[1]

        command = [args.diff, str(first_file_path), str(second_file_path)]
        if args.labels:
            command.extend(
                [
                    '--label',
                    first_label,
                    '--label',
                    second_label,
                ]
            )
        command.extend(args.diff_args)
        logger.info("Calling diff %s", command)
        subprocess.run(command, stdout=sys.stdout, stderr=sys.stderr, check=False)
    else:
        _print_error("Ambiguous values to diff.", args)


def _add_export_command(subparsers):
    export_parser = subparsers.add_parser(
        "export", help="Export items to a local file."
    )
    export_parser.add_argument(
        metavar='QUERY',
        dest='query',
        default=None,
        help="The query in format [<box>:<data>:<run>] describing the item to export.",
    )
    export_parser.add_argument(
        metavar='FILE',
        dest='file',
        default=None,
        help="The file path to export to.",
    )
    export_parser.set_defaults(command=export_command)


def export_command(args):
    """
    Command that exports a data item to a file.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """

    def _export_item_as_file(ref, file_path):
        return ref.load(value_type=FileValueType(file_path=file_path))

    item_query = _parse_query(args.query)
    box = get_box(item_query.box)
    item_query.box = box.box_id
    items = box.storage.list_items(item_query)

    if len(items) == 0:
        _print_error(f"No item found for {args.query}.", args)
    elif len(items) > 1:
        _print_error(f"Multiple items found for {args.query}.", args)
        _print_result('', items, args)
    else:
        ref = DataRef.from_item(items[0])
        export_file_path = pathlib.Path(args.file)
        logger.info("Exporting item %s to file %s", ref.uri, export_file_path)

        _export_item_as_file(ref, export_file_path)
        _print_result(f"{args.query} successfully exported to {args.file}", [], args)


def _add_graph_command(subparsers):
    graph_parser = subparsers.add_parser(
        "graph",
        help="Create a dependency graph from objects in DOT format.",
    )
    graph_parser.add_argument(
        metavar='QUERY',
        dest='query',
        default=None,
        help="The query describing the items to graph.",
    )
    graph_parser.add_argument(
        nargs='?',
        metavar='FILE',
        dest='file',
        default='-',
        help="The file to write the graph to. If left empty, the graph is written to"
        " stdout.",
    )
    graph_parser.set_defaults(command=graph_command)


def graph_command(args):
    """
    Command that creates a graph out of data items.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """

    item_query = _parse_query(args.query)
    if item_query.box is None:
        item_query.box = get_config().default_box
    box = get_box(item_query.box)
    items = box.storage.list_items(item_query)
    refs = [DataRef.from_item(item) for item in items]

    if args.file == '-':
        writer = sys.stdout
    else:
        writer = io.FileIO(args.file, 'w')
        writer = codecs.getwriter('utf-8')(writer)

    with writer:
        write_graph_of_refs(writer, refs)


def _get_run_from_args(args):
    box = get_box(args.default_box)
    storage = box.storage
    runs = storage.list_runs(box.box_id)
    specified_run = None
    for run in runs:
        if args.run in (run.run_id[: len(args.run)], (run.name or '')[: len(args.run)]):
            specified_run = run
            break
    if specified_run is None:
        _print_error(f"No run found with run-id or name starting with {args.run}", args)
    return specified_run


def _get_item_in_run_from_args(args, run):
    box = get_box()
    storage = box.storage
    items = storage.list_items_in_run(box.box_id, run.run_id)

    item = None
    for i in items:
        if args.data in (i.data_id[: len(args.data)], i.name[: len(args.data)]):
            item = i
            break
    if item is None:
        _print_error(f"No item found with data-id starting with {args.data}", args)
    return item


def _parse_query(string):
    try:
        data_ref = DataRef.from_uri(string)
        return ItemQuery(':'.join([data_ref.box_id, data_ref.data_id, data_ref.run_id]))
    except ValueError:
        return ItemQuery(string)


def _print_result(title, result, args):

    if args.json:
        _print_result_as_json(result)
    else:
        print(title)
        _print_human_readable_result(result)


def _print_human_readable_result(result):
    if result:
        if isinstance(result, collections.abc.Mapping):
            _print_human_readable_mapping(result)
        elif isinstance(result, collections.abc.Sequence):
            _print_human_readable_list(result)
        return
    print("No result")


def _print_human_readable_list(result):
    headers = result[0]._fields
    field_lengths = [
        max(max(len(str(x)) for x in item), len(headers[i]))
        for i, item in enumerate(zip(*result))
    ]
    size = shutil.get_terminal_size((80, 20))
    columns = size.columns
    if columns < sum(field_lengths) - len(field_lengths):
        missing_columns = sum(field_lengths) + len(field_lengths) - columns
        shorten_per_columns = math.ceil(missing_columns / len(field_lengths))
        field_lengths = [length - shorten_per_columns for length in field_lengths]
    for i, field in enumerate(headers):
        print(f'|{field:^{field_lengths[i]}}', end='')
    print('|')
    for item in result:
        for i, field in enumerate(headers):
            max_length = field_lengths[i]
            if item[i] is not None:
                value = _shorten_string(str(item[i]), max_length)
            else:
                value = ''
            print(f' {value:<{max_length}}', end='')
        print()


def _print_human_readable_mapping(result, indent=0):
    headers = ["Property", "Value"]
    field_lengths = [
        max(max(len(str(x)) for x in result.keys()), len(headers[0])),
        max(max(len(str(x)) for x in result.values()), len(headers[0])),
    ]
    size = shutil.get_terminal_size((80, 20))
    columns = size.columns
    if columns < sum(field_lengths) - 3 - indent:
        missing_columns = sum(field_lengths) + 3 + indent - columns
        field_lengths = [
            field_lengths[0],
            field_lengths[1] - missing_columns,
        ]
    indent_string = ' ' * indent
    if indent == 0:
        for i, field in enumerate(headers):
            print(f' {indent_string}{field:<{field_lengths[i]}} ', end='')
        print()
    max_length_key = field_lengths[0]
    max_length_value = field_lengths[1]
    for key, value in result.items():
        if value and isinstance(value, collections.abc.Mapping):
            print(f'{indent_string} {key:<{max_length_key}}:')
            _print_human_readable_mapping(value, field_lengths[0] + 2)
        else:
            value = _shorten_string(str(value), max_length_value)
            print(
                f'{indent_string} {key:<{max_length_key}}: {value:<{max_length_value}}',
                end='',
            )
            print()


def _print_result_as_json(result):
    if not isinstance(result, dict):
        result = [item._asdict() for item in result]
    json.dump(
        result,
        sys.stdout,
        cls=_DatetimeJSONEncoder,
        allow_nan=False,
        indent=2,
        separators=(', ', ': '),
        sort_keys=True,
    )


def _print_error(error, args):
    logger.error(error)
    if args.json:
        result = {"error": error}
        json.dump(
            result,
            sys.stderr,
            cls=_DatetimeJSONEncoder,
            allow_nan=False,
            indent=None,
            separators=(',', ':'),
            sort_keys=True,
        )
    else:
        print(f"Error: {error}", file=sys.stderr)


def _confirm(confirmation_message):
    confirmation = input(confirmation_message)
    if confirmation in ['y', 'Y']:
        return True
    return False


class _DatetimeJSONEncoder(json.JSONEncoder):
    def __init__(self, value_serializers=(), **kwargs):
        self.value_serializers = value_serializers
        super().__init__(**kwargs)

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat(timespec='milliseconds')
        if isinstance(o, Exception):
            return o.__class__.__name__ + ': ' + str(o)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, o)


def _shorten_string(string, max_length):
    max_length = max(max_length, 3)
    if len(string) > max_length:
        string = string[: max_length - 3] + '...'
    return string


if __name__ == '__main__':
    main()
