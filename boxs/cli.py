"""Command line interface"""
import argparse
import collections.abc
import datetime
import importlib
import json
import math
import shutil
import sys

from boxs.data import DataRef
from boxs.errors import BoxsError


def main(argv=None):
    """
    main() method of our command line interface.

    Args:
        argv (List[str]): Command line arguments given to the function. If `None`, the
            arguments are taken from `sys.argv`.
    """
    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser(
        prog='boxs',
        description="Allows to inspect and manipulate boxes that are used for "
        "storing data items using the python 'boxs' library.",
    )
    parser.set_defaults(command=lambda _: parser.print_help())
    parser.add_argument(
        '-b',
        '--box',
        dest='box',
        required=True,
        help="The box to inspect (format <module>.<variable>)",
    )
    parser.add_argument(
        '-j',
        '--json',
        dest='json',
        action='store_true',
        help="Print output as json",
    )

    subparsers = parser.add_subparsers(help="concepts")

    run_list_parser = subparsers.add_parser("list", help="List runs or items in a run")
    _add_run_argument(run_list_parser)
    run_list_parser.set_defaults(command=list_command)

    run_info_parser = subparsers.add_parser("info", help="Info about an item")
    _add_run_argument(run_info_parser, required=True)
    run_info_parser.add_argument(
        '-d',
        '--data',
        dest='data',
        required=True,
        help="The data whose info is requested",
    )
    run_info_parser.set_defaults(command=info_command)

    run_name_parser = subparsers.add_parser("name", help="Set the name of a run")
    _add_run_argument(run_name_parser, required=True)
    run_name_parser.add_argument(
        '-n',
        '--name',
        dest='name',
        default=None,
        help="The new name of the run, if left out, the current name will be removed.",
    )
    run_name_parser.set_defaults(command=name_command)

    args = parser.parse_args(argv)

    try:
        args.command(args)
    except BoxsError as error:
        _print_error(error, args)


def _add_run_argument(parser, required=False):
    parser.add_argument(
        '-r',
        '--run',
        dest='run',
        required=required,
        help="Run id or name, can be just the first characters if unambiguous.",
    )


def list_command(args):
    """
    Command that either lists runs or the data items in a specific run.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    if args.run is not None:
        list_run(args)
    else:
        list_runs(args)


def list_runs(args):
    """
    Function that lists runs.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    box = _load_box(args.box)
    storage = box.storage
    runs = storage.list_runs(box.box_id)
    _print_result("List runs", runs, args)


def list_run(args):
    """
    Function that lists the data items of a specific run.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    box = _load_box(args.box)
    storage = box.storage
    run = _get_run_from_args(args)
    if run is None:
        return
    items = storage.list_items_in_run(box.box_id, run.run_id)
    _print_result(f"List run {run.run_id}", items, args)


def name_command(args):
    """
    Command that allows to set a name for a specific run.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    box = _load_box(args.box)
    storage = box.storage
    run = _get_run_from_args(args)
    if run is None:
        return
    run = storage.set_run_name(box.box_id, run.run_id, args.name)
    _print_result(f"Run name set {run.run_id}", [run], args)


def info_command(args):
    """
    Command that shows the information about a data item.

    Args:
        args (argparse.Namespace): The parsed arguments from command line.
    """
    box = _load_box(args.box)
    storage = box.storage
    run = _get_run_from_args(args)
    if run is None:
        return

    item = _get_item_in_run_from_args(args, run)
    if item is None:
        return

    info = storage.create_reader(DataRef(box.box_id, item.data_id, item.run_id)).info
    _print_result(f"Info {item.data_id} {item.run_id}", info, args)


def _load_box(box_import_path):
    module_name, variable_name = box_import_path.rsplit('.', maxsplit=1)
    module = importlib.import_module(module_name)
    return getattr(module, variable_name)


def _get_run_from_args(args):
    box = _load_box(args.box)
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
    box = _load_box(args.box)
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
