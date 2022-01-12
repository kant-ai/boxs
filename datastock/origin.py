"""Origins of data"""
import functools
import inspect
import json


def determine_origin(origin):
    """
    Determine an origin.

    If the given origin is a callable, we run it and take its return value as new
    origin.

    Args:
        origin (Union[str,Callable[[],str]]): A string or a callable that returns a
            string.

    Returns:
        str: The origin as string.
    """
    origin = origin() if callable(origin) else origin
    if origin is None:
        raise ValueError("No origin given (is 'None').")
    return origin


def origin_from_function_name(level=1):
    """
    Returns the executed function name from stack as origin.

    Args:
        level (int): How many stack frames should we go back. Defaults to 1.

    Returns:
        str: The origin as string.
    """
    frame = inspect.currentframe()
    for _ in range(level):
        frame = frame.f_back
    return frame.f_code.co_name


ORIGIN_FROM_FUNCTION_NAME = functools.partial(origin_from_function_name, 3)


def origin_from_name(level=1):
    """
    Returns the value of a 'name' variable from a stack frame as origin.

    Args:
        level (int): How many stack frames should we go back. Defaults to 1.

    Returns:
        str: The origin as string.

    Raises:
        RuntimeError: If no variable with name 'name' is available in the referenced
            frame.
    """
    frame = inspect.currentframe()
    for _ in range(level):
        frame = frame.f_back
    if 'name' not in frame.f_locals:
        raise RuntimeError("No local variable named 'name' in frame. Wrong level?")
    return frame.f_locals['name']


ORIGIN_FROM_NAME = functools.partial(origin_from_name, 3)


def origin_from_tags(level=1):
    """
    Returns the json representation of a 'tags' variable from a stack frame as origin.

    Args:
        level (int): How many stack frames should we go back. Defaults to 1.

    Returns:
        str: The origin as JSON string.

    Raises:
        RuntimeError: If no variable with name 'tags' is available in the referenced
            frame.
    """
    frame = inspect.currentframe()
    for _ in range(level):
        frame = frame.f_back
    if 'tags' not in frame.f_locals:
        raise RuntimeError("No local variable named 'tags' in frame. Wrong level?")
    tags = frame.f_locals['tags']
    return json.dumps(tags, sort_keys=True, separators=(',', ':'))


ORIGIN_FROM_TAGS = functools.partial(origin_from_tags, 3)
