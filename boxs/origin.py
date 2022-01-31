"""Origins of data"""
import inspect
import json
import typing


class OriginContext:
    """
    Context from which an origin mapping function can derive the origin.

    Attributes:
        function_name (str): The name of the function that called.
        arg_info (inspect.ArgInfo): A data structure that contains the arguments of
            the function which called.
        name (str): The name that was given to `store()`.
        tags (Dict[str,str]): The tags this item will be assigned to.
    """

    def __init__(self, name, tags, level=2):
        frame = inspect.currentframe()
        for _ in range(level):
            frame = frame.f_back
        self.function_name = frame.f_code.co_name
        self.arg_info = inspect.getargvalues(frame)
        self.name = name
        self.tags = tags


OriginMappingFunction = typing.Callable[[OriginContext], str]
"""
A function that takes a OriginContext and returns the origin as string.

Args:
    context (boxs.origin.OriginContext): The context from which to derive the origin.

Returns:
    str: The origin.
"""


def determine_origin(origin, name=None, tags=None, level=2):
    """
    Determine an origin.

    If the given origin is a callable, we run it and take its return value as new
    origin.

    Args:
        origin (Union[str, OriginMappingFunction, Callable[[],str]]): A string or a
            callable that returns a string. The callable can either have no arguments
            or a single argument of type `boxs.origin.OriginContext`.
        name (str): Name that will be available in the OriginContext if needed.
        tags (Dict[str,str]): Tags that will be available in the context if needed.
        level (int): The levels on the stack that we should go back. Defaults to 2
            which selects the calling frame of determine_origin().

    Returns:
        str: The origin as string.
    """
    if callable(origin):
        if inspect.signature(origin).parameters:
            context = OriginContext(name, tags, level=level)
            origin = origin(context)
        else:
            origin = origin()
    if origin is None:
        raise ValueError("No origin given (is 'None').")
    return origin


class _OriginFromFunctionName:
    def __call__(self, context):
        """
        Returns the executed function name from stack as origin.

        Args:
            context (OriginContext): The context containing the function_name.

        Returns:
            str: The origin as string.
        """
        return context.function_name

    def __repr__(self):
        return 'ORIGIN_FROM_FUNCTION_NAME'

    def __str__(self):
        return repr(self)


ORIGIN_FROM_FUNCTION_NAME = _OriginFromFunctionName()
"""
OriginMappingFunction that uses the function_name as origin.
"""


class _OriginFromName:
    def __call__(self, context):
        """
        Returns the value of 'name' as origin.

        Args:
            context (OriginContext): The context containing the name.

        Returns:
            str: The origin as string.
        """
        return context.name

    def __repr__(self):
        return 'ORIGIN_FROM_NAME'

    def __str__(self):
        return repr(self)


ORIGIN_FROM_NAME = _OriginFromName()
"""
OriginMappingFunction that uses the name as origin.
"""


class _OriginFromTags:
    def __call__(self, context):
        """
        Returns the json representation of 'tags' as origin.

        Args:
            context (OriginContext): The context containing the tags.

        Returns:
            str: The origin as JSON string.
        """
        return json.dumps(context.tags, sort_keys=True, separators=(',', ':'))

    def __repr__(self):
        return 'ORIGIN_FROM_TAGS'

    def __str__(self):
        return repr(self)


ORIGIN_FROM_TAGS = _OriginFromTags()
"""
OriginMappingFunction that uses the tags in JSON format as origin.
"""
