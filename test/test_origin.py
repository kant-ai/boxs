import unittest

from boxs.origin import (
    determine_origin,
    ORIGIN_FROM_FUNCTION_NAME, ORIGIN_FROM_NAME, ORIGIN_FROM_TAGS,
)


def _shortend_func_name_origin(context):
    return context.function_name[:8]


def _origin_func_without_return(context):
    return None


class TestDetermineOrigin(unittest.TestCase):

    def test_determine_origin_returns_fixed_value(self):
        origin = determine_origin('my-origin')
        self.assertEqual('my-origin', origin)

    def test_determine_origin_returns_callable_return_value(self):
        origin = determine_origin(lambda: 'my-origin')
        self.assertEqual('my-origin', origin)

    def test_determine_origin_from_custom_function(self):
        origin = determine_origin(_shortend_func_name_origin)
        self.assertEqual('test_det', origin)

    def test_determine_origin_raises_if_none(self):
        with self.assertRaisesRegex(ValueError, 'No origin given'):
            determine_origin(None)

    def test_determine_origin_raises_if_none_is_returned(self):
        with self.assertRaisesRegex(ValueError, 'No origin given'):
            determine_origin(lambda: None)

    def test_determine_origin_From_custom_function(self):
        with self.assertRaisesRegex(ValueError, 'No origin given'):
            determine_origin(_origin_func_without_return)

    def test_origin_from_function_name_includes_indirection(self):
        def indirection():
            return determine_origin(ORIGIN_FROM_FUNCTION_NAME)
        origin = indirection()
        self.assertEqual('indirection', origin)

    def test_origin_from_name_includes_indirection(self):
        def indirection():
            return determine_origin(ORIGIN_FROM_NAME, name='my_name')
        origin = indirection()
        self.assertEqual('my_name', origin)

    def test_origin_from_tags_includes_indirection(self):
        def indirection():
            return determine_origin(ORIGIN_FROM_TAGS, tags={'my': 'tags'})

        origin = indirection()
        self.assertEqual('{"my":"tags"}', origin)


class TestRepresentations(unittest.TestCase):

    def test_origin_from_function_name_str(self):
        result = str(ORIGIN_FROM_FUNCTION_NAME)
        self.assertEqual('ORIGIN_FROM_FUNCTION_NAME', result)

    def test_origin_from_function_name_repr(self):
        result = repr(ORIGIN_FROM_FUNCTION_NAME)
        self.assertEqual('ORIGIN_FROM_FUNCTION_NAME', result)

    def test_origin_from_name_str(self):
        result = str(ORIGIN_FROM_NAME)
        self.assertEqual('ORIGIN_FROM_NAME', result)

    def test_origin_from_name_repr(self):
        result = repr(ORIGIN_FROM_NAME)
        self.assertEqual('ORIGIN_FROM_NAME', result)

    def test_origin_from_tags_str(self):
        result = str(ORIGIN_FROM_TAGS)
        self.assertEqual('ORIGIN_FROM_TAGS', result)

    def test_origin_from_tags_repr(self):
        result = repr(ORIGIN_FROM_TAGS)
        self.assertEqual('ORIGIN_FROM_TAGS', result)


if __name__ == '__main__':
    unittest.main()
