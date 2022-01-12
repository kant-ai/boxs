import unittest

from datastock.origin import (
    determine_origin,
    origin_from_function_name, origin_from_name, origin_from_tags,
    ORIGIN_FROM_FUNCTION_NAME, ORIGIN_FROM_NAME, ORIGIN_FROM_TAGS,
)


class TestDetermineOrigin(unittest.TestCase):

    def test_determine_origin_returns_fixed_value(self):
        origin = determine_origin('my-origin')
        self.assertEqual('my-origin', origin)

    def test_determine_origin_returns_callable_return_value(self):
        origin = determine_origin(lambda: 'my-origin')
        self.assertEqual('my-origin', origin)

    def test_determine_origin_raises_if_none(self):
        with self.assertRaisesRegex(ValueError, 'No origin given'):
            determine_origin(None)

    def test_determine_origin_raises_if_none_is_returned(self):
        with self.assertRaisesRegex(ValueError, 'No origin given'):
            determine_origin(lambda: None)

    def test_origin_from_function_name_includes_indirection(self):
        def indirection():
            return determine_origin(ORIGIN_FROM_FUNCTION_NAME)
        origin = indirection()
        self.assertEqual('test_origin_from_function_name_includes_indirection', origin)

    def test_origin_from_name_includes_indirection(self):
        def indirection():
            return determine_origin(ORIGIN_FROM_NAME)
        name = 'my_name'
        origin = indirection()
        self.assertEqual('my_name', origin)

    def test_origin_from_tags_includes_indirection(self):
        def indirection():
            return determine_origin(ORIGIN_FROM_TAGS)
        tags = {'my': 'tags'}
        origin = indirection()
        self.assertEqual('{"my":"tags"}', origin)


class TestOriginFromFunctionName(unittest.TestCase):

    def test_origin_from_function_name_returns_where_it_is_called(self):
        origin = origin_from_function_name()
        self.assertEqual('test_origin_from_function_name_returns_where_it_is_called', origin)

    def test_origin_from_function_name_can_skip_multiple_level(self):
        def indirection():
            return origin_from_function_name(level=2)
        origin = indirection()
        self.assertEqual('test_origin_from_function_name_can_skip_multiple_level', origin)


class TestOriginFromName(unittest.TestCase):

    def test_origin_from_name_returns_where_it_is_called(self):
        name = 'my_name'
        origin = origin_from_name()
        self.assertEqual(name, origin)

    def test_origin_from_name_can_skip_multiple_level(self):
        def indirection():
            return origin_from_name(level=2)
        name = 'my_name'
        origin = indirection()
        self.assertEqual(name, origin)

    def test_origin_from_name_raises_if_variable_is_missing(self):
        with self.assertRaisesRegex(RuntimeError, "No local variable named 'name' in frame. Wrong level?"):
            origin_from_name()


class TestOriginFromTags(unittest.TestCase):

    def test_origin_from_tags_returns_where_it_is_called(self):
        tags = {'a': '1'}
        origin = origin_from_tags()
        self.assertEqual('{"a":"1"}', origin)

    def test_origin_from_tags_can_skip_multiple_level(self):
        def indirection():
            return origin_from_tags(level=2)
        tags = {'a': '1'}
        origin = indirection()
        self.assertEqual('{"a":"1"}', origin)

    def test_origin_from_tags_raises_if_variable_is_missing(self):
        with self.assertRaisesRegex(RuntimeError, "No local variable named 'tags' in frame. Wrong level?"):
            origin_from_tags()

    def test_origin_from_tags_with_multiple_tags(self):
        tags = {'a': '1', 'b': '2'}
        origin = origin_from_tags()
        self.assertEqual('{"a":"1","b":"2"}', origin)

    def test_origin_from_tags_origin_independent_of_tag_order(self):
        tags = {'a': '1', 'b': '2'}
        origin1 = origin_from_tags()
        tags = {'b': '2', 'a': '1'}
        origin2 = origin_from_tags()

        self.assertEqual('{"a":"1","b":"2"}', origin1)
        self.assertEqual('{"a":"1","b":"2"}', origin2)


if __name__ == '__main__':
    unittest.main()
