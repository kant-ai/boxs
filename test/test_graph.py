import io
import unittest

from boxs.data import DataInfo, DataRef
from boxs.graph import write_graph_of_refs


class TestWriteGraphOfRefs(unittest.TestCase):

    def setUp(self):
        self.ref1 = DataInfo(DataRef('box-id', 'data-1', 'run-1'), 'origin-1')
        self.ref2 = DataInfo(DataRef('box-id', 'data-2', 'run-1'), 'origin-2')
        self.ref3 = DataInfo(DataRef('box-id', 'data-3', 'run-2'), 'origin-3', parents=[self.ref1, self.ref2])
        self.ref4 = DataInfo(DataRef('box-id', 'data-4', 'run-2'), 'origin-4', parents=[self.ref2])
        self.refs = [self.ref3, self.ref4]

        self.stream = io.StringIO()

    def test_graph_contains_direct_refs(self):
        write_graph_of_refs(self.stream, self.refs)
        graph = self.stream.getvalue()
        self.assertIn('"boxs://box-id/data-3/run-2"[label="data-3\\norigin origin-3"]', graph)
        self.assertIn('"boxs://box-id/data-4/run-2"[label="data-4\\norigin origin-4"]', graph)

    def test_graph_contains_parents_of_refs(self):
        write_graph_of_refs(self.stream, self.refs)
        graph = self.stream.getvalue()
        self.assertIn('"boxs://box-id/data-1/run-1"[label="data-1\\norigin origin-1"]', graph)
        self.assertIn('"boxs://box-id/data-2/run-1"[label="data-2\\norigin origin-2"]', graph)

    def test_graph_contains_edges_between_parent_and_child(self):
        write_graph_of_refs(self.stream, self.refs)
        graph = self.stream.getvalue()
        self.assertIn('"boxs://box-id/data-1/run-1" -> "boxs://box-id/data-3/run-2"', graph)
        self.assertIn('"boxs://box-id/data-2/run-1" -> "boxs://box-id/data-3/run-2"', graph)
        self.assertIn('"boxs://box-id/data-2/run-1" -> "boxs://box-id/data-4/run-2"', graph)

    def test_runs_are_clustered(self):
        write_graph_of_refs(self.stream, self.refs)
        graph = self.stream.getvalue()
        self.assertIn('subgraph "cluster_run-1" {', graph)
        self.assertIn('subgraph "cluster_run-2" {', graph)

    def test_label_uses_name_if_set(self):
        ref = DataInfo(DataRef('box-id', 'data-1', 'run-1'), 'origin-1', name='my name')
        write_graph_of_refs(self.stream, [ref])
        graph = self.stream.getvalue()
        self.assertIn('"boxs://box-id/data-1/run-1"[label="my name\\norigin origin-1"]', graph)

    def test_size_is_added_if_set_in_meta(self):
        ref = DataInfo(DataRef(
            'box-id', 'data-1', 'run-1'), 'origin-1', name='my name', meta={'size_in_bytes': 1234},
        )
        write_graph_of_refs(self.stream, [ref])
        graph = self.stream.getvalue()
        self.assertIn('"boxs://box-id/data-1/run-1"[label="my name\\norigin origin-1\\nsize 1234"]', graph)


if __name__ == '__main__':
    unittest.main()
