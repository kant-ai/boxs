"""Functions for creating dependency graphs"""
import collections


def write_graph_of_refs(writer, refs):
    """
    Write the dependency graph in DOT format for the given refs to the writer.

    Args:
        writer (io.TextIO): A text stream, to which the graph definition will be
            written.
        refs (list[boxs.data.DataRef]): A list of DataRef instances for which the
            dependency graph will be created.
    """
    writer.write("digraph {\n")
    infos_by_run = collections.defaultdict(list)
    visited = set()
    queue = collections.deque()
    queue.extend(refs)
    while queue:
        ref = queue.popleft()
        if ref.uri in visited:
            continue
        info = ref.info
        infos_by_run[ref.run_id].append(info)
        for parent in info.parents:
            queue.appendleft(parent)
        visited.add(ref.uri)

    for run_id, infos in infos_by_run.items():
        writer.write(f'  subgraph "cluster_{run_id}" {{\n')
        writer.write(f'    label="Run {run_id}";\n')

        _write_nodes_for_infos(infos, writer)

        writer.write("  }\n")

    for run_id, infos in infos_by_run.items():
        _write_edges_to_parents_for_infos(infos, writer)

    writer.write("}\n")


def _write_edges_to_parents_for_infos(infos, writer):
    for info in infos:
        for parent in info.parents:
            writer.write(f'  "{parent.uri}" -> "{info.uri}"\n')


def _write_nodes_for_infos(infos, writer):
    for info in infos:
        if info.name:
            label = info.name
        else:
            label = info.data_id
        writer.write(f'    "{info.uri}"')
        writer.write(f'[label="{label}')
        writer.write(f'\\norigin {info.origin}')
        if 'size_in_bytes' in info.meta:
            writer.write(f'\\nsize {info.meta["size_in_bytes"]}')
        writer.write('"]\n')
