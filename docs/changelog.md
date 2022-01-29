# Changelog

## 0.1: Initial release of the package.

  - Simple api in form of boxs.store(), boxs.load() and boxs.info()
  - FileSystemStorage which stores data items in the local file system
  - Value types for storing and loading data from different types
    - Standard python types (str, bytes, streams, files, directories, dicts and list
      via JSON)
    - Pandas DataFrame
    - Tensorflow Keras model
  - Transformer mechanism for modifying data and meta-data during storing
    - ChecksumTransformer which automatically generates checksums during store and
      verifies them when loading
    - StatisticsTransformer keeps statistics about size, number of lines or time for
      storing
  - Command line interface for inspecting data
    - List all data items and runs in which they were created
    - Print information about individual data items
    - Export data items to the local file system
    - Compare data items using diff
    - Generate dependency graph for data items in DOT format
    - Delete no longer used runs