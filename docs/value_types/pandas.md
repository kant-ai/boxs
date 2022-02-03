# Value types for pandas data structures

[Pandas](https://pandas.pydata.org/) is a library for data analysis and manipulation.
In order to make working with pandas and boxs easier, boxs comes with types for
storing pandas types. Those types are only available, if the `pandas` package is
available, otherwise the value types are not defined because of missing dependencies.


## PandasDataFrameCsvValueType

The [`PandasDataFrameCsvValueType`](../../api/#boxs.pandas.PandasDataFrameCsvValueType)
allows to store a `pandas.DataFrame`. The data is exported to a CSV file using the
pandas `to_csv()` and `read_csv()` functions.

### Supported python types

`pandas.DataFrame`

### Configuration

The value type supports configuring its `default_encoding`. This encoding will be used
when storing a the data frame. The encoding uses 'utf-8' as default. It can be used
with a different `default_encoding` when a new instance of `PandasDataFrameCsvValueType`
is created that takes a different encoding as constructor argument.

### Additional meta-data attributes

- `'encoding'`: The encoding that was used storing this value.
