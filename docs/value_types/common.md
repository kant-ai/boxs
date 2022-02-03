# Value types for common python values

Boxs comes with some ready-to-use `ValueType` implementations, that allow to store
python types that are common used. All of these value types are automatically
available and used for the supported types.

## BytesValueType

The [`BytesValueType`](../../api/#boxs.value_types.BytesValueType) allows to store
binary values.

### Supported python types

`bytes`, `bytearray`

### Configuration

Not available

### Additional meta-data attributes

None


## DirectoryValueType

The [`DirectoryValueType`](../../api/#boxs.value_types.DirectoryValueType) can be used
for storing directories and their content. These directories get added to a Zip
archive that is then stored in the storage. Not all file properties are kept, because
the Zip format doesn't support them. This is especially true for file permissions and
owner and group information. These may change when loading a directory.

### Supported python types

`pathlib.Path` that refer to an existing directory.

### Configuration

When a directory is loaded, the path to the destination directory can be set by
creating a new `DirectoryValueType` with the `dir_path` argument. If no `dir_path`
is given, which is default, the directory is loaded to a temporary directory, that
should be deleted by the user after its no longer needed.

### Additional meta-data attributes

None


## FileValueType

The [`FileValueType`](../../api/#boxs.value_types.FileValueType) can be used
for storing single files. No file properties are kept. This is especially true for
file permissions and owner and group information. These may change when loading the
file again.

### Supported python types

`pathlib.Path` that refer to an existing file.

### Configuration

When a file is loaded, the path to the destination file can be set by
creating a new `FileValueType` with the `file_path` argument. If no `file_path`
is given, which is default, the file is loaded to a temporary file, that
should be deleted by the user after its no longer needed.

### Additional meta-data attributes

None


## JsonValueType

The [`JsonValueType`](../../api/#boxs.value_types.JsonValueType) can be used
for storing lists and dictionaries of primitive values. It encodes those data
structures as JSON string and stores it.

### Supported python types

`list` and `dict`, as long, as they contain only values of types that can be
serialized to JSON using the standard json encoder, without the need of a custom
[JSONEncoder](https://docs.python.org/3/library/json.html#json.JSONEncoder) class.

### Configuration

Not available

### Additional meta-data attributes

- `'media_type'`: `'application/json'`


## StreamValueType

The [`StreamValueType`](../../api/#boxs.value_types.StreamValueType) can be used for
storing the content of binary streams. The value type reads from this stream, until it
reaches EOF. The stream is not automatically closed, so it can be reused.

### Supported python types

`typing.BinaryIO(IO[bytes])`

### Configuration

Not available

### Additional meta-data attributes

None


## StringValueType

The [`StringValueType`](../../api/#boxs.value_types.StringValueType) can be used for
storing the content of binary streams. The value type reads from this stream, until it
reaches EOF. The stream is not automatically closed, so it can be reused. The encoding
that is used for storing the string can be set when defining the `value_type`
explicitly. The encoding is stored in the meta-data and reused when loading a value.

### Supported python types

`str`

### Configuration

The value type supports configuring its `default_encoding`. This encoding will be used
when storing a string. The encoding uses 'utf-8' as default. It can be used with a
different `default_encoding` when a new instance of `StringValueType` is created that
takes a different encoding as constructor argument.

### Additional meta-data attributes

- `'encoding'`: The encoding that was used storing this value.
