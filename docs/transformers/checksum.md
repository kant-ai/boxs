# ChecksumTransformer

The [`ChecksumTransformer`](../../api/#boxs.checksum.ChecksumTransformer)
generates a checksum from a value when it is stored and adds it as additional
meta-data attribute. When the value is loaded later, the checksum is again computed
and verified. If the checksum differs from the checksum that was created when
storing the value, a
[`boxs.checksum.DataChecksumMismatch`](../../api/#boxs.checksum.DataChecksumMismatch)
error is raised. The checksum allows detecting transmission errors or faulty value
types, but it can be used for implementing some kind of deduplication scheme inside
a `Storage`.
The current implementation uses the
[`blake2b`](https://docs.python.org/3/library/hashlib.html#hashlib.blake2b)
hashing function with a configurable `digest_size`.

## Configuration

When creating the `ChecksumTransformer` its constructor can take an optional
`digest_size` argument. This defines the size of the used digest in bytes. If the
argument is not given, a default size of 32 bytes is used.

## Additional meta-data attributes

- `'checksum_digest'`: The hex representation of the calculated digest.
- `'checksum_digest_size'`: The size of the digest (not its representation) in bytes.
- `'checksum_algorithm'`: `'black2b'`

## Example

```python
import boxs

...
box = boxs.Box('my-box-id', storage, boxs.ChecksumTransformer(digest_size=16))

value_info = boxs.store('My value\n', box=box)

print(value_info.meta['checksum_digest'])
print(value_info.meta['checksum_digest_size'])
print(value_info.meta['checksum_algorithm'])

try:
    value = value_info.load()
except boxs.DataChecksumMismatch as error:
    print("Checksum verification failed: %s", error)
```
