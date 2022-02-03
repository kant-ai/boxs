# StatisticsTransformer

The [`StatisticsTransformer`](../../api/#boxs.statistics.StatisticsTransformer)
gathers some statistics about a value when it is stored and adds those as
additional meta-data attributes.

## Configuration

None

## Additional meta-data attributes

- `'size_in_bytes'`: The number of written bytes when storing the value.
- `'number_of_lines'`: The number of lines that the stored value contains.
- `'store_start'`: The timestamp UTC in ISO format when storing the value began.
- `'store_end'`: The timestamp UTC in ISO format when storing the value ended.

## Example

```python
import boxs

...
box = boxs.Box('my-box-id', storage, boxs.StatisticsTransformer())

value_info = boxs.store('My value\nin\nmultiple\nlines.\n', box=box)

print(value_info.meta['size_in_bytes'])
print(value_info.meta['number_of_lines'])
print(value_info.meta['store_start'])
print(value_info.meta['store_end'])
```
