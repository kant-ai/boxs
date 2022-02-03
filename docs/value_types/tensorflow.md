# Value types for tensorflow

[Tensorflow](https://www.tensorflow.org/) is a python framework for machine learning.
Boxs comes with some value types for storing and loading tensorflow data types. These
types require, that the `tensorflow` package is in the `PYTHONPATH` and can be loaded
in order to store or load values.

## TensorflowKerasModelValueType

The [`TensorflowKerasModelValueType`](../../api/#boxs.tensorflow.TensorflowKerasModelValueType)
allows to store a `tensorflow.keras.Model`. It uses the functions `save_model()` and
`load_model()` from the `tensorflow.keras.models` package to first save the model to
a temporary directory and then store the directory as a Zip archive. Loading the value
goes the other way round, extracting the zip to a temporary directory and then recreate
the model using `load_model()` function.

### Supported python types

None, the value type has to be used explicitly by providing it as `value_type` argument
to the call of `store()`.

### Configuration

#### dir_path

When a model is loaded, the path to the destination directory can be set by creating
a new `TensorflowKerasModelValueType` with the `dir_path` argument. If no `dir_path`is
given, which is default, the model is loaded to a temporary directory, that
is automatically deleted. If `dir_path` is set, the model is extracted to the given
directory and the directory is not deleted.

#### default_format

Additionally, the model can be stored in two different formats, 'h5' and 'tf'. As a
default, 'tf' is used. For more information about this, please refer to the
[tensorflow documentation](https://www.tensorflow.org/guide/keras/save_and_serialize#whole-model_saving_loading).


### Additional meta-data attributes

- `'model_format'`: The model format that was used storing the model.


## TensorBoardLogDirValueType

The [`TensorBoardLogDirValueType`](../../api/#boxs.tensorflow.TensorBoardLogDirValueType)
allows to store the log directory for visualizing the training in Tensorboard.
Tensorboard is a web frontend that allows to display training progress and metrics.

### Supported python types

`pathlib.Path`, but the value type should only be used explicitly by providing it as
`value_type` argument to the call of `store()`.

### Configuration

When a log directory is loaded, the path to the destination directory can be set by creating
a new `TensorBoardLogDirValueType` with the `dir_path` argument.

### Additional meta-data attributes

- `'dir_content'`: `'tensorboard-logs'`
