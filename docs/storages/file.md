# FileSystemStorage

The [`FileSystemStorage`](../../api/#boxs.filesystem.FileSystemStorage) stores the
values, their meta-data and the information about the individual runs in a directory
in the local file system. For this it takes a directory path as configuration, which
is the root of all stored data.

The directory structure inside looks like the following:

```
└── <box-id>
    ├── data
    │   ├── <data-id>
    │   │   ├── <run-id>.data
    │   │   ├── <run-id>.info
    │   │   ├── <other-run-id>.data
    │   │   └── <other-run-id>.info
    │   ├── <other-data-id>
    │   │   ├── <run-id>.data
    │   │   ├── <run-id>.info
    │   │   ├── <other-run-id>.data
    │   │   └── <other-run-id>.info
        ...
    │   └── <another-data-id>
    └── runs
        ├── <run-id>
        │   ├── <data-id>
        │   ├── <other-data-id>
        │   ├── <another-data-id>
        │   └── _named
        │       ├── <data-name> -> ../<data-id>
        │       ├── <other-data-name> -> ../<other-data-id>
        │       └── <another-data-name> -> ../<another-data-id>
        ...
        └── _named
            ├── <run-name> -> ../<run-id>
            └── <other-run-name> -> ../<other-run-id>
```

The root directory contains one subdirectory per box named with the box id.
Within these box-directories, one finds two directories named "data" and "runs".

"data" contains one subdirectory per data id and within each of those two files that
contain the stored value (".data") and the information including the meta-data about
this value (".info") per run, where this data item was created.

"runs" doesn't contain any data itself, but just pointers to the data items, that were
created in a run. For each new run, a subdirectory named with the run-id is created.
This subdirectory contains empty files that are named with the data-ids of all data
items that were created in this run. Additionally, each run directory can contains a
subdirectory "_named" which contains symbolic links named with the name of a specific
data item, that links to the file containing the data-id of this data item.
The same mechanism is used on "runs" level, to map a run-name to its corresponding
run-id.

Here an abbreviated example of a storage directory tree:
```
└── my-box-id
    ├── data
    │   ├── 1bb60c8869c65276
    │   │   ├── 118d9332-7df3-11ec-9020-48f17f64520d.data
    │   │   ├── 118d9332-7df3-11ec-9020-48f17f64520d.info
    │   │   ├── bec3dfb2-7df2-11ec-9020-48f17f64520d.data
    │   │   ├── bec3dfb2-7df2-11ec-9020-48f17f64520d.info
    │   │   ├── c0b342b4-7c9d-11ec-abee-48f17f64520d.data
    │   │   ├── c0b342b4-7c9d-11ec-abee-48f17f64520d.info
    │   │   ├── c5caaee0-7d2e-11ec-9020-48f17f64520d.data
    │   │   └── c5caaee0-7d2e-11ec-9020-48f17f64520d.info
    │   ├── 21bfb27c4a636f4e
    │   │   ├── 0e63ad74-8102-11ec-9c51-48f17f64520d.data
    │   │   ├── 0e63ad74-8102-11ec-9c51-48f17f64520d.info
    │   │   ├── 118d9332-7df3-11ec-9020-48f17f64520d.data
    │   │   ├── 118d9332-7df3-11ec-9020-48f17f64520d.info
    │   │   ├── bec3dfb2-7df2-11ec-9020-48f17f64520d.data
    │   │   ├── bec3dfb2-7df2-11ec-9020-48f17f64520d.info
    │   │   ├── c5caaee0-7d2e-11ec-9020-48f17f64520d.data
    │   │   ├── c5caaee0-7d2e-11ec-9020-48f17f64520d.info
    │   │   ├── ed923ea0-7df8-11ec-9020-48f17f64520d.data
    │   │   └── ed923ea0-7df8-11ec-9020-48f17f64520d.info
        ...
    │   └── fc657a8c9d33b5fc
    └── runs
        ├── 0e63ad74-8102-11ec-9c51-48f17f64520d
        │   ├── 15d29183ad09cbfc
        │   ├── 21bfb27c4a636f4e
        │   ├── 6f31c9a1c68b3b02
        │   ├── 7473d9676cbc377e
        │   ├── 800b5a5c9a972d3e
        │   ├── b2560fb2c8b88621
        │   ├── cf64163dd7c1898c
        │   ├── d91bf391601f6f53
        │   ├── ddb065be789696cb
        │   ├── efb03082d67da076
        │   └── _named
        │       ├── encoded_eval_input -> ../800b5a5c9a972d3e
        │       ├── encoded_eval_output -> ../ddb065be789696cb
        │       ├── encoded_train_input -> ../7473d9676cbc377e
        │       ├── encoded_train_output -> ../21bfb27c4a636f4e
        │       ├── eval_data -> ../efb03082d67da076
        │       ├── normalized_eval_input -> ../6f31c9a1c68b3b02
        │       ├── normalized_train_input -> ../15d29183ad09cbfc
        │       ├── tensorboard_logs -> ../b2560fb2c8b88621
        │       ├── train_data -> ../cf64163dd7c1898c
        │       └── trained_model -> ../d91bf391601f6f53
        ...
        └── _named
            ├── First run -> ../cbd0b6dd-7935-11ec-a32c-48f17f64520d
            ├── release_1.0 -> ../12d03f94-7e0a-11ec-9020-48f17f64520d
            └── release_1.1 -> ../0e63ad74-8102-11ec-9c51-48f17f64520d
```

## Configuration

The constructor of `FileSystemStorage` takes a single argument `directory` which
denotes the directory, where it should store its data.

## Example

```python
import boxs

...
storage = boxs.FileSystemStorage('/my/storage/directory')
box = boxs.Box('my-box-id', storage)
```
