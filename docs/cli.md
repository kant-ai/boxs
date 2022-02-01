# Command line interface

Boxs brings with it a command line interface, that lets the user inspect and manage
the data items of a box.

## Install

When boxs is installed using `pip`, the command line interface `boxs` is automatically
installed and added to the path:

```bash
$ pip install boxs
...
$ boxs -h
usage: boxs [-h] [-b BOX] [-i INIT_MODULE] [-j]
            {list-runs,name-run,delete-run,clean-runs,list,info,diff,export,graph}
            ...

```
If the boxs package is used from source, the package can be locally installed together
with the script by running

```bash
$ flit install
...
$ boxs -h
usage: boxs [-h] [-b BOX] [-i INIT_MODULE] [-j]
            {list-runs,name-run,delete-run,clean-runs,list,info,diff,export,graph}
            ...
```

from the boxs root directory. If `flit` isn't available, the command line interface
can be directly executed using the python interpreter:

```bash
$ python -m boxs.cli -h
usage: boxs [-h] [-b BOX] [-i INIT_MODULE] [-j]
            {list-runs,name-run,delete-run,clean-runs,list,info,diff,export,graph}
            ...
```

## Global options

The command line interface supports a set of global options that are relevant for all
commands:

```bash
optional arguments:
  -b BOX, --default-box BOX
                        The id of the default box to use. If not set, the
                        default is taken from the BOXS_DEFAULT_BOX environment
                        variable.
  -i INIT_MODULE, --init-module INIT_MODULE
                        A python module that should be automatically loaded.
                        If not set, the default is taken from the
                        BOXS_INIT_MODULE environment variable.
  -j, --json            Print output as json
```

### --default-box, -b

Many commands apply to a single box. This option allows to set the box id of the box,
that should be used as a default of no other box is specified. This argument has
precedence over the default box defined by the environment variable `BOXS_DEFAULT_BOX`.

See the [user guide](../user_guide/#default_box) for more information.

### --init-module, -i

This option instructs the command line interface to load the specified python module.
It is meant for making sure, the box definitions are setup. The argument has
precedence over the init_module defined by the environment variable
`BOXS_INIT_MODULE`, but it is possible, that both modules get loaded, if they differ.

See the [user guide](../user_guide/#init_module) for more information.

### --json, -j

Often the tools are integrated in some automation. For this, it has the option to
print all results in JSON format so that they can be parsed more easily.

## Commands


### list-runs

```bash
$ boxs list-runs -h
usage: boxs list-runs [-h] [-f FILTER] [-l LIMIT]

optional arguments:
  -h, --help            show this help message and exit
  -f FILTER, --name-filter FILTER
                        Only list runs whose name begins with FILTER.
  -l LIMIT, --limit LIMIT
                        Only list the <LIMIT> last runs.
```

This commands allows to list the runs that can be found in the default box. Without
any other options, all runs are returned and printed in descending order based on the
timestamps of the runs.

#### --name-filter, -f

A name can be manually assigned to a run. This option returns only those runs which
have a name and whose names start with the specified filter string.

#### --limit, -l

Often older runs are not important, so they can be disregarded. This option limits the
total number of runs that are returned.

#### Example

```bash
$ boxs list-runs --name-filter release --limit 2
List runs
|  box_id  |               run_id               |   name    |              time              |
 my-box-id  0e63ad74-8102-11ec-9c51-48f17f64520d release_1.1 2022-01-29 12:50:44.422651+00:00
 my-box-id  12d03f94-7e0a-11ec-9020-48f17f64520d release_1.0 2022-01-25 18:10:35.297825+00:00
```

### name-run

```bash
$ boxs name-run -h
usage: boxs name-run [-h] [-n NAME] RUN

positional arguments:
  RUN                   Run id or name, can be just the first characters.

optional arguments:
  -h, --help            show this help message and exit
  -n NAME, --name NAME  The new name of the run, if left out, the current name will be removed.
```

`name-run` sets the user-defined name of a run. When runs are automatically created,
they can only be referenced by their cryptic run-id. These run-ids are hard to keep in
mind. Therefore, it is possible to assign a name to a run.

#### RUN

This positional argument is required and contains the id or name of the run, whose
name should be modified. The value of argument doesn't have to be the complete name or
the complete run id, the beginning is sufficient, as long as it is not ambiguous.

#### --name, -n

The name that should be used for the run. If this argument is provided, the new name
will replace any previous name. If the argument is not provided, an existing name will
be removed.

#### Example

```bash
$ boxs name-run 0e63 -n release_1.1
Run name set 0e63ad74-8102-11ec-9c51-48f17f64520d
|  box_id  |               run_id               |   name    |              time              |
 my-box-id  0e63ad74-8102-11ec-9c51-48f17f64520d release_1.1 2022-01-29 12:50:44.422651+00:00
```

### delete-run

```bash
$ boxs delete-run -h
usage: boxs delete-run [-h] [-q] RUN

positional arguments:
  RUN          Run id or name, can be just the first characters.

optional arguments:
  -h, --help   show this help message and exit
  -q, --quiet  Don't ask for confirmation.
```

The `delete-run` command can be used to remove single runs.

!!! warning
    This command is potentially dangerous, because it deletes runs without checking
    for dependencies from other runs. Therefore, deleting a run can lead to
    inconsistencies!

#### RUN

This positional argument is required and contains the id or name of the run, which
should be deleted. The value of argument doesn't have to be the complete name or
the complete run id, the beginning is sufficient, as long as it is not ambiguous.

#### --quiet, -q

This option disables the usual confirmation question.

#### Example

```bash
$ boxs delete-run cbd
Really delete the run cbd0b6dd-7935-11ec-a32c-48f17f64520d? There might be other runs referencing data from it. (y/N)
```

### clean-runs

```bash
$ boxs clean-runs -h
usage: boxs clean-runs [-h] [-n] [-r COUNT] [-d] [-q]

optional arguments:
  -h, --help            show this help message and exit
  -n, --remove-named    Delete runs which have names.
  -r COUNT, --preserve-runs COUNT
                        Preserve the <COUNT> last runs. Defaults to 5.
  -d, --ignore-dependencies
                        Delete runs which contain data items referenced by kept runs.
  -q, --quiet           Don't ask for confirmation.
```

The `clean-runs` command can be used to remove runs that are no longer needed. It
removes all runs except the latest N. Runs including data items that are referenced
by kept runs and runs with names are kept, too, except if overridden.


#### --preserve-runs, -r

This optional argument sets the number of runs that should be kept. If not used, a
default value of 5 runs is used.

#### --removed-named, -n

If this option is set, named runs are deleted, too.

#### --ignore-dependencies, -d

With this option being set, no check for dependencies from the kept runs is performed.

!!! warning
    This option is potentially dangerous, because it deletes runs without checking
    for dependencies from other runs. This may lead to items with missing ancestors!

#### --quiet, -q

This option disables the usual confirmation question.

#### Example

```bash
$ boxs clean-runs -n 3 --ignore-dependencies
Delete runs
|  box_id  |               run_id               |name|              time              |
 my-box-id  c5caaee0-7d2e-11ec-9020-48f17f64520d      2022-01-24 16:00:38.460836+00:00
 my-box-id  c0b342b4-7c9d-11ec-abee-48f17f64520d      2022-01-23 22:42:32.900216+00:00
 my-box-id  bb157404-7c9c-11ec-abee-48f17f64520d      2022-01-23 22:36:59.329551+00:00
Really delete all listed runs? (y/N)
```

### list

```bash
$ boxs list -h
usage: boxs list [-h] QUERY

positional arguments:
  QUERY       The query in format [<box>:<data>:<run>] for the items which should be listed.

optional arguments:
  -h, --help  show this help message and exit
```

The `list` command allows to list data items that match a query.

#### QUERY

The query is a string, that can contain a box filter, a data filter and a run filter
separated by ":" in the format "<box>:<data>:<run>".

- box: The box filter matches all data items with a box id that begins with the filter.
- data: The data filter matches all data items where the data id or the data name
  begins with the filter.
- run: The run filter matches all data items with a run id or run name that begins
  with the filter.

Not all filters have to be provided. Preceding ":" can be left out.
The resulting data items listed have to match ALL filters.

Examples:
 - "my-box::release" Matches all items in a box that begins with "my-box" and whose run
   names begin with "release".
 - "train_data:" Matches all items with the name "train_data" in all boxes and all runs.
 - "::release-1.0" or "release-1.0" Matches all items in the run with the name
   "release-1.0"

#### Example

```bash
$ boxs  list ::release
List items boat_price::release
|  box_id  |    data_id     |               run_id               |         name         |              time              |
 my-box-id  d91bf391601f6f53 12d03f94-7e0a-11ec-9020-48f17f64520d trained_model          2022-01-25 18:10:35.297825+00:00
 my-box-id  cf64163dd7c1898c 0e63ad74-8102-11ec-9c51-48f17f64520d train_data             2022-01-29 12:50:37.482767+00:00
 my-box-id  efb03082d67da076 0e63ad74-8102-11ec-9c51-48f17f64520d eval_data              2022-01-29 12:50:37.490767+00:00
 my-box-id  7473d9676cbc377e 0e63ad74-8102-11ec-9c51-48f17f64520d encoded_train_input    2022-01-29 12:50:37.570765+00:00
 my-box-id  21bfb27c4a636f4e 0e63ad74-8102-11ec-9c51-48f17f64520d encoded_train_output   2022-01-29 12:50:37.582765+00:00
 my-box-id  800b5a5c9a972d3e 0e63ad74-8102-11ec-9c51-48f17f64520d encoded_eval_input     2022-01-29 12:50:37.606765+00:00
 my-box-id  ddb065be789696cb 0e63ad74-8102-11ec-9c51-48f17f64520d encoded_eval_output    2022-01-29 12:50:37.610765+00:00
 my-box-id  15d29183ad09cbfc 0e63ad74-8102-11ec-9c51-48f17f64520d normalized_train_input 2022-01-29 12:50:37.734763+00:00
 my-box-id  6f31c9a1c68b3b02 0e63ad74-8102-11ec-9c51-48f17f64520d normalized_eval_input  2022-01-29 12:50:37.762762+00:00
 my-box-id  d91bf391601f6f53 0e63ad74-8102-11ec-9c51-48f17f64520d trained_model          2022-01-29 12:50:44.418651+00:00
 my-box-id  b2560fb2c8b88621 0e63ad74-8102-11ec-9c51-48f17f64520d tensorboard_logs       2022-01-29 12:50:44.422651+00:00
```

### info

```bash
$ boxs list -h
usage: boxs info [-h] QUERY

positional arguments:
  QUERY       The query in format [<box>:<data>:<run>] for the item whose info should be printed.

optional arguments:
  -h, --help  show this help message and exit
```

The `info` command allows print information about a single data item identified by a
query.

#### QUERY

The query is a string, that can contain a box filter, a data filter and a run filter
separated by ":" in the format "<box>:<data>:<run>". Since the command can show only
one info at a time, the query has to be unambiguous, returning only a single item.

For more information see the [description for list](#query)

#### Example

```bash
$ boxs  info :trained_model:release_1.1
Info d91bf391601f6f53 0e63ad74-8102-11ec-9c51-48f17f64520d
 Property  Value                                                                                                                                                                                                                                                          
 ref     :
           box_id  : my-box-id                           
           data_id : d91bf391601f6f53                    
           run_id  : 0e63ad74-8102-11ec-9c51-48f17f64520d
 origin  : train                                                                                                                                                                                                                                                         
 name    : trained_model                                                                                                                                                                                                                                                 
 tags    : {}                                                                                                                                                                                                                                                            
 parents : [{'ref': {'box_id': 'my-box-id', 'data_id': '15d29183ad09cbfc', 'run_id': '0e63ad74-8102-11ec-9c51-48f17f64520d'}, 'origin': 'normalize_input_data', 'name': 'normalized_train_input', 'tags': {}, 'parents': [{'ref': {'box_id': 'boat_price', 'data_id':...
 meta    :
           value_type          : boxs.tensorflow:TensorflowKerasModelValueType:tf                
           size_in_bytes       : 1963138                                                         
           number_of_lines     : 12482                                                           
           store_start         : 2022-01-29T12:50:44.413+00:00                                   
           store_end           : 2022-01-29T12:50:44.413+00:00                                   
           model_format        : tf                                                              
           checksum_digest     : 8ec304eafb8453f7fe41d7547e7b297b1ef335f855acbf3085aa5a5e95e814e1
           checksum_digest_size: 32                                                              
           checksum_algorithm  : blake2b
```

### diff

```bash
$ boxs diff -h
usage: boxs diff [-h] [-d DIFF] [-l] QUERY QUERY [DIFF-ARG [DIFF-ARG ...]]

positional arguments:
  QUERY                 The queries in format [<box>:<data>:<run>] describing the items to compare.
  DIFF-ARG              Arbitrary arguments for the diff command.

optional arguments:
  -h, --help            show this help message and exit
  -d DIFF, --diff-command DIFF
                        The command to use for comparing, defaults to 'diff'.
  -l, --without-labels  Disable the labels.
```

The `diff` command creates a diff of two data items. It uses the system "diff" tool
for this, but the command that is called can be configured.

#### QUERY

The query is a string, that can contain a box filter, a data filter and a run filter
separated by ":" in the format "<box>:<data>:<run>". Since this command compares two
items, it takes two queries, Both queries have to be unambiguous, returning only a
single item.

For more information see the [description for list](#query)

#### DIFF-ARG

The command takes arbitrary additional commands that are relayed to the diff tool.

#### --diff-command, -d

This option allows to use a different command for creating the diff. If not specified
"diff" is used.

#### --without-labels, -l

The command line interface saves the data items to temporary files in order to be able
to use diff. When comparing those files, diff prints their temporary paths, which make
it hard to know which file is which item. Therefore, the tool adds additional "--label"
arguments to the diff call, which change the labels to be the item query.
Since not all diff tools support these option, "--without-labels" disables that those
options are added.

#### Example

```bash
$ boxs diff :trained_model:release_1.1 :trained_model:release_1.0
Binary files :trained_model:release_1.1 and :trained_model:release_1.0 differ
```


### export

```bash
$ boxs export -h
usage: boxs export [-h] QUERY FILE

positional arguments:
  QUERY       The query in format [<box>:<data>:<run>] describing the item to export.
  FILE        The file path to export to.

optional arguments:
  -h, --help  show this help message and exit
```

The `export` command exports data items to the local file system.

#### QUERY

The query is a string, that can contain a box filter, a data filter and a run filter
separated by ":" in the format "<box>:<data>:<run>". Since the command can export only
one item at a time, the query has to be unambiguous, returning only a single item.


For more information see the [description for list](#query)

#### FILE

A file path to which the data item should be exported.

#### Example

```bash
$ boxs export :trained_model:release_1.1 /tmp/model
:trained_model:release_1.1 successfully exported to /tmp/model
```


### graph

```bash
$ boxs graph -h
usage: boxs graph [-h] QUERY [FILE]

positional arguments:
  QUERY       The query describing the items to graph.
  FILE        The file to write the graph to. If left empty, the graph is written to stdout.

optional arguments:
  -h, --help  show this help message and exit
```

The `graph` command creates a representation of the dependency graph in DOT format.
It can be either written to a file or printed out to stdout.

#### QUERY

The query is a string, that can contain a box filter, a data filter and a run filter
separated by ":" in the format "<box>:<data>:<run>".

For more information see the [description for list](#query)

#### FILE

A file path to which the graph should be written. If the argument is left out, the
graph is printed to stdout.

#### Example

```bash
$ boxs graph :trained_model:release_1.1
digraph {
  subgraph "cluster_0e63ad74-8102-11ec-9c51-48f17f64520d" {
    label="Run 0e63ad74-8102-11ec-9c51-48f17f64520d";
    "boxs://boat_price/d91bf391601f6f53/0e63ad74-8102-11ec-9c51-48f17f64520d"[label="trained_model\norigin train\nsize 1963138"]
    "boxs://boat_price/ddb065be789696cb/0e63ad74-8102-11ec-9c51-48f17f64520d"[label="encoded_eval_output\norigin encode_data\nsize 32189"]
    "boxs://boat_price/efb03082d67da076/0e63ad74-8102-11ec-9c51-48f17f64520d"[label="eval_data\norigin partition_data\nsize 189854"]
    "boxs://boat_price/6f31c9a1c68b3b02/0e63ad74-8102-11ec-9c51-48f17f64520d"[label="normalized_eval_input\norigin normalize_input_data\nsize 327758"]
    "boxs://boat_price/800b5a5c9a972d3e/0e63ad74-8102-11ec-9c51-48f17f64520d"[label="encoded_eval_input\norigin encode_data\nsize 226617"]
...
```