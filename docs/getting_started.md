# Getting started

## Install the library

Install the latest version from [PyPI](https://pypi.org/project/boxs/)
using pip:

```bash
pip install boxs
```

## Configure where boxs stores your data

Within your python script, import `boxs` and create a new `Box` with a `Storage`:

```python
import boxs

...

storage = boxs.FileSystemStorage('/path/to/my/data')
box = boxs.Box('my-box-id', storage)
```

## Store data in your box

Use the `store()` function to store your data, which returns a `DataInfo` object with
a reference to the stored data and some additional meta-data:

```python
...

def my_function(x):
    ...
    value = ...
    data = boxs.store(value, box='my-box-id', name='my-data')
    return data

```

## Load the data from the reference

Loading the data can be done either directly on the `DataInfo` object, or by giving it
as argument to the free `load()` function from the `boxs` package:

```python
...

def my_other_function(data):
    value = boxs.load(data)

    # or directly from the reference
    value = data.load()
```

## List your last runs

Boxs comes with a command-line interface that allows to interact with the cached data. It can be
used e.g. to list the runs:

```bash
$ boxs -i 'my_python_module' -b 'my-box-id' list-runs
List runs
|  box_id  |               run_id               |   name    |              time              |
 my-box-id  0e63ad74-8102-11ec-9c51-48f17f64520d             2022-01-29 12:50:44.422651+00:00
 my-box-id  12d03f94-7e0a-11ec-9020-48f17f64520d saved_model 2022-01-25 18:10:35.297825+00:00
 my-box-id  ed923ea0-7df8-11ec-9020-48f17f64520d             2022-01-25 16:07:43.658161+00:00
 my-box-id  118d9332-7df3-11ec-9020-48f17f64520d             2022-01-25 15:25:47.026477+00:00
 my-box-id  bec3dfb2-7df2-11ec-9020-48f17f64520d             2022-01-25 15:23:28.143102+00:00
 my-box-id  c5caaee0-7d2e-11ec-9020-48f17f64520d             2022-01-24 16:00:38.460836+00:00
 my-box-id  c0b342b4-7c9d-11ec-abee-48f17f64520d             2022-01-23 22:42:32.900216+00:00
 my-box-id  bb157404-7c9c-11ec-abee-48f17f64520d             2022-01-23 22:36:59.329551+00:00
 my-box-id  cbd0b6dd-7935-11ec-a32c-48f17f64520d First run   2022-01-19 14:40:50.280643+00:00

```
## Compare items between two runs

Boxs comes with a command-line interface that allows to interact with the cached data. It can be
used e.g. to compare some data artifacts between different runs:

```bash
$ boxs -i 'my_python_module' -b 'my-box-id'  diff :train_data:c0b :train_data:0e6 -- -y | head
,Unnamed: 0,manufacturer,year,length,price                     ,Unnamed: 0,manufacturer,year,length,price
11263,11263,lagoon,2003,12.37,209000                          | 6640,6640,ohlson (se),1980,8.8,15308
7536,7536,bremer bootsbau vegesack (de),1979,14.2,61898       | 3036,3036,prout sail boats,1980,15.0,110000
9778,9778,beneteau,2007,17.8,267000                           | 1241,1241,bavaria,2018,9.99,118044
3122,3122,UNKNOWN,1930,9.25,4034                              | 11463,11463,ovington boats sail boats,1998,9.42,36860
252,252,UNKNOWN,1999,18.84,661860                             | 7617,7617,jeanneau,2013,13.34,115000
```

## Where to go from here?

Read the [user guide](../user_guide/) for some more in-depth explanation about boxs and
its concepts.

- What types of values can be stored?
 - [Standard python values](../value_types/common/)
 - [Pandas](../value_types/pandas/)
 - [Tensorflow](../value_types/tensorflow/)
 - [How to write value types for your own values?](../user_guide/#implementing-custom-value-types)
- How to use all functions of the CLI:
    [Command line tool functions?](../cli/)
- Where can I store the data?
    [`boxs.filesystem.FileSystemStorage`](../storages/file/)
