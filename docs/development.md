# Development

## Tests, type checking and linting locally

To run tests, type checking and linting locally, you'll need to have **python 3.6**, **3.7**, **3.8**, **3.9**, **3.10** and **3.11** installed.
We use `tox` to run tests and type checking on all the supported python versions.
You can set up the development environment with the following commands:

```
git clone git@github.com:osoken/sqlitecollections.git
cd sqlitecollections
python -m venv .venv
source ./.venv/bin/activate
pip install -e .[dev]
```

Then, just type the following command to run the test:

```
tox
```

After a while, you'll see the following message at the bottom of the long logs from `pytest` and others.

```
__________________ summary __________________
  lint: commands succeeded
  py36: commands succeeded
  py37: commands succeeded
  py38: commands succeeded
  py39: commands succeeded
  py310: commands succeeded
  py311: commands succeeded
  congratulations :)
```

## Building documents

We use `mkdocs` to build the documentation.
To set up the environment for building the document, run the following commands:

```
git clone git@github.com:osoken/sqlitecollections.git
cd sqlitecollections
python -m venv .venv
source ./.venv/bin/activate
pip install -e .[docs]
```

Then, building the documentation can be done by the following command:

```
mkdocs build
```

The output will be located in `site` directory in your current directory.

During development, you can also run `mkdoc`'s builtin development server with hot-reloading enabled with the following command:

```
mkdocs serve
```

In that case, you can check the result on `http://127.0.0.1:8000`.

### Benchmarks

We have our own benchmark package.

#### Setup the benchmark package

To setup, run the following commands:

```
pip install docs/scbenchmarker
```

#### Run all benchmarks and render the results

Benchmarking is done in two steps:

Step 1: Run all benchmarks

```
python -m scbenchmarker --prefix=[prefix] benchmarking
```

Step 2: Render the results to markdown:

```
python -m scbenchmarker --prefix=[prefix] render
```

Results are stored in `docs/benchmark_results/[prefix]/`.

You can check the results on `http://127.0.0.1:8000/benchmark/`.

#### Verbose mode

```
python -m scbenchmarker --verbose --prefix=[prefix] benchmarking
```

An example output is as follows:

```
benchmark_dict.py::BenchmarkSetitemAddNewItem: {'subject': '`__setitem__` (add new item)', 'one': {'name': '`dict`', 'timing': 0.0026721060276031494, 'memory': 0.00390625}, 'another': {'name': '`sqlitecollections.Dict`', 'timing': 0.006042510271072388, 'memory': 0.00390625}, 'ratio': {'timing': 2.2613287828598834, 'memory': 1.0}}
```

#### Run individual benchmarks

You can also run individual benchmarks.

```
python -m scbenchmarker --prefix=[prefix] benchmarking [benchmark script]::[benchmark name]
```

This command runs only a single benchmark.
Together with the verbose mode, this is convenient for improving a single method and checking benchmarks frequently.

## Compatibility policy

We aim to implement containers that are as compatible as possible with the built-in containers, but we have a few implementations that intentionally behave differently.

- Normal behavior will be compatible, but in case of errors it may be different.
- The constructor arguments are not compatible, as they require arguments specific to this package's container, such as sqlite3 DB file paths and serialization methods.
- `copy` method in each container behaves similarly to deep copy, since it copies the table containing serialized elements.
- `Dict`'s item order is guaranteed to be insertion order not only for python 3.7 and upper but for all versions.
- `Dict.fromkeys` class method is not provided.
- Any member in the container cannot be mutated directly. If you want to mutate any member, mutate it via temporary variable then write it back.

```python
import sqlitecollections as sc

x = sc.Dict({"a": []}) # create {"a": []}
x["a"].append("b")  # try to mutate the empty list
print(x["a"])  # not ["b"] but []

temp = x["a"]  # temporarily substitute the list to a variable
temp.append("b")  # mutate the temporary variable
x["a"] = temp  # then, write it back
print(x["a"])  # now, we get ["b"]
```
