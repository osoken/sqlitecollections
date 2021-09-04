# sqlitecollections

Python collections that are backended by sqlite3 DB and are compatible with the built-in collections

## Installation

```
$ pip install git+ssh://git@github.com/osoken/sqlitecollections.git
```

## Development

To run tests, type checking and linting locally, we use `tox`.
It will run `pytest`, `mypy` and `black` on _python 3.6_, _3.7_, _3.8_ and _3.9_.
Install them via the following commands:

```
$ git clone git@github.com:osoken/sqlitecollections.git
$ cd sqlitecollections
$ python -m venv .venv
$ source ./.venv/bin/activate
$ pip install -e .[dev]
```

then, run tests:

```
$ tox
```

## Compatibility policy

We aim to implement containers that are as compatible as possible with the built-in containers, but we have a few implementations that intentionally behave differently.

- Normal behavior will be compatible, but in case of errors it may be different.
- The constructor arguments are not compatible, as they require arguments specific to this package's container, such as sqlite3 DB file paths and serialization methods.
- `Dict`'s item order is guaranteed to be insertion order not only for python 3.7 and upper but for all versions.
- `Dict.fromkeys` class method is not provided.
- Any member in the container cannot be mutated directly. If you want to mutate any member, mutate it via temporary variable then write it back.

```python
from sqlitecollections import Dict

x = Dict(data={"a": []}) # create {"a": []}
x["a"].append("b")  # try to mutate the empty list
print(x["a"])  # not ["b"] but []

temp = x["a"]  # temporarily substitute the list to a variable
temp.append("b")  # mutate the temporary variable
x["a"] = temp  # then, write it back
print(x["a"])  # now, we get ["b"]
```
