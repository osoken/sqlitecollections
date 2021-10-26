# Development

## Tests, type checking and linting locally


To run tests, type checking and linting locally, you'll need to have **python 3.6**, **3.7**, **3.8**, **3.9** and **3.10** installed.
We use `tox` to run tests and type checking on all the supported python versions.
You can set up the development environment with the following commands:

```
git clone git@github.com:osoken/sqlitecollections.git
cd sqlitecollections
python -m venv .venv
source ./.venv/bin/activate
pip install -e .[dev]
```

Then, run tests is as easy as:

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
  congratulations :)
```

## Compatibility policy

We aim to implement containers that are as compatible as possible with the built-in containers, but we have a few implementations that intentionally behave differently.

- Normal behavior will be compatible, but in case of errors it may be different.
- The constructor arguments are not compatible, as they require arguments specific to this package's container, such as sqlite3 DB file paths and serialization methods.
- `copy` method in each container behaves similarly to deep copy, since it copies the table containing serialized elements.
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
