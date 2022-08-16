import importlib
import json
import os
import re
import sys
from argparse import ArgumentParser
from collections import defaultdict
from typing import Tuple, TypeVar

import sqlitecollections as sc
from sqlitecollections import factory

if sys.version_info >= (3, 9):
    from collections.abc import Callable, Iterable
else:
    from typing import Callable, Iterable

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .common import Comparison

T = TypeVar("T")


def get_element_by_condition(condition: Callable[[T], bool], iter: Iterable[T]) -> T:
    for d in filter(condition, iter):
        return d
    raise ValueError


def is_special_benchmark_class(x: type, base1: type, base2: type):
    try:
        return issubclass(x, base1) and issubclass(x, base2)
    except TypeError as _:
        return False


def parse_target(s: str) -> Tuple[str]:
    return tuple(s.split("::"))


def get_container_type_str(s: str) -> str:
    return re.sub(r"benchmark_([a-z]+)\.py", "\\1", s)


if __name__ == "__main__":
    wd = os.path.dirname(os.path.abspath(__file__))

    parser = ArgumentParser()
    parser.add_argument("--prefix", default="benchmarks")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--result-cache")

    subcommand_parser = parser.add_subparsers(dest="subcommand")

    benchmarking_parser = subcommand_parser.add_parser("benchmarking")
    benchmarking_parser.add_argument("--timeout", default=None, type=float)
    benchmarking_parser.add_argument("--debug", action="store_true")

    benchmarking_parser.add_argument("targets", nargs="*")

    render_parser = subcommand_parser.add_parser("render")
    render_parser.add_argument("--output-dir")

    args = parser.parse_args()

    cache_path = args.result_cache or os.path.join(os.path.dirname(os.path.dirname(wd)), "temp", "benchmark.db")
    cache_dir = os.path.dirname(cache_path)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    dict_ = factory.DictFactory[str, dict](
        connection=cache_path,
        key_serializer=lambda x: x.encode("utf-8"),
        key_deserializer=lambda x: x.decode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    cache_dict = dict_[args.prefix]()

    if args.subcommand == "benchmarking":
        filter_set = set(parse_target(t) for t in args.targets)
        benchmark_filter = (lambda x: True) if len(filter_set) == 0 else (lambda x: x in filter_set)
        for fn in filter(lambda x: x.startswith("benchmark_") and x.endswith(".py"), os.listdir(wd)):
            printed = False
            container_type_str = get_container_type_str(fn)
            m = importlib.import_module("scbenchmarker.{}".format(re.sub('\\.py$', '', fn)))
            builtin_base = getattr(
                m, get_element_by_condition(lambda s: re.match(r"Builtin[A-Za-z]+BenchmarkBase", s), dir(m))
            )
            sqlitecollections_base = getattr(
                m, get_element_by_condition(lambda s: re.match(r"SqliteCollections[A-Za-z]+BenchmarkBase", s), dir(m))
            )
            for benchmark_name, benchmark_cls in (
                (re.sub("Base$", "", cn), getattr(m, cn))
                for cn in filter(lambda x: re.match(r"^Benchmark.+Base$", x) is not None, dir(m))
            ):
                if not benchmark_filter((fn, benchmark_name)):
                    continue
                try:
                    builtin_benchmark_class = get_element_by_condition(
                        lambda x: is_special_benchmark_class(x, builtin_base, benchmark_cls),
                        (getattr(m, cn) for cn in dir(m)),
                    )
                except ValueError as _:

                    class _(builtin_base, benchmark_cls):
                        ...

                    builtin_benchmark_class = _
                try:
                    sqlitecollections_benchmark_class = get_element_by_condition(
                        lambda x: is_special_benchmark_class(x, sqlitecollections_base, benchmark_cls),
                        (getattr(m, cn) for cn in dir(m)),
                    )
                except ValueError as _:

                    class _(sqlitecollections_base, benchmark_cls):
                        ...

                    sqlitecollections_benchmark_class = _
                comp = Comparison(
                    builtin_benchmark_class(timeout=args.timeout, debug=args.debug),
                    sqlitecollections_benchmark_class(timeout=args.timeout, debug=args.debug),
                )
                res = comp()
                cache_dict[f"{fn}::{benchmark_name}"] = dict(res.dict(), **{"class": benchmark_name})
                if args.verbose:
                    print(f"{fn}::{benchmark_name}: {res.dict()}")
                else:
                    print(".", end="")
                printed = True
            if printed:
                print("")
    elif args.subcommand == "render":
        output_dir = (
            args.output_dir
            if args.output_dir is not None
            else os.path.join(os.path.dirname(os.path.dirname(wd)), "benchmark_results", args.prefix)
        )
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        env = Environment(loader=FileSystemLoader(wd), autoescape=select_autoescape())
        template = env.get_template("template.j2")
        subjects = defaultdict(list)
        for k in cache_dict.keys():
            subjects[parse_target(k)[0]].append(k)
        for fn, keys in subjects.items():
            with open(os.path.join(output_dir, f"{get_container_type_str(fn)}.md"), "w") as fout:
                fout.write(template.render(data=[cache_dict[k] for k in sorted(keys)]))
    else:
        parser.print_help()
