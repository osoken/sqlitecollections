import importlib
import os
import re
import sys
from argparse import ArgumentParser
from typing import Tuple, TypeVar

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


if __name__ == "__main__":
    parser = ArgumentParser()
    subcommand_parser = parser.add_subparsers(dest="subcommand")
    benchmarking_parser = subcommand_parser.add_parser("benchmarking")
    benchmarking_parser.add_argument("--prefix", default="benchmarks")
    benchmarking_parser.add_argument("--timeout", default=None, type=float)
    benchmarking_parser.add_argument("--debug", action="store_true")
    benchmarking_parser.add_argument("targets", nargs="*")
    args = parser.parse_args()
    if args.subcommand == "benchmarking":
        wd = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(os.path.dirname(os.path.dirname(wd)), "benchmark_results", args.prefix)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        env = Environment(loader=FileSystemLoader(wd), autoescape=select_autoescape())
        template = env.get_template("template.j2")

        for fn in filter(lambda x: x.startswith("benchmark_") and x.endswith(".py"), os.listdir(wd)):
            container_type_str = re.sub(r"benchmark_([a-z]+)\.py", "\\1", fn)
            m = importlib.import_module("scbenchmarker.{}".format(re.sub('\\.py$', '', fn)))
            builtin_base = getattr(
                m, get_element_by_condition(lambda s: re.match(r"Builtin[A-Za-z]+BenchmarkBase", s), dir(m))
            )
            sqlitecollections_base = getattr(
                m, get_element_by_condition(lambda s: re.match(r"SqliteCollections[A-Za-z]+BenchmarkBase", s), dir(m))
            )
            buf = []
            for benchmark_cls in (
                getattr(m, cn) for cn in filter(lambda x: re.match(r"^Benchmark.+Base$", x) is not None, dir(m))
            ):
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
                buf.append(comp().dict())
                print(".", end="")
            with open(os.path.join(output_dir, f"{container_type_str}.md"), "w") as fout:
                fout.write(template.render(data=buf))
            print("")
