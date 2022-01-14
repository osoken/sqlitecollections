import os

import sqlitecollections as sc

wd = os.path.dirname(os.path.abspath(__file__))

def define_env(env):
    env.variables["package_version"] = sc.__version__

    env.variables["benchmarks"] = "\n".join(f"""
=== "{p}"
    === "dict"
        {{!benchmark_results/{p}/dict.md!}}
    === "list"
        {{!benchmark_results/{p}/list.md!}}
    === "set"
        {{!benchmark_results/{p}/set.md!}}
""" for p in os.listdir(os.path.join(wd, "benchmark_results")) if os.path.isdir(os.path.join(wd, "benchmark_results", p)))
