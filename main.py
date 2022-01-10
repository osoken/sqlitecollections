import sqlitecollections as sc


def define_env(env):
    env.variables["package_version"] = sc.__version__
