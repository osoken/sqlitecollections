import sqlitecollections


def define_env(env):
    env.variables["package_version"] = sqlitecollections.__version__
