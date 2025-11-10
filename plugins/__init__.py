import importlib
import os
import beelib.beeconfig


class DatadisInputPlugIn(object):
    conf_file = None
    @classmethod
    def get_users(cls):
        if cls.conf_file is None:
            raise NotImplemented
        config = beelib.beeconfig.read_config(cls.conf_file)
        return cls.get_users_plugin(config)

    @classmethod
    def get_users_plugin(cls, config):
        raise NotImplemented

    @classmethod
    def get_source(cls):
        raise NotImplemented

    @classmethod
    def get_row_keys(cls):
        raise NotImplemented

    @classmethod
    def get_tables(cls):
        raise NotImplemented

    @classmethod
    def get_topic(cls):
        raise NotImplemented

    @classmethod
    def prepare_raw_data(cls, df):
        return df


def get_plugins():
    plugins_path = [x for x in os.listdir("plugins") if
                    os.path.isdir(f"plugins/{x}") and not any([x.startswith(c) for c in [".", "_", "infra"]])]
    plugins = []
    for m in plugins_path:
        module = importlib.import_module(f"plugins.{m}")
        plugins.append(module.get_plugin())
    return plugins
