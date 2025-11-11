import importlib
import os
import beelib.beeconfig


class DatadisInputPlugIn(object):
    source = None
    row_keys = None
    tables = None
    topic = None
    config_file = None
    def __init__(self):
        self.config = beelib.beeconfig.read_config(self.config_file)

    def get_users(self):
        raise NotImplemented

    @staticmethod
    def prepare_raw_data(df):
        return df


def get_plugins():
    plugins_path = [x for x in os.listdir("plugins") if
                    os.path.isdir(f"plugins/{x}") and "__init__.py" in os.listdir(f"plugins/{x}")]
    plugins = []
    for m in plugins_path:
        module = importlib.import_module(f"plugins.{m}")
        plugins.append(module.get_plugin())
    return plugins
