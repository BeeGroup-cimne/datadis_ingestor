import importlib
import os


class DatadisInputPlugIn(object):
    @classmethod
    def get_users(self):
        raise NotImplemented

    @classmethod
    def get_source(self):
        raise NotImplemented


def get_plugins():
    plugins_path = [x for x in os.listdir("plugins") if
                    os.path.isdir(f"plugins/{x}") and not any([x.startswith(c) for c in [".", "_"]])]
    plugins = []
    for m in plugins_path:
        module = importlib.import_module(f"plugins.{m}")
        plugins.append(module.get_plugin())
    return plugins
