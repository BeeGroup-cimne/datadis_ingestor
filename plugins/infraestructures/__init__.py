from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
import utils


class InfrastructuresPlugin(DatadisInputPlugIn):
    config = utils.config.read_config("plugins/infraestructures/config_infra.json")

    @classmethod
    def get_users(cls):
        users = pd.DataFrame.from_records(cls.config['infra_plugin'])
        return users

    @classmethod
    def get_source(cls):
        return "icat"


def get_plugin():
    return InfrastructuresPlugin


