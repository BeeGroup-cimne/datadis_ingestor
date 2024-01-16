from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
import utils


class InfrastructuresPlugin(DatadisInputPlugIn):
    config = utils.config.read_config()

    @classmethod
    def get_users(cls):
        users = pd.DataFrame.from_records(cls.config['infra_plugin'])
        # users['password'] = users.password.apply(utils.security.decrypt, args=(cls.config['secret_password'],))
        return users

    @classmethod
    def get_source(cls):
        return "sime"


def get_plugin():
    return InfrastructuresPlugin


