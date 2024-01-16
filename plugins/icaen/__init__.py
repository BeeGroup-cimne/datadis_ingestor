from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from utils import security, config


class SIMEImport(DatadisInputPlugIn):
    config = config.read_config()

    @classmethod
    def get_users(cls):
        driver = GraphDatabase.driver(**cls.config['neo4j'])
        query = "Match(n:DatadisSource) return n.username as username, n.Password as password"
        with driver.session() as session:
            users = pd.DataFrame(data=session.run(query).data())
            users['password'] = users.password.apply(security.decrypt, args=(cls.config['secret_password'],))
        users['authorized_nif'] = ""
        return users

    @classmethod
    def get_source(cls):
        return "sime"


def get_plugin():
    return SIMEImport

