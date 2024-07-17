from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from beelib import beesecurity, beeconfig


class InfrastructuresPlugin(DatadisInputPlugIn):
    config = beeconfig.read_config("plugins/infraestructures/config_infra.json")

    @classmethod
    def get_users(cls):
        driver = GraphDatabase.driver(**cls.config['neo4j'])
        query = "Match(n:bee__DatadisSource) return n.username as username, n.Password as password, n.authorized_nif as authorized_nif"
        with driver.session() as session:
            users = pd.DataFrame(data=session.run(query).data())
            users['password'] = users.password.apply(beesecurity.decrypt, args=(cls.config['secret_password'],))
        return users

    @classmethod
    def get_source(cls):
        return "icat"


def get_plugin():
    return InfrastructuresPlugin


