from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from beelib import beesecurity, beeconfig
import os


class SIMEImport(DatadisInputPlugIn):
    config = beeconfig.read_config("plugins/icaen/config_icaen.json")

    @classmethod
    def get_users(cls):
        driver = GraphDatabase.driver(**cls.config['neo4j'])
        query = "Match(n:DatadisSource) return n.username as username, n.Password as password"
        with driver.session() as session:
            users = pd.DataFrame(data=session.run(query).data())
            users['password'] = users.password.apply(beesecurity.decrypt, args=(cls.config['secret_password'],))
        users['authorized_nif'] = ""
        return users

    @classmethod
    def get_source(cls):
        return "sime"


def get_plugin():
    return SIMEImport

