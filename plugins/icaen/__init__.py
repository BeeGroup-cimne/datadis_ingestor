from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from beelib import beesecurity
import os


class SIMEImport(DatadisInputPlugIn):
    conf_file="plugins/icaen/config.json";
    @classmethod
    def get_users_plugin(cls, config):
        driver = GraphDatabase.driver(**config['neo4j'])
        query = """Match(n:DatadisSource) return n.username as username, n.Password as password, n.authorized_nif as authorized_nif, 
                   n.self as self, n.cups as cups"""
        with driver.session() as session:
            users = pd.DataFrame(data=session.run(query).data())
            users['password'] = users.password.apply(beesecurity.decrypt, args=(config['secret_password'],))
        return users

    @classmethod
    def get_source(cls):
        return "sime"

    @classmethod
    def get_row_keys(cls):
        return [('cups', 'timestamp')]

    @classmethod
    def get_tables(cls):
        return ["datadis:raw_datadis_ts_{prop}_{freq}"]

    @classmethod
    def get_topic(cls):
        return 'datadis.simehbase'

def get_plugin():
    return SIMEImport

