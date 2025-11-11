from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from beelib import beesecurity
import os

class SIMEImport(DatadisInputPlugIn):
    config_file = "plugins/secrets/config_sime.json"
    source = "sime"
    row_keys = [('cups', 'timestamp')]
    tables = ["datadis:raw_datadis_ts_{prop}_{freq}"]
    topic = 'datadis.simehbase'

    def get_users(self):
        driver = GraphDatabase.driver(**self.config['neo4j'])
        query = """Match(n:DatadisSource) return n.username as username, n.Password as password, n.authorized_nif as authorized_nif, 
                   n.self as self, n.cups as cups"""
        with driver.session() as session:
            users = pd.DataFrame(data=session.run(query).data())
            users['password'] = users.password.apply(beesecurity.decrypt, args=(self.config['secret_password'],))
        return users


def get_plugin():
    return SIMEImport

