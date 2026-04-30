from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from beelib import beesecurity
import os
import beelib


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

        # Set all Datadis devices as not enrolled with a new property to correct them in the harmonization
        queryEnrollment = """
                MATCH (n:bigg__Device) 
                WHERE n.source = 'DatadisSource'
                SET n.bigg__enrolled = False
                """
        config_sime = beelib.beeconfig.read_config('plugins/secrets/config_sime.json')
        driver = GraphDatabase.driver(**config_sime['neo4j'])
        with driver.session() as session:
            session.run(queryEnrollment)

        return users


def get_plugin():
    return SIMEImport
