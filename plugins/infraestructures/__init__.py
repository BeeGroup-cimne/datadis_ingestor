from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from beelib import beesecurity, beeconfig


class InfrastructuresPlugin(DatadisInputPlugIn):
    config = beeconfig.read_config("plugins/infraestructures/config_infra.json")

    @classmethod
    def get_users(cls):
        driver = GraphDatabase.driver(**cls.config['neo4j'])
        query = "Match(n:bee__Datadis) return n.username as username, n.Password as password, n.authorized_nif as authorized_nif"
        with driver.session() as session:
            users = pd.DataFrame(data=session.run(query).data())
            users['password'] = users.password.apply(beesecurity.decrypt, args=(cls.config['secret_password'],))
        return users

    @classmethod
    def get_source(cls):
        return "icat"

    @classmethod
    def get_row_keys(cls):
        return [('raw_uri', 'timestamp'), ('timestamp', 'raw_uri')]

    @classmethod
    def get_tables(cls):
        return ["datadis:timeseries_id_{freq}", "datadis:timeseries_time_{freq}"]

    @classmethod
    def get_topic(cls):
        return 'datadis.hbase'

    @classmethod
    def prepare_raw_data(cls, df):
        df['raw_uri'] = 'https://icat.cat#measurement-datadis-' + df['cups'] + '-' + df['prop'] + '-' + df['freq']
        return df


def get_plugin():
    return InfrastructuresPlugin
