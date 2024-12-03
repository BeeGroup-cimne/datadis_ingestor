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

    @classmethod
    def solve_multisources(cls):
        db = GraphDatabase.driver(**cls.config['neo4j'])
        query = """
                MATCH (n:bigg__Device)
                WHERE NOT isEmpty([key IN keys(n) WHERE key =~ "bigg__nif_.*"])
                RETURN {
                  dev: n.uri, 
                  nifs: apoc.map.fromPairs(
                    [key IN keys(n) WHERE key =~ "bigg__nif_.*" | [key, n[key]]]
                  )
                } AS data
            """
        with db.session() as session:
            data = session.run(query).data()
        df = pd.DataFrame.from_records([x['data'] for x in data])
        filtered_df = df[df['nifs'].apply(lambda x: len(x) >= 2)]

        for _, row in filtered_df.iterrows():
            nifs = row['nifs']
            max_nif_pair = max(nifs.items(), key=lambda item: item[1])

            if max_nif_pair[1] == 'Alta':
                other_keys = [key[10:] for key in nifs if key != max_nif_pair[0]]
                db = GraphDatabase.driver(**cls.config['neo4j'])
                query = f"""
                        MATCH (u:bigg__UtilityPointOfDelivery)-[:bigg__hasDevice]->(n:bigg__Device{{uri:'{row.dev}'}})
                        -[r:importedFromSource]->(d:DatadisSource)
                        where d.username in {other_keys}
                        REMOVE n.bigg__endDate
                        REMOVE u.bigg__endDate
                        DELETE r
                        """
                with db.session() as session:
                    session.run(query)

            else:
                query = f"""
                        MATCH (u:bigg__UtilityPointOfDelivery)-[:bigg__hasDevice]->(n:bigg__Device{{uri:'{row.dev}'}})
                        -[r:importedFromSource]->(d:DatadisSource)
                        SET n.bigg__endDate = localdatetime("{max_nif_pair[1]}")
                        SET u.bigg__endDate = localdatetime("{max_nif_pair[1]}")
                        DELETE r
                        """
                with db.session() as session:
                    session.run(query)

        unique_nifs = list(
            set(key for dictionary in df["nifs"] if isinstance(dictionary, dict) for key in dictionary.keys())
        )

        # Delete all nifs properties
        for nif in unique_nifs:
            query = f"""
                    MATCH (n:bigg__Device) 
                    WHERE n.{nif} IS NOT NULL
                    REMOVE n.{nif}
                    """
            with db.session() as session:
                session.run(query)


def get_plugin():
    return SIMEImport

