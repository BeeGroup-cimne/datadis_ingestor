from neo4j import GraphDatabase
import pandas as pd
from plugins import DatadisInputPlugIn
from beelib import beesecurity
import os

# Fragment used to build each saref:Measurement uri:
# https://cosmic.cat#{fragment}-{cups}-{freq}
# Mirrors cosmic_ingestor/sources/datadis/harmonizer/static_mapping.py::RAW_MEASURES
# and the saref:makesMeasurement targets in mapping.yaml.
RAW_MEASURE_FRAGMENTS = {
    "consumptionKWh": "measurement-energy-consumptionKWh",
    "surplusEnergyKWh": "measurement-energy-surplusEnergyKWh",
    "selfConsumptionEnergyKWh": "measurement-self-consumption-energy-kwh",
    "generationEnergyKWh": "measurement-energy-generation-energy-kwh",
}


class COSMICImport(DatadisInputPlugIn):
    config_file = "plugins/secrets/config_cosmic.json"
    source = "cosmic"
    row_keys = [('uri', 'timestamp')]
    tables = ["datadis:raw_datadis_ts_{prop}_{freq}"]
    topic = 'datadis.cosmichbase'

    @staticmethod
    def prepare_raw_data(df):
        freq = df['freq'].iloc[0] if 'freq' in df.columns and len(df) else None
        present = [c for c in RAW_MEASURE_FRAGMENTS if c in df.columns]
        if not present or freq not in ("PT1H", "PT15M"):
            # Not a consumption chunk with known sub-properties (e.g. max power) -
            # no known uri template for those yet, leave untouched.
            return df

        base_cols = [c for c in df.columns if c not in present]
        split_rows = []
        for column in present:
            fragment = RAW_MEASURE_FRAGMENTS[column]
            chunk = df[base_cols + [column]].rename(columns={column: "value"})
            chunk = chunk.dropna(subset=["value"])
            if chunk.empty:
                continue
            chunk["property"] = column
            chunk["uri"] = chunk["cups"].apply(lambda cups: f"https://cosmic.cat#{fragment}-{cups}-{freq}")
            split_rows.append(chunk)

        if not split_rows:
            return df.iloc[0:0]
        return pd.concat(split_rows, ignore_index=True)

    def get_users(self):
        driver = GraphDatabase.driver(**self.config['neo4j'])
        query = """Match(n:DatadisSource) 
        return n.username as username, n.Password as password, n.authorized_nif as authorized_nif, 
        n.self as self, n.cups as cups"""
        with driver.session() as session:
            users = pd.DataFrame(data=session.run(query).data())
            users['password'] = users.password.apply(beesecurity.decrypt, args=(self.config['secret_password'],))
        return users

def get_plugin():
    return COSMICImport

