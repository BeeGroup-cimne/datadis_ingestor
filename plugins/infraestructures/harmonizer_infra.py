import datetime
import sys
from datetime import timedelta
import pytz
import pandas as pd
import rdflib
import hashlib
from thefuzz import process
import beelib.beehbase
from beelib.beetransformation import map_and_save, save_to_neo4j, map_and_print
import settings
import neo4j
import beelib
from urllib.parse import quote
import os
import ast
import logging
from . import InfrastructuresPlugin

program = logging.getLogger("ManttestIngestor")
program.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
program.addHandler(handler)

namespace = "https://icat.cat#"

time_to_timedelta = {
    "PT1H": timedelta(hours=1),
    "PT15M": timedelta(minutes=15)
}

def create_hash(uri):
    uri = quote(uri, safe=':/#')
    uri = uri.encode()
    m = hashlib.sha256(uri)
    return m.hexdigest()

def fuzzy_locations(adm):
    g = rdflib.Graph()
    g.parse("plugins/icaen/all-geonames-rdf-clean-ES.rdf", format="xml")
    res = g.query(f"""SELECT ?name ?b WHERE {{
            ?b gn:name ?name.
            ?b gn:featureCode <https://www.geonames.org/ontology#{adm}> .
        }}
    """)
    return {str(x[0]): str(x[1]).split("/")[-2] for x in list(res)}


def safe_eval(val):
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except (ValueError, SyntaxError):
            return val
    return val


def harmonize_supplies(data):

    df = pd.DataFrame(data)

    ### df = pd.read_csv('supplies_7.csv')
    # df['measurements'] = df['measurements'].apply(lambda x: eval(x))

    # df = df[df['measurements'] != '[]']
    df = df[df['measurements'].apply(lambda x: x != [])]
    df = df.dropna(subset=['measurements'])


    df['measurements'] = df['measurements'].apply(safe_eval)

    if df.empty:
        return

    df = df.explode('measurements').reset_index(drop=True)
    df_measures = pd.DataFrame(df['measurements'].tolist(), columns=['prop', 'freq'])


    df = pd.concat([df.drop(columns=['measurements']), df_measures], axis=1)

    # Just in case:
    df = df[df['freq'] != 'P1M']
    if df.empty:
        return

    cups = list(df['cups'].str[:20].to_list())
    df['cups'] = cups

    ### cups = cups + [x + '0F' for x in cups]
    config = beelib.beeconfig.read_config(InfrastructuresPlugin.conf_file)

    cups = list(df['cups'].unique().tolist())

    driver = neo4j.GraphDatabase().driver(**config['neo4j'])
    with driver.session() as session:
        cups_ens = session.run(f"""MATCH (n:bigg__EnergySupplyPoint) WHERE n.bigg__energySupplyPointNumber
               in {cups} Match (n)<-[:s4syst__connectsAt]-()<-[:s4syst__hasSubSystem*]-()-[:ssn__hasDeployment]->
               (:s4agri__Deployment)-[:s4agri__isDeployedAtSpace]->(p:bigg__Patrimony) return p.bigg__idFromOrganization as patrimony,
               n.bigg__energySupplyPointNumber as cups""").data()
        cups_ens = {v['cups']: v['patrimony'] for v in cups_ens}

    df['patrimony'] = df['cups'].map(cups_ens)

    if df.empty:
        return

    df['uri'] = 'datadis-' + df['cups'] + '-' + df['prop'] + '-' + df['freq']
    df['measurement_uri'] = 'measurement-' + df['uri']
    df['hash'] = df.measurement_uri.apply(lambda x: create_hash(f"{namespace}{x}"))

    df['electric_uri'] = df['patrimony'].astype(str) + '-electric-grid-' + df['cups'] + '-energy-active-imported-from-grid'

    df['electric_measure_uri'] = 'measurement-' + df['electric_uri'].astype(str) + '-' + df['freq']
    df['hash_elec'] = df.electric_measure_uri.apply(lambda x: create_hash(f"{namespace}{x}"))

    # Cleaning fake data to avoid errors when creating columns
    df.loc[df['patrimony'].isna(), 'electric_uri'] = pd.NA
    df.loc[df['patrimony'].isna(), 'electric_measure_uri'] = pd.NA
    df.loc[df['patrimony'].isna(), 'hash_elec'] = pd.NA


    config = beelib.beeconfig.read_config(InfrastructuresPlugin.conf_file)


    beelib.beetransformation.map_and_save({"supplies": df.to_dict(orient="records")},
                                          "plugins/infraestructures/mapping.yaml", config)




def harmonize_timeseries(data, freq, prop):
    """
    :param data:
    :param freq:
    :param prop:
    :return:
    """
    return
    # if freq == 'P1M':
    #     return
    # df = pd.DataFrame(data)
    # # df.to_csv('timeseries.csv', index=False)
    # # df = pd.read_csv('timeseries_.csv')
    # df["start"] = df['timestamp']
    # df["bucket"] = (df['start'] // settings.TS_BUCKETS) % settings.BUCKETS
    # df['end'] = df.start + time_to_timedelta[freq].seconds
    # df['value'] = df['consumptionKWh']
    # df['isReal'] = df['obtainMethod'].apply(lambda x: True if x == "Real" else False)
    #
    # config = beelib.beeconfig.read_config("plugins/infraestructures/config_infra.json")
    # driver = neo4j.GraphDatabase().driver(**config['neo4j'])
    #
    # cups = list(df['cups'].str[:20].to_list())
    # df['cups'] = cups
    #
    # with driver.session() as session:
    #     cups_ens = session.run(f"""MATCH (n:bigg__EnergySupplyPoint) WHERE n.bigg__energySupplyPointNumber
    #               in {cups} Match (n)<-[:s4syst__connectsAt]-()<-[:s4syst__hasSubSystem*]-()-[:ssn__hasDeployment]->
    #               (:s4agri__Deployment)-[:s4agri__isDeployedAtSpace]->(p:bigg__Patrimony) return p.bigg__idFromOrganization as patrimony,
    #               n.bigg__energySupplyPointNumber as cups""").data()
    #     cups_ens = {v['cups']: v['patrimony'] for v in cups_ens}
    # program.debug(cups_ens)
    #
    # df['patrimony'] = df['cups'].map(cups_ens)
    # df = df.dropna(subset=['patrimony'])
    #
    # if df.empty:
    #     return
    #
    # df['freq'] = freq
    # df['prop'] = prop
    # df['uri'] = df['patrimony'] + '-datadis-' + df['cups'] + '-' + df['freq'] + '-' + df['prop']
    # # print(df['uri'])
    #
    # df['measurement_uri'] = 'measurement-' + df['uri']
    # df['hash'] = df.measurement_uri.apply(lambda x: create_hash(f"{namespace}{x}"))


def end_process():
    pass
    # config = beelib.beeconfig.read_config("plugins/infraestructures/config_infra.json")
    # driver = neo4j.GraphDatabase.driver(**config['neo4j'])
    # with driver.session() as session:
    #     session.run("""Match(n:bigg__UtilityPointOfDelivery)
    #     WHERE n.bigg__newSupply is NULL
    #     SET n.bigg__newSupply=true""")
