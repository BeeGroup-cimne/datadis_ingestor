import datetime
from datetime import timedelta
import pytz
import pandas as pd
import rdflib
import hashlib
from thefuzz import process
import beelib.beehbase
from beelib.beetransformation import map_and_save, save_to_neo4j
import settings
import neo4j
import beelib
import numpy as np


time_to_timedelta = {
    "PT1H": timedelta(hours=1),
    "PT15M": timedelta(minutes=15)
}


def fuzzy_locations(adm):
    g = rdflib.Graph()
    g.parse("plugins/icaen/all-geonames-rdf-clean-ES.rdf", format="xml")
    res = g.query(f"""SELECT ?name ?b WHERE {{
            ?b gn:name ?name.
            ?b gn:featureCode <https://www.geonames.org/ontology#{adm}> .
        }}
    """)
    return {str(x[0]): str(x[1]).split("/")[-2] for x in list(res)}


def send_to_kafka(producer, kafka_topic, df_to_send):
    df_list = df_to_send.to_list()
    data = None
    for i in range(0, len(df_list), 500):
        for data in df_list[i:i + 500]:
            producer.send(kafka_topic, data)
        producer.flush()
    return data


def harmonize_for_influx(data, timestamp_key, end, value_key, hash_key, is_real):
    """harmonizes the timeseries to be sent to druid"""
    to_save = {
        "start": int(data[timestamp_key]),
        "end": int((data[end])) - 1,
        "value": data[value_key],
        "isReal": is_real,
        "hash": data[hash_key]
    }
    return to_save


def harmonize_supplies(data):
    df = pd.DataFrame(data)
    config = beelib.beeconfig.read_config("plugins/icaen/config_icaen.json")

    # If the CUPS is old, keep its relationship with its current ENS
    driver = neo4j.GraphDatabase().driver(**config['neo4j'])
    with driver.session() as session:
        cups_ens = session.run("""MATCH (n:bigg__UtilityPointOfDelivery)<-[:bigg__hasUtilityPointOfDelivery]-
        (:bigg__BuildingSpace)<-[:bigg__hasSpace]-(b:bigg__Building) 
        RETURN distinct n.bigg__pointOfDeliveryIDFromOrganization as cups, b.bigg__buildingIDFromOrganization 
        as ens""").data()
        cups_ens = {v['cups']: v['ens'] for v in cups_ens}
    df['supply_name'] = df['cups'].str[:20]
    df['ens'] = df.supply_name.map(cups_ens)

    # If the CUPS is new, then link it to the corresponding generic building
    driver = neo4j.GraphDatabase().driver(**config['neo4j'])
    with driver.session() as session:
        nif_ens = session.run("""MATCH (o:bigg__Organization)-[:bigg__hasSubOrganization]->(g:bigg__Organization)-
        [:bigg__managesBuilding]->(b:bigg__Building) WHERE g.generic=1
        RETURN o.bigg__organizationIDFromOrganization as nif, b.bigg__buildingIDFromOrganization as ens""").data()
        nif_ens = {v['nif']: v['ens'] for v in nif_ens}
    df.loc[df["ens"].isna(), "ens"] = df["nif"].map(nif_ens)

    locations_mun = fuzzy_locations("A.ADM3")
    locations_prov = fuzzy_locations("A.ADM2")
    municipalities = df['municipality'].unique()
    provinces = df['province'].unique()
    fuzzy_map_mun = {x: locations_mun[process.extractOne(x, locations_mun.keys())[0]] for x in municipalities}
    fuzzy_map_prov = {x: locations_prov[process.extractOne(x, locations_prov.keys())[0]] for x in provinces}
    df['municipality'] = df['municipality'].map(fuzzy_map_mun)
    df['province'] = df['province'].map(fuzzy_map_prov)
    df['update_date'] = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()
    df['startDate'] = pd.to_datetime(df['dateOwner'].apply(lambda x: x[-1]['startDate']), format='%Y-%m-%d').apply(lambda x: x.isoformat() if pd.notnull(x) else np.nan)
    df['endDate'] = pd.to_datetime(df['dateOwner'].apply(lambda x: x[-1]['endDate']), format='%Y-%m-%d').apply(lambda x: x.isoformat() if pd.notnull(x) else np.nan)
    df['endDate'] = df['endDate'].astype('object')
    df['stateCancelled'] = np.where(df['endDate'] > df['startDate'], 'Accepted', None)
    df['nif_ab'] = df['endDate'].apply(lambda x: 'Alta' if pd.isna(x) else x)
    df['contractedPowerkW'] = df['contractedPowerkW'].apply(
        lambda x: '-'.join(str(item) for item in x) if isinstance(x, list) else x
    )
    df['lastMarketerDate'] = pd.to_datetime(df['lastMarketerDate'], format='%Y/%m/%d').apply(lambda x: x.isoformat() if pd.notnull(x) else np.nan)
    map_and_save({"supplies": df.to_dict(orient="records")},
                 "plugins/icaen/mapping.yaml", config)
    with driver.session() as session:

        session.run("""MATCH (n:bigg__Device) 
        WHERE n.bigg__source="DatadisSource"  
        SET n.source="DatadisSource"
        REMOVE n.bigg__source""")

        session.run("""MATCH (n:bigg__Device)-[:bigg__hasDeviceLocationInfo]->(l)
        WHERE n.source="DatadisSource" WITH l
        MATCH (l)-[rp:bigg__hasAddressProvince]->(p) 
        MATCH (l)-[rc:bigg__hasAddressCity]->(c) 
        SET rp.selected=true, rp.source="DATADIS", 
            rc.selected=true, rc.source="DATADIS"
        """)
        session.run(f"""MATCH (n:bigg__Device) 
        WHERE n.bigg__nif is not NULL 
        MATCH (s:DatadisSource) WHERE s.username=n.bigg__nif
        MERGE(n)-[:importedFromSource]->(s)
        REMOVE n.bigg__nif""")


def create_sensor_measurement(device_uri, sensor_uri, measurement_uri, sensor):
    rdf_tmp = rdflib.Graph()
    rdf_tmp.add((rdflib.URIRef(device_uri), rdflib.URIRef("http://bigg-project.eu/ontology#hasSensor"), rdflib.URIRef(sensor_uri)))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.RDF.type, rdflib.URIRef("http://bigg-project.eu/ontology#Sensor")))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.RDF.type, rdflib.URIRef("http://bigg-project.eu/ontology#TimeSeriesList")))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#timeSeriesEnd"), rdflib.Literal(sensor['timeSeriesEnd'])))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#timeSeriesFrequency"), rdflib.Literal(sensor['timeSeriesFrequency'])))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#timeSeriesIsCumulative"), rdflib.Literal(sensor['timeSeriesIsCumulative'])))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#timeSeriesIsOnChange"), rdflib.Literal(sensor['timeSeriesIsOnChange'])))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#timeSeriesIsRegular"), rdflib.Literal(sensor['timeSeriesIsRegular'])))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#timeSeriesStart"), rdflib.Literal(sensor['timeSeriesStart'])))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#timeSeriesTimeAggregationFunction"), rdflib.Literal(sensor['timeSeriesTimeAggregationFunction'])))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#hasMeasuredProperty"), rdflib.URIRef("http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity")))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#hasMeasurementUnit"), rdflib.URIRef("http://qudt.org/vocab/unit/KiloW-HR")))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#hasEstimationMethod"), rdflib.URIRef("http://bigg-project.eu/ontology#Profiled")))
    rdf_tmp.add((rdflib.URIRef(sensor_uri), rdflib.URIRef("http://bigg-project.eu/ontology#hasMeasurement"), rdflib.URIRef(measurement_uri)))
    rdf_tmp.add((rdflib.URIRef(measurement_uri), rdflib.RDF.type, rdflib.URIRef("http://bigg-project.eu/ontology#Measurement")))
    rdf_tmp.add((rdflib.URIRef(measurement_uri), rdflib.RDF.type, rdflib.URIRef("http://bigg-project.eu/ontology#TimeSeriesPoint")))
    rdf_tmp.add((rdflib.URIRef(measurement_uri), rdflib.URIRef("http://bigg-project.eu/ontology#hash"), rdflib.Literal(measurement_uri.split('#')[1])))
    return rdf_tmp


def harmonize_timeseries(data, freq, prop):
    """
    :param data:
    :param freq:
    :param prop:
    :return:
    """
    df = pd.DataFrame(data)
    df["start"] = df['timestamp']
    df["bucket"] = (df['start'] // settings.TS_BUCKETS) % settings.BUCKETS
    df['end'] = df.start + time_to_timedelta[freq].seconds
    df['value'] = df['consumptionKWh']
    df['isReal'] = df['obtainMethod'].apply(lambda x: True if x == "Real" else False)
    rdf = rdflib.Graph()
    df_final = pd.DataFrame()
    config = beelib.beeconfig.read_config("plugins/icaen/config_icaen.json")
    driver = neo4j.GraphDatabase().driver(**config['neo4j'])
    for device_id, data_group in df.groupby("cups"):
        data_group.set_index("datetime", inplace=True)
        data_group.sort_index(inplace=True)
        device_uri = f"https://icaen.cat#DEVICE-DatadisSource-{device_id}"
        sensor_uri = f"https://icaen.cat#SENSOR-datadis-{device_id}-{prop}-RAW-{freq}"
        with driver.session() as session:
            ts = session.run(f"""Match(n:bigg__Sensor) where n.uri="{sensor_uri}"
              RETURN n.bigg__timeSeriesStart as start, n.bigg__timeSeriesEnd as end""").single()
        if ts:
            start_neo = ts['start'].to_native()
            end_neo = ts['end'].to_native()
        else:
            start_neo = pytz.UTC.localize(datetime.datetime.max)
            end_neo = pytz.UTC.localize(datetime.datetime.min)
        dt_ini = data_group.iloc[0].name
        dt_end = data_group.iloc[-1].name
        dt_ini = datetime.datetime.fromisoformat(dt_ini)
        dt_end = datetime.datetime.fromisoformat(dt_end)
        dt_ini = min(dt_ini, start_neo)
        dt_end = max(dt_end, end_neo)
        measurement_id = hashlib.sha256(sensor_uri.encode("utf-8")).hexdigest()
        measurement_uri = f"https://icaen.cat#{measurement_id}"
        sensor = {"timeSeriesEnd": dt_end, "timeSeriesStart": dt_ini, "timeSeriesFrequency": freq,
                  "timeSeriesIsCumulative": False, "timeSeriesIsRegular": True, "timeSeriesIsOnChange": False,
                  "timeSeriesTimeAggregationFunction": "SUM"}
        rdf += create_sensor_measurement(device_uri, sensor_uri, measurement_uri, sensor)
        data_group["hash"] = measurement_id
        df_final = pd.concat([df_final, data_group[["hash", "bucket", "start", "end", "value", "isReal"]]])
    save_to_neo4j(rdf, config)
    table_name = config['hbase']['harmonized_data'].format(data_type=prop, freq=freq)
    beelib.beehbase.save_to_hbase(df_final.to_dict(orient="records"), table_name, config['hbase']['connection'],
                              [("v", ["value"]), ("info", ["end", "isReal"])],
                              ["bucket", "hash", "start"])
    config_kafka = beelib.beeconfig.read_config('config.json')
    producer = beelib.beekafka.create_kafka_producer(config_kafka['kafka'], encoding="JSON")
    df_final['freq'] = freq
    df_final['property'] = prop
    df_to_save = df_final.reset_index().apply(
        harmonize_for_influx, timestamp_key="start", end="end", value_key="value",
        hash_key="hash", is_real=True,
        axis=1)
    send_to_kafka(producer, 'sime.influx', df_to_save)


def end_process():
    config = beelib.beeconfig.read_config("plugins/icaen/config_icaen.json")
    driver = neo4j.GraphDatabase.driver(**config['neo4j'])
    with driver.session() as session:
        session.run("""Match(n:bigg__UtilityPointOfDelivery) 
        WHERE n.bigg__newSupply is NULL 
        SET n.bigg__newSupply=true""")
