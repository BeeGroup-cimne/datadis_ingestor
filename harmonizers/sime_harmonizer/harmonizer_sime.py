import datetime
from datetime import timedelta
import pytz
import pandas as pd
import rdflib
import hashlib
from thefuzz import process
import utils.hbase
from utils.transformation import map_and_save, save_to_neo4j, map_and_print
import settings
import neo4j

time_to_timedelta = {
    "PT1H": timedelta(hours=1),
    "PT15M": timedelta(minutes=15)
}


def fuzzy_locations(adm):
    g = rdflib.Graph()
    g.parse("harmonizers/sime_harmonizer/all-geonames-rdf-clean-ES.rdf", format="xml")
    res = g.query(f"""SELECT ?name ?b WHERE {{
            ?b gn:name ?name.
            ?b gn:featureCode <https://www.geonames.org/ontology#{adm}> .
        }}
    """)
    return {str(x[0]): str(x[1]).split("/")[-2] for x in list(res)}


def harmonize_supplies(data):
    df = pd.DataFrame(data)
    config = utils.config.read_config()
    driver = neo4j.GraphDatabase().driver(**config['neo4j'])
    with driver.session() as session:
        cups_ens = session.run("""MATCH (n:bigg__Device)<-[:bigg__isObservedByDevice]-(:bigg__BuildingSpace)
        <-[:bigg__hasSpace]-(b:bigg__Building) 
        RETURN distinct n.bigg__deviceName as ens, b.bigg__buildingIDFromOrganization as cups""").data()
        cups_ens = {v['ens']: v['cups'] for v in cups_ens}
    df['ens'] = df.cups.map(cups_ens)
    df['supply_name'] = df['cups'].str[:20]
    locations_mun = fuzzy_locations("A.ADM3")
    locations_prov = fuzzy_locations("A.ADM2")
    municipalities = df['municipality'].unique()
    provinces = df['province'].unique()
    fuzzy_map_mun = {x: locations_mun[process.extractOne(x, locations_mun.keys())[0]] for x in municipalities}
    fuzzy_map_prov = {x: locations_prov[process.extractOne(x, locations_prov.keys())[0]] for x in provinces}
    df['municipality'] = df['municipality'].map(fuzzy_map_mun)
    df['province'] = df['province'].map(fuzzy_map_prov)
    df['update_date'] = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()
    map_and_print({"supplies": df.to_dict(orient="records")},
                 "harmonizers/sime_harmonizer/mapping.yaml", config)
    map_and_save({"supplies": df.to_dict(orient="records")},
                 "harmonizers/sime_harmonizer/mapping.yaml", config)
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
    config = utils.config.read_config()
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
    utils.hbase.save_to_hbase(df_final.to_dict(orient="records"), table_name, config['hbase']['connection'],
                              [("v", ["value"]), ("info", ["end", "isReal"])],
                              ["bucket", "hash", "start"])


def end_process():
    config = utils.config.read_config()
    driver = neo4j.GraphDatabase.driver(**config['neo4j'])
    with driver.session() as session:
        session.run("""Match(n:bigg__UtilityPointOfDelivery) 
        WHERE n.bigg__newSupply is NULL 
        SET n.bigg__newSupply=true""")

