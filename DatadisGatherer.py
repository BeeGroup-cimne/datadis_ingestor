#!/usr/bin/env python3
import sys
import pytz
import beelib
import pickle
import pymongo
import settings
import pandas as pd
from datetime import datetime
from functools import partial
from beedis import ENDPOINTS, Datadis
from dateutil.relativedelta import relativedelta
import logging
import plugins

logger = logging.getLogger()

class DatadisGatherer:
    TZ = pytz.timezone("Europe/Madrid")

    def __init__(self, policy='last'):
        self.config = beelib.beeconfig.read_config("config.json")
        self.source = "datadis"
        self.policy = policy

    @staticmethod
    def get_values_period(init_time, end_time, freq):
        init_time = DatadisGatherer.TZ.localize(init_time)
        end_time = DatadisGatherer.TZ.localize(end_time)
        return len(pd.date_range(init_time, end_time, freq=freq))

    @staticmethod
    def parse_max_power_chunk(max_power):
        if len(max_power) <= 0:
            return list()
        try:
            df_consumption = pd.DataFrame(max_power)
            # Cast datetime64[ns] to timestamp (int64)
            df_consumption.set_index('datetime', inplace=True)
            df_consumption.sort_index(inplace=True)
            for c in [x for x in df_consumption.columns if x.startswith("datetime_")]:
                df_consumption[c] = df_consumption[c].astype('int64') // 10 ** 9

            df_consumption['timestamp'] = df_consumption.index.astype('int64') // 10 ** 9
            m_power = df_consumption.to_dict(orient='records')
            for x in m_power:
                dts = [dt for dt in x.keys() if dt.startswith("datetime_")]
                x['cups'] = x.pop('cups_period_')
                for c in dts:
                    if x[c] == -9223372037:  # NaT representation in timestamp
                        period = c.split("_")[2]
                        x.pop(f"datetime_period_{period}")
                        x.pop(f"maxPower_period_{period}")

            return m_power
        except Exception as e:
            return list()

    @staticmethod
    def parse_consumption_chunk(consumption):
        if len(consumption) <= 0:
            return list()
        try:
            df_consumption = pd.DataFrame(consumption)
            df_consumption.index = df_consumption['datetime']
            df_consumption.sort_index(inplace=True)
            # Cast datetime64[ns] to timestamp (int64)
            df_consumption['timestamp'] = df_consumption['datetime'].astype('int64') // 10 ** 9
            return df_consumption.to_dict(orient='records')
        except Exception as e:
            return list()

    data_types_dict = {
        "EnergyConsumptionGridElectricity_PT1H": {
            "mongo_collection": "data_1h",
            "type_data": "timeseries",
            "freq_rec": relativedelta(day=31, hour=23, minute=59, second=59),
            "measurement_type": "0",
            "endpoint": ENDPOINTS.GET_CONSUMPTION,
            "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type",
                       "authorized_nif"],
            "elements_in_period": partial(get_values_period, freq="h"),
            "parser": parse_consumption_chunk,
        },
        "EnergyConsumptionGridElectricity_PT15M": {
            "mongo_collection": "data_15m",
            "type_data": "timeseries",
            "freq_rec": relativedelta(day=31, hour=23, minute=59, second=59),
            "measurement_type": "1",
            "endpoint": ENDPOINTS.GET_CONSUMPTION,
            "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type",
                       "authorized_nif"],
            "elements_in_period": partial(get_values_period, freq="15t"),
            "parser": parse_consumption_chunk,
        },
        "Power_P1M": {
            "mongo_collection": "max_power",
            "type_data": "timeseries",
            "freq_rec": relativedelta(months=5) + relativedelta(day=31, hour=23, minute=59, second=59),
            "endpoint": ENDPOINTS.GET_MAX_POWER,
            "params": ["cups", "distributor_code", "start_date", "end_date", "authorized_nif"],
            "elements_in_period": partial(get_values_period, freq="1M"),
            "parser": parse_max_power_chunk,
        }
    }

    def save_datadis_data(self, topic, collection_type, key, data, row_keys, dblist, tables, **kwargs):
        plug = [x for x in plugins.get_plugins() if x.get_source() == dblist[0]][0]

        if collection_type == 'timeseries':
            prop = kwargs['property'] if 'property' in kwargs else None
            freq = kwargs['freq'] if 'freq' in kwargs else None
            tables = [s.format(freq=freq, prop=prop) for s in tables]
            topic = plug.get_topic()
            row_keys = [list(item) for item in row_keys]

            # Get raw data prepared to upload to HBase using the proper plugin to set up the timeseries
            data = pd.DataFrame(data)
            data['freq'] = freq
            data['prop'] = prop
            data = plug.prepare_raw_data(data)
            data = data.to_dict(orient='records')

        else:
            tables = ['']

        for entry in data:
            if 'datetime' in entry and isinstance(entry['datetime'], pd.Timestamp):
                entry['datetime'] = entry['datetime'].isoformat()

        producer = beelib.beekafka.create_kafka_producer(self.config['kafka'], encoding="JSON")
        for db in dblist:
            kwargs.update({"dblist": [db]})
            kwargs.update({'collection_type': collection_type})

            # Format the row_keys correctly

            logger.debug(f"Sending timeseries to Kafka", extra={"phase": "GATHER", "tables": tables})

            beelib.beekafka.send_to_kafka(producer, topic, key, data,
                                          tables=tables, row_keys=row_keys, kwargs=kwargs)
        producer.flush()
        producer.close()

    def parse_arguments(self, row, type_params, date_ini, date_end):
        arguments = {}
        for a in type_params['params']:
            if a == "cups":
                arguments["cups"] = row["cups"]
            elif a == "distributor_code":
                arguments["distributor_code"] = row['distributorCode']
            elif a == "start_date":
                arguments["start_date"] = date_ini
            elif a == "end_date":
                arguments["end_date"] = date_end
            elif a == "measurement_type":
                arguments["measurement_type"] = type_params['measurement_type']
            elif a == "point_type":
                arguments["point_type"] = str(row["pointType"])
            elif a == "authorized_nif":
                arguments["authorized_nif"] = row["authorized_nif"]
        return arguments

    def get_devices(self, user, password, authorized_nif, db_list, tables, row_keys, red):

        try:
            Datadis.connection(username=user, password=password, timeout=1000)
            logger.info(f"Login success", extra={'user': user, "phase": "GATHER"})
            supplies = Datadis.datadis_query(ENDPOINTS.GET_SUPPLIES, authorized_nif=authorized_nif)
            if not supplies:
                supplies = []
                logger.error(f"User empty", extra={"phase": "GATHER", "user": user,
                                                   "authorized_nif": authorized_nif, "db_list": db_list})
        except PermissionError as e:
            logger.error(f"Login failed", extra={"phase": "GATHER", "user": user, "exception": str(e),
                                                 "authorized_nif": authorized_nif, "db_list": db_list})
            return

        except Exception as e:
            logger.error(f"Request error", extra={"phase": "GATHER", "user": user, "exception": str(e),
                                                  "authorized_nif": authorized_nif, "db_list": db_list})
            return

        logger.debug(f"{user} done", extra={"phase": "GATHER"})
        # print(f"{user} end", file=sys.stderr)
        for i in range(0, len(supplies), 10):
            supply_dict = {'user': user, 'password': password, 'db_list': db_list, 'supplies': supplies[i:i + 10],
                           'authorized_nif': authorized_nif, 'tables': tables, 'row_keys': row_keys}
            logger.debug("Gathered devices", extra={"user": supply_dict['user'], 'supplies': supplies[i:i + 10],
                                                    'phase': "GATHER"})
            red.lpush("datadis.devices", pickle.dumps(supply_dict))

    def download_chunk(self, supply, type_params, status):
        try:
            date_ini_req = status['date_ini_block'].date()
            date_end_req = status['date_end_block'].date()
            logger.debug(f"Downloading {supply['cups']} from {date_ini_req} to {date_end_req}",
                         extra={"phase": "GATHER"})
            kwargs = self.parse_arguments(supply, type_params, date_ini_req, date_end_req)
            # kwargs.update('authorized_nif': supply['authorized_nif'], **kwargs)
            consumption = Datadis.datadis_query(type_params['endpoint'], **kwargs)
            if not consumption:
                raise Exception(f"No data could be found")
            return consumption
        except Exception as e:
            logger.error(f"Error downloading data",
                         extra={"phase": "GATHER", "exception": str(e), "cups": supply['cups'],
                                'date_ini': status['date_ini_block'],
                                "date_end": status['date_end_block']})
            return list()

    def download_device(self, supply, device, datadis_devices, dblist, tables, row_keys):
        downloaded_elems = set()
        for data_type, type_params in self.data_types_dict.items():
            m_property, freq = data_type.split("_")
            if dblist == ['icat']:
                m_property = 'energy-active'
            if type_params['type_data'] == "timeseries":
                if self.policy == "last":
                    try:
                        # get last chunk
                        status = list(device[type_params["mongo_collection"]].values())[-1]
                    except IndexError as e:
                        continue
                    # check if chunk is in current time
                    if not (status['date_ini_block'] <= datetime.today()
                            <= status['date_end_block']):
                        continue
                    data = self.download_chunk(supply, type_params, status)
                    data_df = type_params['parser'](data)
                    if len(data_df) > 0:
                        self.save_datadis_data(settings.TOPIC_TS, "timeseries", supply['cups'],
                                               data_df, row_keys, dblist, tables, property=m_property, freq=freq)
                        status['date_min'] = pd.to_datetime(data_df[0]['timestamp'], unit="s").tz_localize(pytz.UTC)
                        status['date_max'] = pd.to_datetime(data_df[-1]['timestamp'], unit="s"). \
                            tz_localize(pytz.UTC)
                        # Can't add lists to a python Set
                        downloaded_elems.add((m_property, freq))
                    # store status info
                    status['values'] = len(data_df)
                if self.policy == "repair":
                    # get all incomplete chunks
                    status_list = [x for x in device[type_params["mongo_collection"]].values()
                                   if x['values'] < x['total'] and x['retries'] > 0 and
                                   x['date_ini_block'] > datetime.today() - relativedelta(years=1, months=11)]
                    for status in status_list:
                        data = self.download_chunk(supply, type_params, status)
                        data_df = type_params['parser'](data)
                        if len(data_df) > 0:
                            self.save_datadis_data(settings.TOPIC_TS, "timeseries", supply['cups'],
                                                   data_df, row_keys, dblist, tables, property=m_property,
                                                   freq=freq)
                            status['date_min'] = pd.to_datetime(data_df[0]['timestamp'], unit="s").tz_localize(
                                pytz.UTC)
                            status['date_max'] = pd.to_datetime(data_df[-1]['timestamp'], unit="s"). \
                                tz_localize(pytz.UTC)
                            downloaded_elems.add((m_property, freq))
                        else:
                            status['retries'] -= 1
                        status['values'] = len(data_df)
        # store status info
        datadis_devices.replace_one({"_id": device['_id']}, device, upsert=True)
        return list(downloaded_elems)

    def get_device(self, supply, datadis_devices):
        """
        get or create the data chunks from mon go to update the data
        :param supply: the supply to get chunks
        :param datadis_devices: the mongo database where logs are stored
        :return: the device chunks created
        """
        try:
            device = datadis_devices.find_one({"_id": supply['cups']})
        except IndexError:
            device = None
        if not device:
            # if there is no log document create a new one
            device = {
                "_id": supply['cups']
            }

        date_ini = datetime.today().replace(hour=0, day=1, minute=0, second=0, microsecond=0) - relativedelta(
                months=23)
        now = datetime.today().date() + relativedelta(day=31, hour=23, minute=59, second=59)
        try:
            date_end = datetime.strptime(supply['validDateTo'][:-2], '%Y/%m') + \
                       relativedelta(day=31, hour=23, minute=59, second=59)
            if date_end <= now:
                date_end = date_end
            else:
                raise Exception()
        except Exception as e:
            date_end = now
        for t, type_params in [(x, y) for x, y in self.data_types_dict.items() if y["type_data"] == "timeseries"]:
            if type_params["mongo_collection"] not in device:
                device[type_params["mongo_collection"]] = {}
            loop_date_ini = date_ini
            while loop_date_ini < date_end:
                current_ini = loop_date_ini
                current_end = current_ini + type_params['freq_rec']
                k = "~".join([current_ini.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")])
                date_ini_block = current_ini
                date_end_block = current_end
                if k not in device[type_params["mongo_collection"]]:
                    device[type_params["mongo_collection"]].update({
                        k: {
                            "date_ini_block": date_ini_block,
                            "date_end_block": date_end_block,
                            "values": 0,
                            "total": type_params['elements_in_period'](date_ini_block, date_end_block),
                            "retries": 6,
                        }
                    })
                loop_date_ini = current_end + relativedelta(seconds=1)
        return device

    def get_data(self, user, password, nif, dblist, supplies, tables, row_keys):
        try:
            Datadis.connection(username=user, password=password, timeout=1000)
            logger.info(f"Login success for data", extra={'user': user, "phase": "GATHER"})
            for supply in supplies:
                try:
                    supply['nif'] = user
                    supply['authorized_nif'] = nif
                    mongo = pymongo.MongoClient(
                        f"mongodb://{self.config['mongo']['user']}:{self.config['mongo']['password']}@"
                        f"{self.config['mongo']['host']}:{self.config['mongo']['port']}/{self.config['mongo']['database']}")
                    datadis_devices = mongo[self.config['mongo']['database']][self.config['mongo']['collection']]
                    device = self.get_device(supply, datadis_devices)
                    downloaded_elems = self.download_device(supply, device, datadis_devices, dblist, tables, row_keys)
                    supply['measurements'] = downloaded_elems
                    self.save_datadis_data(settings.TOPIC_STATIC, "supplies", supply['cups'], supply, row_keys,
                                           dblist, tables)
                    contracts_dd = Datadis.datadis_query(ENDPOINTS.GET_CONTRACT, cups=supply['cups'],
                                                         distributor_code=supply['distributorCode'], authorized_nif=nif)
                    contract = pd.DataFrame.from_records(contracts_dd).set_index('startDate') \
                        .reset_index().iloc[-1].to_dict()
                    logger.info(f"Contracts gathered", extra={'user': user, "phase": "GATHER"})
                    self.save_datadis_data(settings.TOPIC_STATIC, "contracts", contract['cups'], contract,
                                           row_keys, dblist, tables)
                except Exception as e:
                    logger.error(f"Error", extra={"phase": "GATHER", "user": user, "exception": str(e),
                                                  "authorized_nif": nif, ", db_list": dblist})

        except Exception as e:
            logger.error(f"Error", extra={"phase": "GATHER", "user": user, "exception": str(e),
                                          "authorized_nif": nif, ", db_list": dblist})
