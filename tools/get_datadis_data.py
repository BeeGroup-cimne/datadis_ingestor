#!/usr/bin/env python3
import datetime
import calendar
from beedis import ENDPOINTS, Datadis

user = ''
password = ''
nif = ''

Datadis.connection(username=user, password=password, timeout=1000)
supplies = Datadis.datadis_query(ENDPOINTS.GET_SUPPLIES, authorized_nif=nif)
s = supplies[0]

contracts_dd = Datadis.datadis_query(ENDPOINTS.GET_CONTRACT, cups=s['cups'],
                                                         distributor_code=s['distributorCode'], authorized_nif=nif)
s['start_date'] = datetime.date(2024, 5, 1)
last_day = calendar.monthrange(s['start_date'].year, s['start_date'].month)[1]
s['end_date'] = datetime.date(s['start_date'].year, s['start_date'].month, last_day)
s["distributor_code"] = s["distributorCode"]
s["point_type"] = s["pointType"]
s["measurement_type"] = '0'

del s["address"]
del s["postalCode"]
del s["province"]
del s["municipality"]
del s["distributor"]
del s["validDateFrom"]
del s["validDateTo"]
del s["distributorCode"]
del s["pointType"]

consumption = Datadis.datadis_query(ENDPOINTS.GET_CONSUMPTION, **s)
