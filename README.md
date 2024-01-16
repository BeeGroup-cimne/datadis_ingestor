# Datadis description
This module will import data from Datadis and harmonize it from different sources.

The following image depicts the workflow of the script.

<img src="datadis_schema.png">

## Gather data

It consists on a set of plugins to obtain the users and passwords, then a mapreduce is launched to gather all the data.

###### Mapreduce description:
```
    > STEP 1:
    MAP
      input -> user,password,source_db
      output-> id,user,password,source_db
    REDUCE
      input -> id,user,password,source_db
        - get_supplies as CUPS
      output-> id CUPS,user,password,source_db
    
    > STEP 2:
    MAP
      input -> id CUPS,user,password,source_db
      output-> id CUPS,user,password,source_db
    REDUCE
      input -> id CUPS,user,password,source_db
        - get_contracts
        - send CUPS and CONTRACTS to harmonizer
        - get_consumption as CONS
        - send CONS to harmonizer
      output -> null
```
###### Howto:
To run the gathering application, execute the python script with the following parameters:

```bash
python3 launcher.py
```

## Harmonize data

To harmonize the gathered data a faust application have been developed

###### Howto:
run the raw data store:
```bash
python3 -m store_raw worker -l info
```
run the harmonizers by source

```bash
# icaen
python3 -m plugins.icaen worker -l info
```

## Deploy in production

To deploy in production, we need to dockerize the project using the included Dockerfile
```bash
docker buildx build --push -t docker.tech.beegroup-cimne.com/jobs/datadis . --provenance false
```
then, using the kubernetes yamls included in kubeconfigs, we can deploy the project to kubernetes

create the required secrets:
```bash
#basic secret
kubectl create secret generic datadis-harmonize-secret --from-file=config.json=config.json -n datadis
#icaen secret
kubectl create secret generic datadis-icaen-secret --from-file=config_icaen.json=plugins/icaen/config_icaen.json -n datadis
#icat secret
kubectl create secret generic datadis-icat-secret --from-file=config_infra.json=plugins/infraestructures/config_infra.json -n datadis
```
