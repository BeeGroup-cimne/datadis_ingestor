# COSMIC PLUGIN
This is the plugin of Datadis for COSMIC. 

It requires a config_file with the following fields:
```json
  {
      "neo4j": {
        "uri": "db_uri",
        "auth": ["username", "password"],
        "encrypted": true,
        "trust": "TRUST_ALL_CERTIFICATES"
      },
      "secret_password": "",
      "hbase": {
        "row_keys": [""],
        "tables" : ""
      }
  }
```

# Deployment
Add the file to the datadis-plugins-secret secret
