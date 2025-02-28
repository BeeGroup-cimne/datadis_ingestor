## Data model
The following figure shows the different nodes that are created in the harmonization of this data source, as well as their relationships:

![image](/plugins/infraestructures/datadisicat.png)

## Static mapping
The following section shows all nodes created from this source. The origin of the data to map is ICat Datadis.

The following section shows all the nodes created from this source. 
We can see the origin and the harmonized field of each of the properties as well as the origin of the relationships that are created. 


### *Datadis*

### bee:Datadis (s4syst:System)
#### Node properties

| Origin  | Harmonization  |
|---------|----------------|
| Datadis | username       |
| Datadis | Password       |
| Datadis | authorized_nif |

#### Node relations: 
| Origin                        | Node 1      | Relation                 | Node 2            |
|-------------------------------|-------------|--------------------------|-------------------|
| Datadis.cups<br/>Datadis.prop | bee:Datadis | [s4syst:hasSubSystem] -> | bee:DatadisDevice |

### bee:DatadisDevice (saref:Device, s4syst:System)
#### Node relations: 
| Origin                        | Node 1            | Relation                   | Node 2            |
|-------------------------------|-------------------|----------------------------|-------------------|
| Datadis.cups<br/>Datadis.prop | bee:DatadisDevice | [saref:makesMeasurement]-> | saref:Measurement |
| "Energy.Active"               | bee:DatadisDevice | [saref:measuresProperty]-> | saref:Property    |

### saref:Measurement
#### Node properties
| Origin                              | Harmonization             |
|-------------------------------------|---------------------------|
| hash(Datadis.cups<br/>Datadis.prop) | bigg:hash                 |
| Datadis.freq                        | bigg:measurementFrequency |


#### Node relations: 
| Origin                                      | Node 1            | Relation                     | Node 2            |
|---------------------------------------------|-------------------|------------------------------|-------------------|
| "KiloW-HR"                                  | saref:Measurement | [saref:isMeasuredIn] ->      | qudt:Unit         |
| "Energy.Active"                             | saref:Measurement | [saref:relatesToProperty] -> | saref:Property    |
| Datadis.cups<br/>Datadis.freq<br/>patrimony | saref:Measurement | [owl:sameAs ]->              | saref:Measurement |

### *Electric devices*
### saref:Device (saref:Meter, s4syst:System)
#### Node properties
| Origin             | Harmonization |
|--------------------|---------------|
| "ImportedFromGrid" | foaf:name     |

#### Node relations: 
| Origin                                      | Node 1       | Relation                   | Node 2            |
|---------------------------------------------|--------------|----------------------------|-------------------|
| Datadis.cups<br/>Datadis.freq<br/>patrimony | saref:Device | [saref:makesMeasurement]-> | saref:Measurement |
| "Energy.Active"                             | saref:Device | [saref:measuresProperty]-> | saref__Property   |

### saref:Measurement
#### Node properties
| Origin                                            | Harmonization             |
|---------------------------------------------------|---------------------------|
| hash(Datadis.cups<br/>Datadis.freq<br/>patrimony) | bigg:hash                 |
| Datadis.freq                                      | bigg:measurementFrequency |

#### Node relations
| Origin                        | Node 1             | Relation                     | Node 2         |
|-------------------------------|--------------------|------------------------------|----------------|
| Datadis.cups<br/>Datadis.freq | saref:Measurement  | [saref:isMeasuredIn]->       | qudt:Unit      |
| "Energy.Active"               | saref:Measurement  | [saref:relatesToProperty ]-> | saref:Property |
