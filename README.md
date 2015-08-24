# hbase-cleanup

## Execution
======
1. *mvn clean install* 
2. With parameters *java -jar target/hbase-cleanup.jar --ttl=12 --uri=vin-mongo.poc-vin.cloud.edmunds.com:27017 --cleanTrash=true*.
Default parameters values:
| Param         | Value         |
| ------------- |:-------------:|
| uri           | vin-mongo.poc-vin.cloud.edmunds.com:27017     |
| ttl           | 12                                            |
| cleanTrash    | false                                         |


