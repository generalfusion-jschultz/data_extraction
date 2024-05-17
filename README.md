# data_exctraction
### Backs MQTT subscription data on csv files

This module subscribes to a given MQTT topics list and periodically backs data up to a csv file. At the end of the day it processes the csv file and outputs a formatted csv file of all the data.


```
client = DataExtractionClient()
client.connect()
client.run_forever()
```
