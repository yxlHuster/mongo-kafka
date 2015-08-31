# mongo-kafka
send mongo oplog stream to kafka<br />

## Descriptions

  1. Mainly used for reading Mongo oplog data and send to kafka 
  2. Mongo should work under shards with replication and no-sharded mode
  3. Test cases are not running through currently
  
## References

  1. [mongo-storm](https://github.com/christkv/mongo-storm)  
  2. [flume](https://github.com/apache/flume)<br />
  
## Usage

java -jar mongo-kafka.jar -c mongo-kafka.properties


