# Run Kafka Spark Streaming inside Docker

1. Start Kafka Cluster with docker-compose
2. Note the network name
3. Publish some messages
4. Build this project
5. Start a spark container 
```bash
docker run --rm -it \
--name <some_name> \
--network <network from step2> \
<spark image> /bin/bash
```
6. copy the fat jar to docker container
```bash
docker cp \
target/kafkaTest-1.0-SNAPSHOT-jar-with-dependencies.jar \
<spark_container>:/
```
7. Run `spark-submit`
```bash
/usr/local/spark/bin/spark-submit \
--class org.telia.example.SparkKafkaConsumer \
kafkaTest-1.0-SNAPSHOT-jar-with-dependencies.jar
```