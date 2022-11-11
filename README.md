Kafka producers and consumers example:
Fast and Safe

commands:
##### docker-compose up -d

##### docker exec -it kafka_broker1_1 kafka-topics --zookeeper zookeeper:2181 --create --topic task_topic --partitions 2 --replication-factor 2 --config unclean.leader.election.enable=false --config min.insync.replicas=2


#### safe producer
![saveProducer](https://user-images.githubusercontent.com/45298383/201394230-1295cfad-977e-46ea-8f07-bad936f8b4cc.PNG)


#### safe consumer
![saveConsumer](https://user-images.githubusercontent.com/45298383/201394342-ccf61377-a218-4017-8fbc-01ddca47c691.PNG)
![fastProducer](https://user-images.githubusercontent.com/45298383/201396932-55a52c38-1e9d-4281-8d0e-a4b5dcc059fb.PNG)


#### fast producer
![fastProducer](https://user-images.githubusercontent.com/45298383/201397000-b3a0a34a-9721-43db-81a7-1088713b2465.PNG)

#### 1000 messges: safe-25s fast-3.85s
