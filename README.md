Kafka producers and consumers example:
Fast and Safe

commands:
##### docker-compose up -d

##### docker exec -it kafka_broker1_1 kafka-topics --zookeeper zookeeper:2181 --create --topic task_topic --partitions 2 --replication-factor 2 --config unclean.leader.election.enable=false --config min.insync.replicas=2
