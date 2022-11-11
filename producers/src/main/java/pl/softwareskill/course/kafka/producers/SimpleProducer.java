package pl.softwareskill.course.kafka.producers;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

    public static void main(String[] args) {

        final var TOPIC_NAME = "softwareskill_topic";
        final var BOOTSTRAP_SERVERS = "localhost:9091";

        // 1. create producer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        // 3. crate a producer records
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "Docker simple fire and forget producer");

        // 4. send data
        producer.send(record);

        // 5. clear connection
        producer.flush();
        producer.close();
    }
}
