package pl.softwareskill.course.kafka.producers;

import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class KafkaAsynchronousProducer {

    public static void main(String[] args) {

        final var TOPIC_NAME = "softwareskill_topic";
        final var BOOTSTRAP_SERVERS = "localhost:9091";

        //create producer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        try {
            for (int i = 1; i <= 5; i++) {
                // crate a producer records
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "send asychronous message with nr. " + i);
                // send message to Kafka Cluster
                producer.send(record, new ProducerCallback());
            }
        } catch (Exception e) {
            log.error("Error while send asynchronous messages", e);
        } finally {
            producer.close();
        }
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (Objects.nonNull(e)) {
                log.error("There was error while write message to Kafka Cluster");
            } else {
                log.info("Succesfully write message with topic = {}, partition = {} and offset = {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        }
    }
}
