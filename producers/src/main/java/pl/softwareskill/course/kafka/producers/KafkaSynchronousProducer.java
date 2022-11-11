package pl.softwareskill.course.kafka.producers;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class KafkaSynchronousProducer {

    public static void main(String[] args) {

        final var TOPIC_NAME = "softwareskill_topic";
        final var BOOTSTRAP_SERVERS = "localhost:9091";

        //create producer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        try {
            for (int i = 1; i <= 5; i++) {
                // crate a producer record
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "send sychronous message with nr. " + i);
                // send message to Kafka Cluster
                Future<RecordMetadata> future = producer.send(record);
                // flush producer queue to spare queuing time
                producer.flush();
                // throw error when kafka is unreachable
                RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
                log.info("Succesfully write message with topic = {}, partition = {} and offset = {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Sending interrupted", e);
        } catch (Exception e) {
            log.error("Error while send synchronous messages", e);
        } finally {
            producer.close();
        }
    }
}
