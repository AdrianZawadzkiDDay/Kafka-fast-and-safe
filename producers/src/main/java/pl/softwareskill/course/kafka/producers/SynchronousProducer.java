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
public class SynchronousProducer {

    public static void main(String[] args) {

        final var TOPIC_NAME = "softwareskill_safe_topic";
        final var BOOTSTRAP_SERVERS = "localhost:9092";

        // 1. create producer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        // 3. crate a producer records
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "simple synchronous producer");

        // 4. synchronous send data
        try {

            Future<RecordMetadata> future = producer.send(record);
            // flush producer queue to spare queuing time
            producer.flush();
            // throw error when kafka is unreachable
            var recordMetadata = future.get(10, TimeUnit.SECONDS);
            log.info("RecordMetadata after synchronous send: topic={}, partition={}, offset={}",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Sending interrupted", e);
        } catch (Exception e) {
            log.error("Error while synchronous send", e);
        }

        // 5. clear connection
        producer.flush();
        producer.close();
    }
}
