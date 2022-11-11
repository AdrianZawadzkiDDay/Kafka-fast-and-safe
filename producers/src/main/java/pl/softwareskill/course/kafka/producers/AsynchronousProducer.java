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
public class AsynchronousProducer {

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
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "simple asynchronous");

        // 4. asynchronous send data
        producer.send(record, new ProducerCallback());

        // 5. clear connection
        producer.flush();
        producer.close();
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (Objects.nonNull(e)) {
                log.error("Error while asynchronous send", e);
            } else {
                log.info("RecordMetadata after asynchronous send: topic={}, partition={}, offset={}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            }
        }
    }
}
