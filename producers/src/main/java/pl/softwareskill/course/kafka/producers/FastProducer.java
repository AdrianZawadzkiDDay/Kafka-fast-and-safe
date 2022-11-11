package pl.softwareskill.course.kafka.producers;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class FastProducer {

    public static void main(String[] args) {

        final var TOPIC_NAME = "softwareskill_safe_topic";
        final var BOOTSTRAP_SERVERS = "localhost:9092";

        // 1. create producer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // performence settings
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "0");

        // 2. create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        Instant start = Instant.now();

        for (int i = 0; i < 10000000; i++) {
            // 3. crate a producer records
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "simple fire and forget producer " + i);

            // 4. send data by fire and forget method
            producer.send(record);
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        log.info("Time Elapsed in ms: " + timeElapsed);
        log.info("Send " + 10000000 / timeElapsed * 1000 + " on second");

        // 5. clear connection
        producer.flush();
        producer.close();
    }
}
