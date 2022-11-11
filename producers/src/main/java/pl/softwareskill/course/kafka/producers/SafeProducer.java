package pl.softwareskill.course.kafka.producers;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SafeProducer {

    public static void main(String[] args) {

        final var TOPIC_NAME = "softwareskill_safe_topic";
        final var BOOTSTRAP_SERVERS = "127.0.0.1:9092";

        // 1. create producer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // performence settings
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        //safe settings
        kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //acks is set to all after enable idempotence

        // 2. create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        Instant start = Instant.now();

        for (int i = 0; i < 1000; i++) {

            try {
                // 3. crate a producer records
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "safe producer " + i);

                // 4. send data synchrously
                Future<RecordMetadata> future = producer.send(record);
                // flush producer queue to spare queuing time
                producer.flush();
                // throw error when kafka is unreachable
                var recordMetadata = future.get(10, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Sending interrupted", e);
            } catch (LeaderNotAvailableException e) {
                log.error("Leader not available", e);
            } catch (NotEnoughReplicasException e) {
                log.error("Not enought replicas", e);
            } catch (Exception e) {
                log.error("Error while synchronous send", e);
            }
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toSeconds();
        log.info("Time Elapsed: " + timeElapsed + " seconds");

        // 5. clear connection
        producer.flush();
        producer.close();
    }
}
