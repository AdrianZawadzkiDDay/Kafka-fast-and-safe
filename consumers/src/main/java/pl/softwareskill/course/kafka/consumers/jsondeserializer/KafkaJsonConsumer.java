package pl.softwareskill.course.kafka.consumers.jsondeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class KafkaJsonConsumer {

    static final String TOPIC_NAME = "persons";
    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String GROUP_ID = "group-1";

    public static void main(String[] args) {

        //1. create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // 2. Create KafkaConsumer
        KafkaConsumer<String, Person> consumer = new KafkaConsumer<String, Person>(
                consumerProps,
                new StringDeserializer(),
                new KafkaJsonDeserializer<Person>(Person.class));

        // 3. Subscribe the topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        //4. Poll records
        try {
            while (true) { // bad practice while(true)
                ConsumerRecords<String, Person> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, Person> record : records) {
                    final var person = record.value();
                    log.info("firstName = {}, lastName = {}, birthday = {}",
                            person.getFirstName(), person.getLastName(), person.getBirthday());
                }
            }
        } catch (Exception e) {
            log.error("Exception while poll messages", e);
        } finally {
            // 5. Close the consumer
            consumer.close();
        }
    }
}
