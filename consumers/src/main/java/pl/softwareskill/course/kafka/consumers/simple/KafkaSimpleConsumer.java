package pl.softwareskill.course.kafka.consumers.simple;

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
public class KafkaSimpleConsumer {

    static final String TOPIC_NAME = "softwareskill_topic";
    static final String BOOTSTRAP_SERVERS = "localhost:9091";
    static final String GROUP_ID = "group-1";

    public static void main(String[] args) {

        //1. create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 2. Create KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);

        // 3. Subscribe the topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        //4. Poll records
        try {
            while (true) { // bad practice while(true)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    //doing something with records
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
