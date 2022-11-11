package pl.softwareskill.course.kafka.consumers.task.safe;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Properties;

@Slf4j
class ConsumerFactory {

    public static KafkaConsumer<String, Text> createSafeConsumer() {
        var consumerProperites = createConsumerProperites();
        return new KafkaConsumer<String, Text>(
                consumerProperites,
                new StringDeserializer(),
                new KafkaJsonDeserializer<Text>(Text.class));

    }

    private static Properties createConsumerProperites() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}