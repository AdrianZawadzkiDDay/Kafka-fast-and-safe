package pl.softwareskill.course.kafka.consumers.commit;

import java.util.Properties;
import static lombok.AccessLevel.PRIVATE;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

@FieldDefaults(level = PRIVATE, makeFinal = true)
public class KafkaSimpleConsumerRunner {

    static final String TOPIC_NAME = "softwareskill_topic";
    static final String GROUP_ID = "group-1";
    static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        var consumerProperites = createConsumerProperites();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperites);

        Thread thread = new Thread(new KafkaAsynchronousConsumerRunnable(consumer, TOPIC_NAME));
        thread.start();
    }

    private static Properties createConsumerProperites() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
}