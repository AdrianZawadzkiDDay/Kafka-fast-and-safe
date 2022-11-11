package pl.softwareskill.course.kafka.consumers.task.fast;

import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.softwareskill.course.kafka.consumers.jsondeserializer.Person;
import pl.softwareskill.course.kafka.consumers.task.json.KafkaJsonDeserializer;
import pl.softwareskill.course.kafka.consumers.task.json.Text;

import java.util.Properties;

import static lombok.AccessLevel.PRIVATE;

@FieldDefaults(level = PRIVATE, makeFinal = true)
public class KafkaFastConsumerRunner {

    static final String TOPIC_NAME = "task_topic";
    static final String GROUP_ID = "group-1";
    static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        KafkaConsumer<String, Text> consumer =  new KafkaConsumer<String, Text>(
                createConsumerProperites(),
                new StringDeserializer(),
                new KafkaJsonDeserializer<Text>(Text.class));


        Thread thread = new Thread(new KafkaFastConsumerRunnable(consumer, TOPIC_NAME));
        thread.start();
    }

    private static Properties createConsumerProperites() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //default
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); //default
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 2048);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        return props;
    }
}