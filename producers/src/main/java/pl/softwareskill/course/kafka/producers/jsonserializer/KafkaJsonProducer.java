package pl.softwareskill.course.kafka.producers.jsonserializer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaJsonProducer {

    public static void main(String[] args) {

        final var TOPIC_NAME = "softwareskill_safe_topic";
        final var BOOTSTRAP_SERVERS = "localhost:9092";

        // 1. create producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        // 2. create producer
        KafkaProducer<String, Person> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            // 3. crate a producer records
            var person = PersonFactory.createRandomPerson();
            ProducerRecord<String, Person> record = new ProducerRecord<>(TOPIC_NAME, createKey(person), person);

            // 4. send data
            producer.send(record);
        }

        // 5. clear connection
        producer.flush();
        producer.close();
    }

    private static String createKey(Person person) {
        return person.getFirstName() + "_" + person.getLastName();
    }
}
