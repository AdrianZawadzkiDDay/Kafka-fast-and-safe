package pl.softwareskill.course.kafka.consumers.task.safe;

import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.softwareskill.course.kafka.consumers.task.json.Text;

import static lombok.AccessLevel.PRIVATE;

@Slf4j
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class KafkaSafeConsumerRunner {

    static final String TOPIC_NAME = "task_topic";

    public static void main(String[] args) {
        OffsetRepository offsetRepository = new OffsetRepository();
        EventRepository eventRepository = new EventRepository();

        KafkaConsumer<String, Text> consumer = ConsumerFactory.createSafeConsumer();

        Thread thread = new Thread(new KafkaSafeConsumerRunnable(consumer, offsetRepository, eventRepository, TOPIC_NAME));
        thread.start();
    }
}