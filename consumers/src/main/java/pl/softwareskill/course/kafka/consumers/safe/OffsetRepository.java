package pl.softwareskill.course.kafka.consumers.safe;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import static lombok.AccessLevel.PRIVATE;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/*
 * database mock
 */
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class OffsetRepository {

    /**
     * Nie robić tak na produkcji!
     * Rozwiązanie nie wspiera wielowątkowości
     * Więcej - https://stackoverflow.com/questions/7266042/ring-buffer-in-java/7266175#7266175
     * Dodatkowo trzeba zaopiekować temat czyszczenia mapy - nie potrzebujemy tych informacji w nieskończoność!
     */
    Map<String, Long> storedOffsets = new ConcurrentHashMap<>();

    public Optional<Long> getOffset(TopicPartition partition) {
        var key = partition.topic() + "_" + partition.partition();
        return Optional.ofNullable(storedOffsets.get(key));
    }

    public void storeOffset(ConsumerRecord<String, Person> consumerRecord) {
        var key = consumerRecord.topic() + "_" + consumerRecord.partition();
        storedOffsets.put(key, Long.valueOf(consumerRecord.offset()));
    }
}
