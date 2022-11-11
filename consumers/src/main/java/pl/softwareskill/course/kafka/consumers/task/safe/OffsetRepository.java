package pl.softwareskill.course.kafka.consumers.task.safe;

import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import pl.softwareskill.course.kafka.consumers.task.safe.Text;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static lombok.AccessLevel.PRIVATE;

/*
 * database mock
 */
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class OffsetRepository {

    Map<String, Long> storedOffsets = new ConcurrentHashMap<>();

    public Optional<Long> getOffset(TopicPartition partition) {
        var key = partition.topic() + "_" + partition.partition();
        return Optional.ofNullable(storedOffsets.get(key));
    }

    public void storeOffset(ConsumerRecord<String, Text> consumerRecord) {
        var key = consumerRecord.topic() + "_" + consumerRecord.partition();
        storedOffsets.put(key, Long.valueOf(consumerRecord.offset()));
    }
}
