package pl.softwareskill.course.kafka.consumers.task.safe;

import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static lombok.AccessLevel.PRIVATE;

@Slf4j
@FieldDefaults(level = PRIVATE)
class RebalanceListner implements ConsumerRebalanceListener {
    KafkaConsumer consumer;
    OffsetRepository offsetRepository;
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new ConcurrentHashMap<>();

    RebalanceListner(KafkaConsumer kafkaConsumer, OffsetRepository offsetRepository) {
        this.consumer = kafkaConsumer;
        this.offsetRepository = offsetRepository;
    }

    public void addOffset(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Following Partitions Assigned ....");
        partitions.forEach(partition -> {
            log.info(partition.partition() + ",");
        });

        //start reading from stored offset
        partitions.forEach(partition -> {
            var offset = offsetRepository.getOffset(partition);
            if (offset.isPresent()) {
                consumer.seek(partition, offset.get().longValue());
            }
        });
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // this you can commit database transaction and call sync commit offset
        log.info("Following Partitions Revoked ....");
        partitions.forEach(partition -> {
            log.info(partition.partition() + ",");
        });

        log.info("Following Partitions commited ....");
        currentOffsets.keySet().forEach(topicPartition -> {
            log.info("" + topicPartition.partition());
        });

        log.info("Call commit sync");
        consumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }
}