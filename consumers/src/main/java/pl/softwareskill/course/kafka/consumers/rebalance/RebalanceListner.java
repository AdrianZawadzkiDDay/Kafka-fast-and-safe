package pl.softwareskill.course.kafka.consumers.rebalance;

import java.util.Collection;
import static lombok.AccessLevel.PRIVATE;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

@Slf4j
@FieldDefaults(level = PRIVATE)
class RebalanceListner implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Following Partitions Assigned ....");
        partitions.forEach(partition -> {
            log.info(partition.partition() + ",");
        });
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // this you can commit database transaction and call sync commit offset
        log.info("Following Partitions Revoked ....");
        partitions.forEach(partition -> {
            log.info(partition.partition() + ",");
        });
    }
}