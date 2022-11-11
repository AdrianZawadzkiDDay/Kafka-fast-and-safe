package pl.softwareskill.course.kafka.consumers.commit;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import static lombok.AccessLevel.PRIVATE;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class KafkaCommitSpecyficConsumerRunnable implements Runnable {
    AtomicBoolean closed = new AtomicBoolean(false);
    KafkaConsumer consumer;
    String topicName;

    public KafkaCommitSpecyficConsumerRunnable(KafkaConsumer consumer, String topicName) {
        this.consumer = consumer;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        int count = 0;

        try {
            consumer.subscribe(Arrays.asList(topicName));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    // doing something with received messages

                    final var topicPartition = new TopicPartition(record.topic(), record.partition());
                    final var offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, "no metadata");
                    currentOffsets.put(topicPartition, offsetAndMetadata);

                    if (count % 10 == 0) {
                        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                            if (Objects.nonNull(exception)) {
                                log.error("Commit failed for offsets {}", offsets, exception);
                            } else {
                                log.info("Async commited successful");
                            }
                        });
                    }
                    count++;
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                log.error("Synchronous commit offset failed", e);
            } finally {
                consumer.close();
            }
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}