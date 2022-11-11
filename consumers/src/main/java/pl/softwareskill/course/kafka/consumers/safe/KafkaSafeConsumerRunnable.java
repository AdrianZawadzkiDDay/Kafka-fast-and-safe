package pl.softwareskill.course.kafka.consumers.safe;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import static lombok.AccessLevel.PRIVATE;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
@FieldDefaults(level = PRIVATE, makeFinal = true)
class KafkaSafeConsumerRunnable implements Runnable {
    AtomicBoolean closed = new AtomicBoolean(false);
    KafkaConsumer consumer;
    OffsetRepository offsetRepository;
    EventRepository eventRepository;
    String topicName;

    public KafkaSafeConsumerRunnable(KafkaConsumer consumer, OffsetRepository offsetRepository, EventRepository eventRepository, String topicName) {
        this.consumer = consumer;
        this.offsetRepository = offsetRepository;
        this.eventRepository = eventRepository;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        RebalanceListner rebalanceListner = new RebalanceListner(consumer, offsetRepository);

        try {
            consumer.subscribe(Arrays.asList(topicName), rebalanceListner);
            consumer.poll(Duration.ofMillis(10));
            seekToSpecyficOffset();
            processEvents(rebalanceListner);

        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            try {
                log.info("Call commit sync");
                consumer.commitSync(rebalanceListner.getCurrentOffsets());
            } finally {
                log.info("Close consumer");
                consumer.close();
            }
        }
    }

    private void seekToSpecyficOffset() {
        consumer.assignment().forEach(partition -> {
            TopicPartition topicPartition = (TopicPartition) partition;
            var offset = offsetRepository.getOffset(topicPartition);
            if (offset.isPresent()) {
                long startOffset = offset.get() + 1;
                log.info("Consumer seek to partition = {}, and offset = {}", topicPartition, startOffset);
                consumer.seek(topicPartition, startOffset);
            }
        });
    }

    private void processEvents(RebalanceListner rebalanceListner) {
        while (!closed.get()) {
            ConsumerRecords<String, Person> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Person> record : records) {
                processSingleEvent(rebalanceListner, record);
            }
        }
    }

    private void processSingleEvent(RebalanceListner rebalanceListner, ConsumerRecord<String, Person> record) {
        log.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());

        final var eventId = record.value().getEventId();
        if (eventRepository.isEventProcessed(eventId)) {
            log.info("Event with eventId: " + eventId + " has been processed!");
            return;
        }

        //doing something with records
        eventRepository.saveEventId(eventId);
        offsetRepository.storeOffset(record);

        rebalanceListner.addOffset(record.topic(), record.partition(), record.offset());

        consumer.commitAsync(rebalanceListner.getCurrentOffsets(), new MyOffsetCommitCallback());
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    private class MyOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (Objects.nonNull(exception)) {
                log.error("Commit failed for offsets {}", offsets, exception);
            } else {
                log.info("Async commited successful");
            }
        }
    }
}