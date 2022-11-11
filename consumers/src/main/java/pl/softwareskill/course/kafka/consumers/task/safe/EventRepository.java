package pl.softwareskill.course.kafka.consumers.task.safe;

import lombok.experimental.FieldDefaults;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static lombok.AccessLevel.PRIVATE;

/*
 * database mock
 */
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class EventRepository {

    Set<UUID> eventIds = new HashSet<>();

    public boolean isEventProcessed(UUID eventId) {
        return eventIds.contains(eventId);
    }

    public void saveEventId(UUID eventId) {
        eventIds.add(eventId);
    }
}
