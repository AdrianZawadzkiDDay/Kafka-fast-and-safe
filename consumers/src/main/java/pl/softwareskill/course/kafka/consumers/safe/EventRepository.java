package pl.softwareskill.course.kafka.consumers.safe;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import static lombok.AccessLevel.PRIVATE;
import lombok.experimental.FieldDefaults;

/*
 * database mock
 */
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class EventRepository {

    /**
     * Nie robić tak na produkcji!
     * Rozwiązanie nie wspiera wielowątkowości
     * Więcej - https://stackoverflow.com/questions/7266042/ring-buffer-in-java/7266175#7266175
     * Dodatkowo trzeba zaopiekować temat czyszczenia Seta uuid - nie potrzebujemy tych informacji w nieskończoność!
     */
    Set<UUID> eventIds = new HashSet<>();

    public boolean isEventProcessed(UUID eventId) {
        return eventIds.contains(eventId);
    }

    public void saveEventId(UUID eventId) {
        eventIds.add(eventId);
    }
}
