package pl.softwareskill.course.kafka.producers.jsonserializer;

import java.util.Date;
import java.util.UUID;
import static lombok.AccessLevel.PRIVATE;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = PRIVATE)
@Builder
@Data
public class Person {
    String firstName;
    String lastName;
    Date birthday;
    UUID eventId;
}
