package pl.softwareskill.course.kafka.consumers.jsondeserializer;

import com.fasterxml.jackson.annotation.JsonProperty;
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

    public Person(@JsonProperty("firstName") String firstName, @JsonProperty("lastName") String lastName, @JsonProperty("birthday") Date birthday, @JsonProperty("eventId") UUID eventId) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthday = birthday;
        this.eventId = eventId;
    }

    String firstName;
    String lastName;
    Date birthday;
    UUID eventId;
}
