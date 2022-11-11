package pl.softwareskill.course.kafka.producers.task.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

import static lombok.AccessLevel.PRIVATE;

@FieldDefaults(level = PRIVATE)
@Builder
@Data
public class Text {

    public Text(@JsonProperty("textId") UUID textId,
                @JsonProperty("lastName") String text,
                @JsonProperty("createDate") Date createDate,
                @JsonProperty("eventId") UUID eventId) {

        this.textId = textId;
        this.text = text;
        this.createDate = createDate;
        this.eventId = eventId;
    }

    UUID textId;
    String text;
    Date createDate;
    UUID eventId;
}
