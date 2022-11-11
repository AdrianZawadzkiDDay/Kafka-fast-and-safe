package pl.softwareskill.course.kafka.producers.task.json;

import com.github.javafaker.Faker;
import pl.softwareskill.course.kafka.producers.jsonserializer.Person;

import java.time.LocalDate;
import java.util.*;

public class TextFactory {

    private static final Map<Integer, String> texts = Map.of(
            0, "Mars diameter: 6 779 km",
            1, "Distance of the Earth from the Moon: 384 400 km",
            2, "Ceres is small planet in asteroid belt",
            3, "Phobos and Deimos are the moons of Mars",
            4, "Jupiter: 80 moons    Saturn: 83 moons"
    );


    public static Text createRandomText() {
        return Text.builder()
                .textId(UUID.randomUUID())
                .text(getText())
                .createDate(new Date())
                .eventId(UUID.randomUUID())
                .build();
    }

    private static String getText() {
        int random = new Random().nextInt(5);
        return texts.get(random);
    }

}
