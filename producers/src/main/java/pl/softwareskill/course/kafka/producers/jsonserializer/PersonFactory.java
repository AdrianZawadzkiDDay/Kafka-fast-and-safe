package pl.softwareskill.course.kafka.producers.jsonserializer;

import com.github.javafaker.Faker;
import java.util.UUID;

public class PersonFactory {

    public static Person createRandomPerson() {
        Faker faker = new Faker();
        return Person.builder()
                .firstName(faker.name().firstName())
                .lastName(faker.name().lastName())
                .birthday(faker.date().birthday())
                .eventId(UUID.randomUUID())
                .build();
    }
}
