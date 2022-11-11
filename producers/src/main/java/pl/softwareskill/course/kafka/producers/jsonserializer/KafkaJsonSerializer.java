package pl.softwareskill.course.kafka.producers.jsonserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class KafkaJsonSerializer implements Serializer {

    @Override
    public void configure(Map map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
            log.error("Error while serialize message", e);
        }
        return retVal;
    }

    @Override
    public void close() {
    }
}
