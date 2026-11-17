package com.yprnv7.kstreamlake.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Class<T> targetClass;

    public JsonSerde(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) return null;
            try {
                return OBJECT_MAPPER.readValue(bytes, targetClass);
            } catch (IOException e) {
                throw new SerializationException("Error deserializing JSON message", e);
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
