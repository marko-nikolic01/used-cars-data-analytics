package com.example;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ModelListingSerde extends Serdes.WrapperSerde<ModelListing> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ModelListingSerde() {
        super(new ModelListingSerializer(), new ModelListingDeserializer());
    }

    public static class ModelListingSerializer implements Serializer<ModelListing> {
        @Override
        public byte[] serialize(String topic, ModelListing data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing ModelListing", e);
            }
        }
    }

    public static class ModelListingDeserializer implements Deserializer<ModelListing> {
        @Override
        public ModelListing deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, ModelListing.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing ModelListing", e);
            }
        }
    }
}
