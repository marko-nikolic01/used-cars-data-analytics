package com.example;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class FuelListingSerde extends Serdes.WrapperSerde<FuelListing> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public FuelListingSerde() {
        super(new FuelListingSerializer(), new FuelListingDeserializer());
    }

    public static class FuelListingSerializer implements Serializer<FuelListing> {
        @Override
        public byte[] serialize(String topic, FuelListing data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing FuelListing", e);
            }
        }
    }

    public static class FuelListingDeserializer implements Deserializer<FuelListing> {
        @Override
        public FuelListing deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, FuelListing.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing FuelListing", e);
            }
        }
    }
}
