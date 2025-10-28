package com.example;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ListingSerde extends Serdes.WrapperSerde<Listing> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ListingSerde() {
        super(new ListingSerializer(), new ListingDeserializer());
    }

    public static class ListingSerializer implements Serializer<Listing> {
        @Override
        public byte[] serialize(String topic, Listing data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing Listing", e);
            }
        }
    }

    public static class ListingDeserializer implements Deserializer<Listing> {
        @Override
        public Listing deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, Listing.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing Listing", e);
            }
        }
    }
}
