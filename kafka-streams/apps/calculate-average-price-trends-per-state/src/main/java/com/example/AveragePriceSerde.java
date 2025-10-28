package com.example;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class AveragePriceSerde extends Serdes.WrapperSerde<AveragePrice> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public AveragePriceSerde() {
        super(new AveragePriceSerializer(), new AveragePriceDeserializer());
    }

    public static class AveragePriceSerializer implements Serializer<AveragePrice> {
        @Override
        public byte[] serialize(String topic, AveragePrice data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing AveragePrice", e);
            }
        }
    }

    public static class AveragePriceDeserializer implements Deserializer<AveragePrice> {
        @Override
        public AveragePrice deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, AveragePrice.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing AveragePrice", e);
            }
        }
    }
}
