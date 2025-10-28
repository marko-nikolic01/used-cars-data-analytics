package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class CalculateAveragePriceTrendsByState {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "calculate-average-price-trends-pet-state");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("transformed_data");

        // Cleaned stream with Listing objects
        KStream<String, Listing> listingStream = inputStream
            .mapValues(value -> {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    JsonNode jsonNode = objectMapper.readTree(value);

                    if (!jsonNode.hasNonNull("make") ||
                        !jsonNode.hasNonNull("model") ||
                        !jsonNode.hasNonNull("make_year") ||
                        !jsonNode.hasNonNull("price") ||
                        !jsonNode.hasNonNull("dealership_state") ||
                        !jsonNode.hasNonNull("longitude") ||
                        !jsonNode.hasNonNull("latitude") ||
                        !jsonNode.hasNonNull("vin")) {
                        return null;
                    }

                    double price = jsonNode.get("price").asDouble();
                    if (price <= 0) {
                        return null;
                    }
                    
                    return value;
                } catch (Exception e) {
                    return null;
                }
            })
            .filter((k, v) -> v != null)
            .map((key, value) -> {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    JsonNode json = objectMapper.readTree(value);

                    String make = json.get("make").asText();
                    String model = json.get("model").asText();
                    int year = json.get("make_year").asInt();
                    double price = json.get("price").asDouble();
                    String state = json.get("dealership_state").asText();
                    String vin = json.get("vin").asText();
                    String carKey = make + "-" + model + "-" + year;

                    Listing listing = new Listing(vin, make, model, year, state, price, carKey);
                    return new KeyValue<>(carKey, listing);

                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            })
            .filter((key, value) -> key != null && value != null);

        // Aggregate rolling average price per carKey
        Map<String, AveragePrice> averages = new ConcurrentHashMap<>();
        KStream<String, Listing> enrichedStream = listingStream.mapValues(listing -> {
            AveragePrice avg = averages.computeIfAbsent(listing.getCarKey(), k -> new AveragePrice());
            double currentAvg = avg.getAverage();
            avg.add(listing.getPrice());
            double offset = currentAvg > 0 ? (listing.getPrice() - currentAvg) / currentAvg * 100.0 : 0.0;
            listing.setAvgPrice(avg.getAverage());
            listing.setPercentOffset(offset);
            return listing;
        });

        // Write enriched listings to MongoDB
        MongoDBWriter mongoDBWriter = new MongoDBWriter();
        enrichedStream.foreach((carKey, listing) -> {
            mongoDBWriter.writeToMongo(
                listing.getState(),
                new Date(),
                listing.getVin(),
                listing.getMake(),
                listing.getModel(),
                listing.getYear(),
                listing.getPrice(),
                listing.getAvgPrice(),
                listing.getPercentOffset()
            );
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}