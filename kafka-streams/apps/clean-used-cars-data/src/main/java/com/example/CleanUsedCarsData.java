package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.*;
import java.util.Map.Entry;

public class CleanUsedCarsData {
    public static void main(String[] args) {
        // Kafka Streams Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clean-used-cars-data");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Read from the raw_data topic
        KStream<String, String> rawStream = builder.stream("raw_data");

        ObjectMapper objectMapper = new ObjectMapper();

        // List of fields to remove
        List<String> fieldsToRemove = Arrays.asList("vin", "history", "wholesaleListing");

        // Map of renamed fields
        Map<String, String> fieldRenameMap = Map.ofEntries(
            Map.entry("@id", "listing_id"),
            Map.entry("location_0", "longitude"),
            Map.entry("location_1", "latitude"),
            Map.entry("createdAt", "listing_date_time"),
            Map.entry("online", "is_listing_online"),
            Map.entry("vehicle_vin", "vin"),
            Map.entry("vehicle_squishVin", "short_vin"),
            Map.entry("vehicle_year", "make_year"),
            Map.entry("vehicle_make", "make"),
            Map.entry("vehicle_model", "model"),
            Map.entry("vehicle_trim", "trim"),
            Map.entry("vehicle_drivetrain", "drivetrain"),
            Map.entry("vehicle_engine", "engine"),
            Map.entry("vehicle_fuel", "fuel_type"),
            Map.entry("vehicle_transmission", "transmission"),
            Map.entry("vehicle_confidence", "data_confidence_score"),
            Map.entry("vehicle_doors", "doors"),
            Map.entry("vehicle_seats", "seats"),
            Map.entry("vehicle_exteriorColor", "exterior_color"),
            Map.entry("vehicle_interiorColor", "interior_color"),
            Map.entry("retailListing_vdp", "detail_page_url"),
            Map.entry("retailListing_price", "price"),
            Map.entry("retailListing_miles", "mileage"),
            Map.entry("retailListing_used", "is_used"),
            Map.entry("retailListing_cpo", "certified_preowned_status"),
            Map.entry("retailListing_carfaxUrl", "carfax_history_url"),
            Map.entry("retailListing_dealer", "dealership"),
            Map.entry("retailListing_city", "dealership_city"),
            Map.entry("retailListing_state", "dealership_state"),
            Map.entry("retailListing_zip", "dealership_zip"),
            Map.entry("retailListing_primaryImage", "primary_photo_url"),
            Map.entry("retailListing_photoCount", "photo_count")
        );

        // Remove unwanted fields and rename fields
        KStream<String, String> transformedStream = rawStream.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode root = objectMapper.readTree(value);

                if (root.isObject()) {
                    ObjectNode obj = (ObjectNode) root;
                    for (String field : fieldsToRemove) {
                        obj.remove(field);
                    }
                }

                ObjectNode flat = objectMapper.createObjectNode();
                flattenJson("", root, flat, objectMapper);

                ObjectNode renamed = objectMapper.createObjectNode();
                Iterator<Entry<String, JsonNode>> fields = flat.fields();
                while (fields.hasNext()) {
                    Entry<String, JsonNode> entry = fields.next();
                    String renamedKey = fieldRenameMap.getOrDefault(entry.getKey(), entry.getKey());
                    renamed.set(renamedKey, entry.getValue());
                }

                return objectMapper.writeValueAsString(renamed);
            } catch (Exception e) {
                e.printStackTrace();
                return value;
            }
        });

        // Write the transformed data to the transformed_data topic
        transformedStream.to("transformed_data", Produced.with(Serdes.String(), Serdes.String()));

        // Start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void flattenJson(String prefix, JsonNode node, ObjectNode flat, ObjectMapper objectMapper) {
        if (node.isObject()) {
            Iterator<Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Entry<String, JsonNode> entry = fields.next();
                String newPrefix = prefix.isEmpty() ? entry.getKey() : prefix + "_" + entry.getKey();
                flattenJson(newPrefix, entry.getValue(), flat, objectMapper);
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                String newPrefix = prefix + "_" + i;
                flattenJson(newPrefix, node.get(i), flat, objectMapper);
            }
        } else {
            flat.set(prefix, node);
        }
    }
}
