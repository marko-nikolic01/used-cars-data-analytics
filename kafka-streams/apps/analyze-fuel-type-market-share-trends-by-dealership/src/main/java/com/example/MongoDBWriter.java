package com.example;

import com.mongodb.client.*;
import org.bson.Document;

import java.util.Date;

public class MongoDBWriter {
    private static final String MONGO_URI = "mongodb://mongodb:27017";
    private static final String DATABASE_NAME = "used_cars";
    private static final String COLLECTION_NAME = "fuel_type_market_share_by_dealership";

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public MongoDBWriter() {
        this.mongoClient = MongoClients.create(MONGO_URI);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.collection = database.getCollection(COLLECTION_NAME);
    }

    public void writeToMongo(String dealership, String fuelType, double marketShare, long dealerCount, long fuelTotal, Date windowEnd) {
        Document doc = new Document()
            .append("dealership", dealership)
            .append("fuel_type", fuelType)
            .append("market_share", marketShare)
            .append("dealer_count", dealerCount)
            .append("fuel_total_count", fuelTotal)
            .append("window_end", windowEnd);

    collection.insertOne(doc);
}
}
