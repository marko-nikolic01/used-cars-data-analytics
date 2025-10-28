package com.example;

import java.util.Date;

import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBWriter {

    private static final String MONGO_URI = "mongodb://mongodb:27017";
    private static final String DATABASE_NAME = "used_cars";
    private static final String COLLECTION_NAME = "average_price_trends_per_state";

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public MongoDBWriter() {
        this.mongoClient = MongoClients.create(MONGO_URI);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.collection = database.getCollection(COLLECTION_NAME);
    }

public void writeToMongo(String state, Date timestamp, String vin, String make, String model, int year, double listingPrice, double avgPrice, double percentOffset) {
    Document doc = new Document()
            .append("state", state)
            .append("timestamp", timestamp)
            .append("vin", vin)
            .append("make", make)
            .append("model", model)
            .append("year", year)
            .append("listing_price", listingPrice)
            .append("avg_price_last_year", avgPrice)
            .append("percent_offset", percentOffset);

    collection.insertOne(doc);
}

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
