package com.example;

public class ModelListing {
    private String make;
    private String model;
    private int year;
    private int price;
    private String priceRange;
    private String listingDate;

    public ModelListing() {}

    public ModelListing(String make, String model, int year, int price, String priceRange, String listingDate) {
        this.make = make;
        this.model = model;
        this.year = year;
        this.price = price;
        this.priceRange = priceRange;
        this.listingDate = listingDate;
    }

    public String getMake() { return make; }
    public String getModel() { return model; }
    public int getYear() { return year; }
    public int getPrice() { return price; }
    public String getPriceRange() { return priceRange; }
    public String getListingDate() { return listingDate; }
}


