package com.example;

public class Listing {
    private String make;
    private String model;
    private int year;
    private double price;
    private String listingDate;
    private String dealershipState;
    private double longitude;
    private double latitude;
    private String vin;

    public Listing() {}

    public Listing(String make, String model, int year, double price, String listingDate, String dealershipState, double longitude, double latitude, String vin) {
        this.make = make;
        this.model = model;
        this.year = year;
        this.price = price;
        this.listingDate = listingDate;
        this.dealershipState = dealershipState;
        this.longitude = longitude;
        this.latitude = latitude;
        this.vin = vin;
    }

    public String getMake() { return make; }
    public String getModel() { return model; }
    public int getYear() { return year; }
    public double getPrice() { return price; }
    public String getListingDate() { return listingDate; }
    public String getDealershipState() { return dealershipState; }
    public double getLongitude() { return longitude; }
    public double getLatitude() { return latitude; }
    public String getVin() { return vin; }
}
