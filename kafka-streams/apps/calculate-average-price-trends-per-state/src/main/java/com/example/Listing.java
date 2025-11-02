package com.example;

class Listing {
    private String vin;
    private String make;
    private String model;
    private int year;
    private String state;
    private double price;
    private String carKey;
    private double avgPrice;
    private double percentOffset;

    public Listing() {}

    public Listing(String vin, String make, String model, int year, String state, double price, String carKey) {
        this.vin = vin;
        this.make = make;
        this.model = model;
        this.year = year;
        this.state = state;
        this.price = price;
        this.carKey = carKey;
    }

    public String getVin() { return vin; }
    public String getMake() { return make; }
    public String getModel() { return model; }
    public int getYear() { return year; }
    public String getState() { return state; }
    public double getPrice() { return price; }
    public String getCarKey() { return carKey; }
    public double getAvgPrice() { return avgPrice; }
    public double getPercentOffset() { return percentOffset; }
    public void setAvgPrice(double avgPrice) { this.avgPrice = avgPrice; }
    public void setPercentOffset(double percentOffset) { this.percentOffset = percentOffset; }
}

