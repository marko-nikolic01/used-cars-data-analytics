package com.example;

public class AveragePrice {
    private double sum = 0.0;
    private long count = 0;
    private double average = 0.0;

    public AveragePrice() {}

    public void add(double price) { 
        sum += price; 
        count++; 
        average = getAverage(); 
    }

    public void remove(double price) { 
        sum -= price; 
        count--; 
        average = getAverage();
    }

    public double getAverage() { return count > 0 ? sum / count : 0; }
    public long getCount() { return count; }
}
