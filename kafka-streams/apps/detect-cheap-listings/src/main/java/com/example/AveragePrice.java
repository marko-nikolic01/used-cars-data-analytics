package com.example;

public class AveragePrice {
    private double total = 0.0;
    private int count = 0;
    private double average = 0.0;

    public AveragePrice() {}

    public void add(double price) {
        total += price;
        count++;
        average = count == 0 ? 0.0 : total / count;
    }

    public double getTotal() { return total; }
    public int getCount() { return count; }
    public double getAverage() { return average; }

    public void setTotal(double total) { this.total = total; }
    public void setCount(int count) { this.count = count; }
    public void setAverage(double average) { this.average = average; }
}
