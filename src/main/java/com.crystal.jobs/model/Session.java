package com.crystal.jobs.model;

import java.io.Serializable;

public class Session implements Serializable {
    private int id;
    private double rate;

    public Session(int id, double rate) {
        this.id = id;
        this.rate = rate;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    @Override
    public String toString() {
        return "Session{" +
                "id=" + id +
                ", rate=" + rate +
                '}';
    }
}
