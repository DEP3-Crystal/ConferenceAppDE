package com.crystal.jobs.model;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data

public class Session implements Serializable {
    private int id;
    private String sessionTitle;
    private double sessionRate;
    private List<Speaker> speakers;

    public Session(int id, String sessionTitle, double sessionRate) {
        this.id = id;
        this.sessionTitle = sessionTitle;
        this.sessionRate = sessionRate;
        this.speakers =new ArrayList<>();
    }

    @Override
    public String toString() {
        return
                "sessionTitle='" + sessionTitle + '\'' +
                ", presented by " + speakers.stream().map(Speaker::toString).collect(Collectors.joining(", "))  ;

    }
}
