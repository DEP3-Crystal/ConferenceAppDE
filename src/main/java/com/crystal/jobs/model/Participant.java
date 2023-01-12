package com.crystal.jobs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data

@NoArgsConstructor
public class Participant implements Serializable {
    private String firstName;
    private String lastName;
    private String email;
    private String mailContent;

    public Participant(String firstName, String lastName, String email) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
    }
}
