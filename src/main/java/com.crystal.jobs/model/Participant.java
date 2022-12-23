package com.crystal.jobs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Participant implements Serializable {
    private String firstName;
    private String lastName;
    private String email;
}
