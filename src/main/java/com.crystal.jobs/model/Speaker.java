package com.crystal.jobs.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
@Data
@AllArgsConstructor
public class Speaker implements Serializable {
    private String speakerFirstName;
    private String speakerLastName;

    @Override
    public String toString() {
        return
                speakerFirstName + '\'' + speakerLastName;
    }
}
