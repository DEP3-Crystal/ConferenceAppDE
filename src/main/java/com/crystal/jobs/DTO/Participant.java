package com.crystal.jobs.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Participant implements Serializable {
    private String id;
    private String name;
    private String surname;
    private String email;

}
