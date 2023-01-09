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
@DefaultCoder(SerializableCoder.class)
public class UserDTO implements Serializable {
    private String id;
    private String type;
    private String firstName;
    private String lastName;
    private String email;
}
