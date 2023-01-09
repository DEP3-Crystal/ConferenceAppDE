package com.crystal.jobs.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@DefaultCoder(SerializableCoder.class)
public class ParticipantDTO {
//    userData
    private String id;
    private String name;
    private String surname;
    private String email;


    //participant-session data
    private String sessionId;

    private int chairNumber;
    private String zone;


    //participant-session data
    private String eventId;

}
