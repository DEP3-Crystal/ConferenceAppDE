package com.crystal.jobs.utils;

import com.crystal.jobs.model.Participant;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FormatEmail implements Serializable {

    private static FormatEmail INSTANCE ;

    private FormatEmail() {
    }
    public static FormatEmail getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new FormatEmail();
        }
        return INSTANCE;
    }
    public List<Participant> format(Iterable<Participant> participantIterator,EmailTransform EMAIL_TRANSFORM) {
        return StreamSupport.stream(participantIterator.spliterator(), false)
                .map(participant1 -> {
                    Participant clone = SerializationUtils.clone(participant1);
                    clone.setMailContent(
                            EMAIL_TRANSFORM.mailTransform("[client]",
                                    participant1.getFirstName() + " " + participant1.getLastName(),"emailData/testmail.txt"));
                    return clone;
                }).collect(Collectors.toList());
    }
}
