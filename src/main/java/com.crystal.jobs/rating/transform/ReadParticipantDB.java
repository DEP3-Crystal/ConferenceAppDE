package com.crystal.jobs.rating.transform;

import com.crystal.jobs.model.Participant;
import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public class ReadParticipantDB extends PTransform<PBegin, PCollection<Participant>> implements Serializable {
    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();
    @Override
    public PCollection<Participant> expand(PBegin input) {
        return input.apply(JDBC_CONNECTOR.<Participant>databaseInit("/sqlScripts/session/TopSessionForParticipant.txt")
                .withRowMapper(resultSet -> {
                    String firstName = resultSet.getString(1);
                    String lastName = resultSet.getString(2);
                    String email = resultSet.getString(3);
                    return new Participant(firstName, lastName, email);
                }).withCoder(SerializableCoder.of(Participant.class))
        );
    }
}
