package com.crystal.jobs.utils;

import com.crystal.jobs.DTO.ParticipantDTO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;

public class ParticipantPreparedStatementSetter implements JdbcIO.PreparedStatementSetter<ParticipantDTO> {
    private Field[] fields;

    public ParticipantPreparedStatementSetter(Field[] fields) {
        this.fields = fields;
    }

    public ParticipantPreparedStatementSetter() {
    }

    @Override
    public void setParameters(ParticipantDTO participantDTO, PreparedStatement preparedStatement) throws Exception {
        preparedStatement.setString(1, participantDTO.getId());
    }
}


