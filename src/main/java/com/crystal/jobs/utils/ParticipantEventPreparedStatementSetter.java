package com.crystal.jobs.utils;

import com.crystal.jobs.DTO.ParticipantDTO;

import java.sql.PreparedStatement;

public class ParticipantEventPreparedStatementSetter implements org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter<com.crystal.jobs.DTO.ParticipantDTO> {
    @Override
    public void setParameters(ParticipantDTO element, PreparedStatement preparedStatement) throws Exception {
        //todo to implement

    }
}
