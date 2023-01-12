package com.crystal.jobs.utils;

import com.crystal.jobs.DTO.ParticipantDTO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.sql.PreparedStatement;

public class ParticipantEventPreparedStatementSetter implements JdbcIO.PreparedStatementSetter<ParticipantDTO> {
    @Override
    public void setParameters(ParticipantDTO element, PreparedStatement preparedStatement) throws Exception {
   preparedStatement.setInt(1, element.getEventId());
   preparedStatement.setInt(2, element.getId());

    }
}
