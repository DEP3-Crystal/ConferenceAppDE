package com.crystal.jobs.utils;

import com.crystal.jobs.DTO.ParticipantDTO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.sql.PreparedStatement;

public class ParticipantSessionPreparedStatementSetter implements JdbcIO.PreparedStatementSetter<ParticipantDTO> {
    @Override
    public void setParameters(ParticipantDTO element, PreparedStatement preparedStatement) throws Exception {
        preparedStatement.setInt(1, element.getId());
        preparedStatement.setInt(2, element.getSessionId());
        preparedStatement.setInt(3, 5);
    }
}
