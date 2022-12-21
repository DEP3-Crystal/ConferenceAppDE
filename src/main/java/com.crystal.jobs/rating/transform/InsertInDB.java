package com.crystal.jobs.rating.transform;

import com.crystal.jobs.model.Session;
import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.io.IOException;
import java.io.Serializable;

public class InsertInDB extends PTransform<PCollection<Session>, PDone> implements Serializable {
    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();

    @Override
    public PDone expand(PCollection<Session> input) {
        try {
            return input.apply(JDBC_CONNECTOR.<Session>databaseWrite("sqlScripts/session/sessionUpdate.txt")
                    .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<Session>) (element, preparedStatement) -> {
                        preparedStatement.setDouble(1, element.getRate());
                        preparedStatement.setInt(2, element.getId());
                    })
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}