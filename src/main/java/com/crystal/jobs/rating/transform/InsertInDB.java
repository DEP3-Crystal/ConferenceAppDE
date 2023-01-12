package com.crystal.jobs.rating.transform;

import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.io.Serializable;

public class InsertInDB extends PTransform<PCollection<KV<Integer, Double>>, PDone> implements Serializable {
    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();

    @Override
    public PDone expand(PCollection<KV<Integer, Double>> input) {
        return input.apply(JDBC_CONNECTOR.<KV<Integer, Double>>databaseWrite("sqlScripts/session/sessionUpdate.txt")
                .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<KV<Integer, Double>>) (element, preparedStatement) -> {
                    preparedStatement.setDouble(1, element.getValue());
                    preparedStatement.setInt(2, element.getKey());
                })
        );

    }
}