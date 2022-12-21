package com.crystal.jobs.rating.transform;

import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.io.Serializable;

public  class ReadDataFRDB extends PTransform<PBegin, PCollection<String>> implements Serializable {
    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();
    private  int id;
    private String path;
    public ReadDataFRDB(int id,String path) {
        this.id = id;
        this.path = path;
    }
    public ReadDataFRDB(String path) {
        this.path = path;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
        try {
            return input.apply(JDBC_CONNECTOR.<String>databaseInit(path)
                    .withStatementPreparator((JdbcIO.StatementPreparator) preparedStatement ->
                            preparedStatement.setInt(1, id))
                    .withRowMapper((JdbcIO.RowMapper<String>) resultSet ->
                            resultSet.getString(1) + "," + resultSet.getString(2)
                                    + "," + resultSet.getString(3)));
        } catch (IOException e) {
            throw new RuntimeException("Something went wrong", e);
        }
    }
}