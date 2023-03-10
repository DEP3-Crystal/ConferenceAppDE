package com.crystal.jobs.utils;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;

public class ObjectPreparedStatementSetter<T> implements JdbcIO.PreparedStatementSetter<T> {
    private Field[] fields;

    public ObjectPreparedStatementSetter(Field[] fields) {
        this.fields = fields;
    }


    @Override
    public void setParameters(T object, PreparedStatement preparedStatement) throws Exception {

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            Object value = field.get(object);
            preparedStatement.setObject(i + 1, value);
        }
    }
}


