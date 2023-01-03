package com.crystal.jobs.pipeline_jobs;


import com.crystal.jobs.utils.JdbcConnector;
import com.crystal.jobs.utils.ObjectPreparedStatementSetter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.lang.reflect.Field;
import java.util.Objects;

public class CsvDTOToDatabase<T> {

    public interface CsvDTOToDatabaseOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Validation.Required
        String getInputFileCSV();

        void setInputFileCSV(String value);


        @Description("table to write")
        @Validation.Required
        String getTableName();

        void setTableName(String value);


        @Description("My Class")
        @Default.Class(Objects.class)
        Objects getMyClass();

        void setMyClass(Objects myClass);


    }

    public static void main(String... args) {

        CsvDTOToDatabaseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CsvDTOToDatabaseOptions.class);

        new CsvDTOToDatabase<>().runPipeline(options, options.getMyClass(), options.getInputFileCSV(), options.getTableName());


    }

    public void runPipeline(PipelineOptions options, T t, String fileName, String tableName) {

        Pipeline pipeline = Pipeline.create(options);

        // Read the CSV file
        PCollection<String> lines = pipeline.apply(TextIO.read().from(fileName));

        // Convert each line to an object
        PCollection<T> objects = lines.apply(ParDo.of(new LineToObjectFn<T>()));

        // Save the objects to the database
        objects.apply(JdbcIO.<T>write()
                .withStatement("INSERT INTO" + tableName + "  VALUES (?)")
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                JdbcConnector.getInstance().getDRIVER_CLASS_NAME()
                                , JdbcConnector.getInstance().getDB_URL())
                        .withUsername(JdbcConnector.getInstance().getDB_USER_NAME())
                        .withPassword(JdbcConnector.getInstance().getDB_PASSWORD()))
                .withPreparedStatementSetter(new ObjectPreparedStatementSetter<T>()));

        pipeline.run();
    }


    static class LineToObjectFn<T> extends DoFn<String, T> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IllegalAccessException {
            String[] line = Objects.requireNonNull(c.element()).split(",");
            T object = null;

            Class<?> objectClass = object.getClass();
            Field[] fields = objectClass.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                field.set(object, line[i]);
            }

            // Convert the line to an object and emit it
            c.output(object);
        }
    }
}



