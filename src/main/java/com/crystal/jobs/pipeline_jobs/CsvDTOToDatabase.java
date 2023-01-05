package com.crystal.jobs.pipeline_jobs;


import com.crystal.jobs.DTO.EmailInfoDTO;
import com.crystal.jobs.utils.JdbcConnector;
import com.crystal.jobs.utils.Log;
import com.crystal.jobs.utils.ObjectPreparedStatementSetter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Objects;

public class CsvDTOToDatabase<T extends Serializable> implements Serializable {

    private CsvDTOToDatabase() {
    }

    private static CsvDTOToDatabase<EmailInfoDTO> INSTANCE;

    public static synchronized CsvDTOToDatabase<EmailInfoDTO> getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CsvDTOToDatabase<>();
        }
        return INSTANCE;
    }

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
        @Default.Class(EmailInfoDTO.class)
        EmailInfoDTO getMyClass();

        void setMyClass(EmailInfoDTO myClass);


    }

    public static void main(String... args) {


        CsvDTOToDatabaseOptions options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
                .as(CsvDTOToDatabaseOptions.class);
        options.setInputFileCSV(String.valueOf(EmailSenderPipeline.class.getResource("")).concat("emailDTO"));
        try {
            options.setMyClass(EmailInfoDTO.class.newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        options.setTableName("email_dto");


            CsvDTOToDatabase.getInstance().runPipeline(options, options.getMyClass());



    }

    public void runPipeline(CsvDTOToDatabaseOptions options, T t) {

        Pipeline pipeline = Pipeline.create(options);

        Class<?> objectClass = t.getClass();
        Field[] fields = objectClass.getDeclaredFields();
        StringBuilder sqlStatement = new StringBuilder();
        sqlStatement.append("INSERT INTO ")
                .append(options.getTableName())
                .append(" (");
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (i > 0) {
                sqlStatement.append(", ");
            }
            sqlStatement.append(field.getName());
        }
        sqlStatement.append(") VALUES (");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sqlStatement.append(", ");
            }
            sqlStatement.append("?");
        }
        sqlStatement.append(")");


        // Read the CSV file
        PCollection<String> lines = pipeline.apply(TextIO.read().from(options.getInputFileCSV()));


        // Convert each line to an object
        PCollection<T> objects = lines.apply(ParDo.of(new LineToObjectFn<>(fields, t)));

        // Save the objects to the database
        objects.apply(JdbcIO.<T>write()
                .withStatement(sqlStatement.toString())
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                JdbcConnector.getInstance().getDRIVER_CLASS_NAME(),
                                JdbcConnector.getInstance().getDB_URL())
                        .withUsername(JdbcConnector.getInstance().getDB_USER_NAME())
                        .withPassword(JdbcConnector.getInstance().getDB_PASSWORD()))

                .withPreparedStatementSetter(new ObjectPreparedStatementSetter<>(fields)));
        pipeline.run().waitUntilFinish();
    }


    static class LineToObjectFn<T extends Serializable> extends DoFn<String, T> {
        private final Field[] fields;
        private T t;

        LineToObjectFn(Field[] fields, T t) {
            this.fields = fields;
            this.t = t;
        }


        @ProcessElement
        public void processElement(ProcessContext c) throws IllegalAccessException {
            String[] line = Objects.requireNonNull(c.element()).split(",");


            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);

                field.set(t, line[i]);

            }
            // Convert the line to an object and emit it
            c.output(t);
        }
    }
}



