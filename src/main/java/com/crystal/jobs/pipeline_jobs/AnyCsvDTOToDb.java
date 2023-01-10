package com.crystal.jobs.pipeline_jobs;

import com.crystal.jobs.DTO.EmailInfoDTO;
import com.crystal.jobs.utils.JdbcConnector;
import com.crystal.jobs.utils.ObjectPreparedStatementSetter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.lang.reflect.Field;
import java.util.Objects;

/*
 * @Experimental job
 */
public class AnyCsvDTOToDb<T extends Object & java.io.Serializable> {
    private static AnyCsvDTOToDb<Class<EmailInfoDTO>> INSTANCE;

    public static synchronized AnyCsvDTOToDb<Class<EmailInfoDTO>> getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new AnyCsvDTOToDb<Class<EmailInfoDTO>>();
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


//        @Description("My Class")
//        @Default.Class(EmailInfoDTO.class)
//        EmailInfoDTO getMyClass();
//
//        void setMyClass(EmailInfoDTO myClass);


    }

    public static void main(String[] args) {
//        AnyCsvDTOToDb.getInstance().writeAnyObjToDB(PipelineOptionsFactory.fromArgs(args).as(CsvDTOToDatabaseOptions.class),EmailInfoDTO.class);


    }

    public void writeAnyObjToDB(CsvDTOToDatabaseOptions options, T t) {

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
        PCollection<String> lines = CsvParticipantDTOToDb.getInstance().readCSVJob(pipeline, options.getInputFileCSV());


        // Convert each line to an object
        PCollection<T> objects = lines.apply(ParDo.of(new LineToObjectFn<T>(fields, t)));

        // Save the objects to the database
        objects.apply(JdbcIO.<T>write()
                .withStatement(sqlStatement.toString())
                .withDataSourceConfiguration(JdbcConnector.getInstance().getDB_SOURCE_CONFIGURATION())
                .withPreparedStatementSetter(new ObjectPreparedStatementSetter<>(fields)));


        pipeline.run().waitUntilFinish();
    }

    public class LineToObjectFn<T extends java.io.Serializable> extends DoFn<String, T> {
        private Field[] fields;
        private T t;

        public LineToObjectFn(Field[] fields, T t) {
            this.fields = fields;
            this.t = t;
        }

        public LineToObjectFn() {
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
