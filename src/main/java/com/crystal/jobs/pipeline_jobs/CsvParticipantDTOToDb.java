package com.crystal.jobs.pipeline_jobs;

import com.crystal.jobs.DTO.ParticipantDTO;
import com.crystal.jobs.DTO.UserDTO;
import com.crystal.jobs.utils.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.Serializable;
import java.util.Objects;

public class CsvParticipantDTOToDb implements Serializable {

    private CsvParticipantDTOToDb() {
    }

    private static CsvParticipantDTOToDb INSTANCE;

    public static synchronized CsvParticipantDTOToDb getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CsvParticipantDTOToDb();
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

    public static void main(String... args) {
        CsvDTOToDatabaseOptions options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
                .as(CsvDTOToDatabaseOptions.class);
        options.setInputFileCSV(
                String
                        .valueOf(EmailReminderOneDayBeforeEvent.class.getResource(""))
                        .concat("emailDTO"));
        options.setTableName("email_dto");
        CsvParticipantDTOToDb.getInstance().writeParticipantInfoToDb(options);
    }


    public void writeParticipantInfoToDb(CsvDTOToDatabaseOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        JdbcIO.DataSourceConfiguration sourceConfiguration = JdbcConnector.getInstance().getDB_SOURCE_CONFIGURATION();

        PCollectionView<Iterable<KV<String, UserDTO>>> users = pipeline.apply("read all users from db", JdbcIO.<UserDTO>read()
                        .withDataSourceConfiguration(sourceConfiguration)
                        .withQuery("Select id as id,type as userType,firstName as name,lastName as surname,email From users")
                        .withCoder(SerializableCoder.of(TypeDescriptor.of(UserDTO.class)))
                        .withRowMapper(
                                (JdbcIO.RowMapper<UserDTO>) resultSet -> new UserDTO(
                                        resultSet.getString("id"),
                                        resultSet.getString("userType"),
                                        resultSet.getString("firstName"),
                                        resultSet.getString("lastName"),
                                        resultSet.getString("email")
                                )
                        )
                )
                .apply(ParDo.of(new ConvertUserDTOToKV()))
                .apply(View.asIterable());

        PCollection<String> csvLines = readCSVJob(pipeline, options.getInputFileCSV());


        String header = "ID,NAME,SURNAME,EMAIL,...";
        String sqlStatementInsertIntoParticipants = "INSERT INTO participants  VALUES (?)";
        //todo ...
        String sqlStatementInsertIntoParticipantSession = "INSERT INTO participant_session  VALUES (?, ?, ?)";
        //todo ...
        String sqlStatementInsertIntoParticipantEvent = "INSERT INTO participant_event  VALUES (?, ?, ?)";

        PCollection<ParticipantDTO> participantsDTO = csvLines.apply(ParDo.of(new LineConvertToParticipantDTO(header)))
                .apply(ParDo.of(new DoFn<ParticipantDTO, ParticipantDTO>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        c.sideInput(users).forEach(k -> {
                                            if (Objects.equals(Objects.requireNonNull(c.element()).getId(), k.getKey())) {
                                                c.output(c.element());
                                            }
                                        });
                                    }
                                }
                ).withSideInputs(users));

        participantsDTO.apply("insert  participantDTO data into db participant table ", JdbcIO.<ParticipantDTO>write()
                .withDataSourceConfiguration(sourceConfiguration)
                .withStatement(sqlStatementInsertIntoParticipants)
                .withPreparedStatementSetter(new ParticipantPreparedStatementSetter()));


        participantsDTO.apply("insert  participant data into db participant_session table ", JdbcIO.<ParticipantDTO>write()
                .withDataSourceConfiguration(sourceConfiguration)
                .withStatement(sqlStatementInsertIntoParticipantSession)
                .withPreparedStatementSetter(new ParticipantSessionPreparedStatementSetter()));


        participantsDTO.apply("insert  participant data into db participant_event table ", JdbcIO.<ParticipantDTO>write()
                .withDataSourceConfiguration(sourceConfiguration)
                .withStatement(sqlStatementInsertIntoParticipantEvent)
                .withPreparedStatementSetter(new ParticipantEventPreparedStatementSetter()));


        pipeline.run().waitUntilFinish();


    }

    public PCollection<String> readCSVJob(Pipeline pipeline, String csv) {
        return pipeline.apply("readCSV", TextIO.read().from(csv));
    }
}



