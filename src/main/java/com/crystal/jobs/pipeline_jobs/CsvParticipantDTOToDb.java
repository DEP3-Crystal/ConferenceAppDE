package com.crystal.jobs.pipeline_jobs;

import com.crystal.jobs.DTO.ParticipantDTO;
import com.crystal.jobs.DTO.UserDTO;
import com.crystal.jobs.utils.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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


    public static void main(String... args) {
        CsvDTOToDatabaseOptions options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
                .as(CsvDTOToDatabaseOptions.class);
        options.setInputFileCSV("src/main/resources/data/participantData.csv");

        CsvParticipantDTOToDb.getInstance().writeParticipantInfoToDb(options);
    }


    public void writeParticipantInfoToDb(CsvDTOToDatabaseOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        JdbcIO.DataSourceConfiguration sourceConfiguration = JdbcConnector.getInstance().getDB_SOURCE_CONFIGURATION();

        PCollectionView<Iterable<KV<String, UserDTO>>> users = pipeline.apply("read all users from db", JdbcIO.<UserDTO>read()
                        .withDataSourceConfiguration(sourceConfiguration)
                        .withQuery("Select id as id,dtype as user_type,first_name as name,last_name as surname,email From user")
                        .withCoder(SerializableCoder.of(TypeDescriptor.of(UserDTO.class)))
                        .withRowMapper(
                                (JdbcIO.RowMapper<UserDTO>) resultSet -> new UserDTO(
                                        resultSet.getString("id"),
                                        resultSet.getString("user_type"),
                                        resultSet.getString("name"),
                                        resultSet.getString("surname"),
                                        resultSet.getString("email")
                                )
                        )
                )
                .apply(ParDo.of(new ConvertUserDTOToKV()))
                .apply(View.asIterable());

//        PCollection<String> csvLines = readCSVJob(pipeline, options.getInputFileCSV());


        String header = "id,name,surname,email,session_id,chairNumber,zone,eventId";
        String sqlStatementInsertIntoParticipants = "INSERT INTO conference.participant  VALUES (?)";
        String sqlStatementInsertIntoParticipantSession = "INSERT INTO participant_session  VALUES (?, ?, ?)";
        String sqlStatementInsertIntoParticipantEvent = "INSERT INTO participant_event  VALUES (?, ?)";

        PCollection<ParticipantDTO> participantsDTO = readCSVJob(pipeline, options.getInputFileCSV())
                .apply("Convert to participant objects", ParDo.of(new LineConvertToParticipantDTO(header)))
                .apply("check for id match between 2 datasets ", ParDo.of(new DoFn<ParticipantDTO, ParticipantDTO>() {
                                                                              @ProcessElement
                                                                              public void apply(ProcessContext c) {
                                                                                  c.sideInput(users).forEach(k -> {
                                                                                      if (Objects.equals(Objects.requireNonNull(c.element()).getId(), Integer.parseInt(k.getKey()))) {
                                                                                          c.output(c.element());
                                                                                        Log.logInfo(c.element() +" match with " + k.getValue());
                                                                                      }
                                                                                  });
                                                                              }
                                                                          }
                ).withSideInputs(users));
        try {
            participantsDTO.apply("insert  participantDTO data into db participant table ", JdbcIO.<ParticipantDTO>write()
                    .withDataSourceConfiguration(sourceConfiguration)
                    .withStatement(sqlStatementInsertIntoParticipants)
                    .withPreparedStatementSetter(new ParticipantPreparedStatementSetter()));
            Log.logInfo("participant data inserted");
        } catch (Exception e) {
            Log.logInfo("participant data not  inserted");
        }

        try {
            participantsDTO.apply("insert  participant data into db participant_session table ", JdbcIO.<ParticipantDTO>write()
                    .withDataSourceConfiguration(sourceConfiguration)
                    .withStatement(sqlStatementInsertIntoParticipantSession)
                    .withPreparedStatementSetter(new ParticipantSessionPreparedStatementSetter()));
            Log.logInfo("participant_session data inserted");
        } catch (Exception e) {
            Log.logInfo("participant_session data not  inserted");

        }

        try {
            participantsDTO.apply("insert  participant data into db participant_event table ", JdbcIO.<ParticipantDTO>write()
                    .withDataSourceConfiguration(sourceConfiguration)
                    .withStatement(sqlStatementInsertIntoParticipantEvent)
                    .withPreparedStatementSetter(new ParticipantEventPreparedStatementSetter()));
            Log.logInfo("participant_event data inserted");
        } catch (Exception e) {
            Log.logInfo("participant_event data not  inserted");

        }


        pipeline.run().waitUntilFinish();


    }

    public PCollection<String> readCSVJob(Pipeline pipeline, String csv) {
        return pipeline.apply(
                "readCSV",
                TextIO.read().from(csv)
        );
    }
}



