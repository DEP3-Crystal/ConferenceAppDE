package com.crystal.jobs.rating;

import com.crystal.jobs.model.Participant;
import com.crystal.jobs.model.Session;
import com.crystal.jobs.model.Speaker;
import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TopSessionRate {

    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(new ReadSessionDB())
                .apply(ParDo.of(new SessionIdKvFn()))
                .apply(GroupByKey.create())
                .apply(ParDo.of(new SessionWithSpeakersFn()))
                .apply(ParDo.of(new DoFn<Session, Void>() {
                    @ProcessElement
                    public void apply(ProcessContext c) {
                        System.out.println(c.element());
                    }
                }));

//        pipeline.apply(new ReadParticipantDB());

        pipeline.run();

    }

    private static class ReadParticipantDB extends PTransform<PBegin, PCollection<Participant>> {
        @Override
        public PCollection<Participant> expand(PBegin input) {
            try {
                return input.apply(JDBC_CONNECTOR.<Participant>databaseInit("sqlScripts/session/TopSessionForParticipant.txt")
                        .withRowMapper(resultSet -> {
                            String firstName = resultSet.getString(1);
                            String lastName = resultSet.getString(2);
                            String email = resultSet.getString(3);
                            return new Participant(firstName, lastName, email);
                        }).withCoder(SerializableCoder.of(Participant.class))
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ReadSessionDB extends PTransform<PBegin, PCollection<Session>> {
        @Override
        public PCollection<Session> expand(PBegin input) {
            try {
                return input.apply(JDBC_CONNECTOR.<Session>databaseInit("sqlScripts/session/SelectSessionRate.txt")
                        .withRowMapper((JdbcIO.RowMapper<Session>) resultSet -> {
                            int id = resultSet.getInt(1);
                            String sessionTitle = resultSet.getString(2);
                            double rate = resultSet.getDouble(3);
                            String firstName = resultSet.getString(4);
                            String lastName = resultSet.getString(5);
                            Session session = new Session(id, sessionTitle, rate);
                            session.getSpeakers().add(new Speaker(firstName, lastName));
                            return session;
                        })
                        .withCoder(SerializableCoder.of(Session.class)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static class SessionIdKvFn extends DoFn<Session, KV<Integer, Session>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(KV.of(c.element().getId(), c.element()));
        }
    }

    public static class SessionWithSpeakersFn extends DoFn<KV<Integer, Iterable<Session>>, Session> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Iterable<Session> sessionIterable = c.element().getValue();

            Session session = SerializationUtils.clone(sessionIterable.iterator().next());
            List<Speaker> speakers = StreamSupport.stream(sessionIterable.spliterator(), false)
                    .flatMap(sessions -> sessions.getSpeakers().stream()).collect(Collectors.toList());
            session.setSpeakers(speakers);

            c.output(session);
        }

    }
}

