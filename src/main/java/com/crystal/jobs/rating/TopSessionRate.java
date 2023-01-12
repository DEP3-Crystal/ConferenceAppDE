package com.crystal.jobs.rating;

import com.crystal.jobs.model.Participant;
import com.crystal.jobs.model.Session;
import com.crystal.jobs.model.Speaker;
import com.crystal.jobs.rating.transform.KeyValueFn;
import com.crystal.jobs.rating.transform.ParticipantMail;
import com.crystal.jobs.rating.transform.ReadParticipantDB;
import com.crystal.jobs.utils.EmailTransform;
import com.crystal.jobs.utils.FormatEmail;
import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TopSessionRate {


    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();
    private static final EmailTransform EMAIL_TRANSFORM = EmailTransform.getInstance();
    private static final FormatEmail FORMAT_EMAIL=FormatEmail.getInstance();
    private static final TupleTag<String> mailOutputTag = new TupleTag<>() {
    };
    private static final TupleTag<Participant> participantOutputTag = new TupleTag<>() {
    };


    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<Integer, String>> email = pipeline.apply("Read Session from db", new ReadSessionDB())
                .apply("making a kv for session id", ParDo.of(new SessionIdKvFn()))
                .apply("Grouping by session id", GroupByKey.create())
                .apply("inserting all speakers into session", ParDo.of(new SessionWithSpeakersFn()))
                .apply("Getting a list of top sessions", Combine.globally(new SessionListCombineFn()))
                .apply("Making the mail for top session", ParDo.of(new MailMaker()))
                .apply("Making a kv for session", ParDo.of(new KeyValueFn<>()));

        PCollection<KV<Integer, Participant>> participant =
                pipeline.apply("Reading participants from db", new ReadParticipantDB())
                        .apply("Making a kv for participant ", ParDo.of(new KeyValueFn<>()));

        PCollection<KV<Integer, CoGbkResult>> mailAndParticipant = KeyedPCollectionTuple.of(participantOutputTag, participant)
                .and(mailOutputTag, email)
                .apply(CoGroupByKey.create());
        mailAndParticipant.apply("Setting mail content to each participant", ParDo.of(new ParticipantWithMailContent()))
                .apply("Sending mail to each participant", ParDo.of(new ParticipantMail()));
        pipeline.run();


    }

    private static class ParticipantWithMailContent extends DoFn<KV<Integer, CoGbkResult>, Participant> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String mail = Objects.requireNonNull(c.element()).getValue().getOnly(mailOutputTag);
            Iterable<Participant> participants = Objects.requireNonNull(c.element()).getValue().getAll(participantOutputTag);
            List<Participant> participantList = FORMAT_EMAIL.format(participants,EMAIL_TRANSFORM);
            participantList.forEach(participant -> participant.setMailContent(mail));
            participantList.forEach(c::output);
        }
    }



    public static class ReadSessionDB extends PTransform<PBegin, PCollection<Session>> {
        @Override
        public PCollection<Session> expand(PBegin input) {
            return input.apply(JDBC_CONNECTOR.<Session>databaseInit("/sqlScripts/session/SelectSessionRate.txt")
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

        }
    }


    public static class SessionIdKvFn extends DoFn<Session, KV<Integer, Session>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(KV.of(Objects.requireNonNull(c.element()).getId(), c.element()));
        }
    }

    public static class SessionWithSpeakersFn extends DoFn<KV<Integer, Iterable<Session>>, Session> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Iterable<Session> sessionIterable = Objects.requireNonNull(c.element()).getValue();

            Session session = SerializationUtils.clone(sessionIterable.iterator().next());
            List<Speaker> speakers = StreamSupport.stream(sessionIterable.spliterator(), false)
                    .flatMap(sessions -> sessions.getSpeakers().stream()).collect(Collectors.toList());
            session.setSpeakers(speakers);

            c.output(session);
        }
    }

    public static class SessionListCombineFn extends Combine.CombineFn<Session, List<Session>, List<Session>> {

        @Override
        public List<Session> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<Session> addInput(List<Session> mutableAccumulator, Session session) {

            Objects.requireNonNull(mutableAccumulator).add(session);
            return mutableAccumulator;
        }

        @Override
        public List<Session> mergeAccumulators(Iterable<List<Session>> accumulators) {
            List<Session> sessionMerge = createAccumulator();
            for (List<Session> sessions : accumulators) {
                sessionMerge.addAll(sessions);
            }
            return sessionMerge;
        }

        @Override
        public List<Session> extractOutput(List<Session> sessions) {
            return sessions;
        }
    }

    public static class MailMaker extends DoFn<List<Session>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            List<Session> sessions = c.element();
            assert sessions != null;
            String sessionSpeaker = sessions.stream()
                    .map(Session::toString).collect(Collectors.joining(" and "));
            String mail = EMAIL_TRANSFORM.mailTransform("[title of most viewed session]", sessionSpeaker,"emailData/testmail.txt");
            c.output(mail);
        }
    }


}

