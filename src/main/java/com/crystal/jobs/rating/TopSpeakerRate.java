package com.crystal.jobs.rating;

import com.crystal.jobs.model.Participant;
import com.crystal.jobs.model.Speaker;
import com.crystal.jobs.rating.transform.KeyValueFn;
import com.crystal.jobs.rating.transform.ParticipantMail;
import com.crystal.jobs.rating.transform.ReadParticipantDB;
import com.crystal.jobs.utils.EmailTransform;
import com.crystal.jobs.utils.FormatEmail;
import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TopSpeakerRate {

    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();
    private static final EmailTransform EMAIL_TRANSFORM = EmailTransform.getInstance();
    private static final FormatEmail FORMAT_EMAIL=FormatEmail.getInstance();
    private static final TupleTag<String> mailTupleTag = new TupleTag<>();
    private static final TupleTag<Participant> participantTupleTag = new TupleTag<>();

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<Integer, String>> speakerKv = pipeline.apply(new SpeakerFromDB())
                .apply(Combine.globally(new TopSpeakersFn()))
                .apply(ParDo.of(new SpeakerMailFn()))
                .apply(ParDo.of(new KeyValueFn<>()));

        PCollection<KV<Integer, Participant>> participantKv = pipeline.apply(new ReadParticipantDB())
                .apply(ParDo.of(new KeyValueFn<>()));

             KeyedPCollectionTuple.of(mailTupleTag, speakerKv)
                .and(participantTupleTag, participantKv)
                .apply(CoGroupByKey.create())
                     .apply(new ParticipantMailSender());


        pipeline.run().waitUntilFinish();
    }

    public static class SpeakerFromDB extends PTransform<PBegin, PCollection<Speaker>> {

        @Override
        public PCollection<Speaker> expand(PBegin input) {
            return input.apply(JDBC_CONNECTOR.<Speaker>databaseInit("/sqlScripts/speaker/TopSpeakerRate.txt")
                    .withRowMapper(resultSet -> {
                        String firstName = resultSet.getString(1);
                        String lastName = resultSet.getString(2);
                        return new Speaker(firstName, lastName);
                    })
                    .withCoder(SerializableCoder.of(Speaker.class)));
        }
    }


    private static class TopSpeakersFn extends Combine.CombineFn<Speaker, List<Speaker>, List<Speaker>> {

        @Override
        public List<Speaker> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<Speaker> addInput(List<Speaker> mutableAccumulator, Speaker input) {
            Objects.requireNonNull(mutableAccumulator).add(input);
            return mutableAccumulator;
        }

        @Override
        public List<Speaker> mergeAccumulators(Iterable<List<Speaker>> accumulators) {
            List<Speaker> speakerMerge = createAccumulator();
            for (List<Speaker> speakers : accumulators) {
                speakerMerge.addAll(speakers);
            }
            return speakerMerge;
        }

        @Override
        public List<Speaker> extractOutput(List<Speaker> accumulator) {
            return accumulator;
        }
    }

    private static class SpeakerMailFn extends DoFn<List<Speaker>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            List<Speaker> speakers = c.element();
            String speaker = Objects.requireNonNull(speakers).stream().map(Speaker::toString).collect(Collectors.joining(" and "));
            String mail = EMAIL_TRANSFORM.mailTransform("[title of most viewed session]",
                    speaker, "emailData/speakerMail.txt");
            c.output(mail);
        }
    }

    private static class ParticipantMailSender extends PTransform<PCollection<KV<Integer, CoGbkResult>>, PCollection<Void>> {

        @Override
        public PCollection<Void> expand(PCollection<KV<Integer, CoGbkResult>> mailParticipantJoin) {
            return mailParticipantJoin.apply(ParDo.of(new ParticipantMailFormat()))
                    .apply(ParDo.of(new ParticipantMail()));
        }
    }

    private static class ParticipantMailFormat extends DoFn<KV<Integer, CoGbkResult>, Participant> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String mail = Objects.requireNonNull(c.element()).getValue().getOnly(mailTupleTag);
            Iterable<Participant> participants = Objects.requireNonNull(c.element()).getValue().getAll(participantTupleTag);
            List<Participant> participantList = FORMAT_EMAIL.format(participants, EMAIL_TRANSFORM);
            participantList.forEach(participant -> participant.setMailContent(mail));
            participantList.forEach(c::output);
        }
    }
}