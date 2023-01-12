package com.crystal.jobs.rating;

import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class SpeakerRating {
    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();

    public interface SpeakerIdSession extends PipelineOptions {
        @Description("Path of th file to read from")
        int getSpeakerId();

        void setSpeakerId(int value);
    }

    public static void main(String[] args) {
        SpeakerIdSession options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(SpeakerIdSession.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(new ReadDataFRDB(options.getSpeakerId()))
                .apply(ParDo.of(new SpeakerKVFn()))
                .apply(Mean.perKey())
                .apply(new InsertSpeakerDB());
        pipeline.run().waitUntilFinish();

    }

    private static class SpeakerKVFn extends DoFn<String, KV<Integer, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] speakerData = c.element().split(",");
            if (speakerData.length == 2) {
                int speakerId = Integer.parseInt(speakerData[0]);
                int speakerRate = Integer.parseInt(speakerData[1]);
                c.output(KV.of(speakerId, speakerRate));
            }
        }
    }

    public static class ReadDataFRDB extends PTransform<PBegin, PCollection<String>> {
        private final int id;

        public ReadDataFRDB(int id) {
            this.id = id;
        }

        @Override
        public PCollection<String> expand(PBegin input) {
            return input.apply(JDBC_CONNECTOR.<String>databaseInit("/sqlScripts/speaker/SelectSpeaker.txt")
                    .withCoder(StringUtf8Coder.of())
                    .withStatementPreparator((JdbcIO.StatementPreparator) preparedStatement ->
                            preparedStatement.setInt(1, id))
                    .withRowMapper((JdbcIO.RowMapper<String>) resultSet ->
                            resultSet.getString(1) + "," + resultSet.getString(2)));

        }
    }

    private static class InsertSpeakerDB extends PTransform<PCollection<KV<Integer, Double>>, PDone> {
        @Override
        public PDone expand(PCollection<KV<Integer, Double>> input) {
            return input.apply(JDBC_CONNECTOR.<KV<Integer, Double>>databaseWrite("/sqlScripts/speaker/InsertSpeaker.txt")
                    .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<KV<Integer, Double>>) (element, preparedStatement) -> {
                        preparedStatement.setInt(2, element.getKey());
                        preparedStatement.setDouble(1, element.getValue());
                    })
            );

        }
    }
}
