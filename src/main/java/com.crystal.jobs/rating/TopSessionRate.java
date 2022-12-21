package com.crystal.jobs.rating;

import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;

public class TopSessionRate {
    private static Double max = -9999D;
    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        try {
            pipeline.apply(JDBC_CONNECTOR.<KV<Integer, Double>>databaseInit("sqlScripts/session/SelectSessionRate.txt")
                            .withRowMapper((JdbcIO.RowMapper<KV<Integer, Double>>) resultSet -> {
                                int id = resultSet.getInt(1);
                                double rate = resultSet.getDouble(2);
                                return KV.of(id, rate);
                            })
                            .withCoder(KvCoder.of(SerializableCoder.of(Integer.class), SerializableCoder.of(Double.class))))
                    .apply(ParDo.of(new DoFn<KV<Integer, Double>, KV<Integer,Double>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            KV<Integer, Double> sessionRate = c.element();
                            if (max < sessionRate.getValue()) {
                                max = sessionRate.getValue();
                            }
                            c.output(c.element());
                        }
                    }))
                    .apply(ParDo.of(new DoFn<KV<Integer, Double>, KV<Integer, Double>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            if (c.element().getValue().doubleValue() == max) {
                                c.output(c.element());
                            }
                        }
                    }))
                    .apply(MapElements.via(new SimpleFunction<KV<Integer, Double>, Void>() {
                        @Override
                        public Void apply(KV<Integer, Double> input) {
                            System.out.println(input.getKey() + " " + input.getValue());
                            return null;
                        }
                    }));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        pipeline.run();
    }
}
