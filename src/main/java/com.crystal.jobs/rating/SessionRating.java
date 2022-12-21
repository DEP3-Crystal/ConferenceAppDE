package com.crystal.jobs.rating;


import com.crystal.jobs.model.Session;
import com.crystal.jobs.rating.transform.InsertInDB;
import com.crystal.jobs.rating.transform.ReadDataFRDB;
import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import java.util.Objects;

public class SessionRating {
    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();

    public interface SessionIdOption extends PipelineOptions {
        @Description("Path of th file to read from")
        int getSessionId();

        void setSessionId(int value);
    }

    public static void main(String[] args) {
        SessionIdOption options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SessionIdOption.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(new ReadDataFRDB(options.getSessionId(),"sqlScripts/session/session.txt"))
                .apply(ParDo.of(new SessionRateKVFN()))
                .apply(Mean.perKey())
                .apply(MapElements.via(new SimpleFunction<KV<Integer, Double>, Session>() {
                    @Override
                    public Session apply(KV<Integer, Double> input) {
                        return new Session(input.getKey(), input.getValue());
                    }
                }))
                .apply(new InsertInDB());
        pipeline.run();

    }

    public static class SessionRateKVFN extends DoFn<String, KV<Integer, Integer>> {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] data = Objects.requireNonNull(c.element()).split(",");
                if (data.length >= 3) {
                    int sessionId = Integer.parseInt(data[1]);
                    int rating = Integer.parseInt(data[2]);
                    c.output(KV.of(sessionId, rating));
                }
            }
        }




}