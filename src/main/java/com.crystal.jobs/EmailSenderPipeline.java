package com.crystal.jobs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;

public class EmailSenderPipeline {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<Subscriber> subscribers = p.apply(
                JdbcIO.<Subscriber>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "com.mysql.cj.jdbc.Driver", "jdbc:mysql://hostname/database_name")
                                .withUsername("username")
                                .withPassword("password"))
                        .withQuery("SELECT name, email FROM subscribers s WHERE s.sented==1")
                        .withCoder(SerializableCoder.of(Subscriber.class))
                        .withRowMapper(new JdbcIO.RowMapper<Subscriber>() {
                            public Subscriber mapRow(ResultSet resultSet) throws Exception {
                                return new Subscriber(resultSet.getString(1), resultSet.getString(2));
                            }
                        }));

        subscribers.apply(ParDo.of(new DoFn<Subscriber, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Subscriber subscriber = c.element();
                // Send email to subscriber using their name and email address
                // ...
            }
        }));

        p.run();
    }
}


