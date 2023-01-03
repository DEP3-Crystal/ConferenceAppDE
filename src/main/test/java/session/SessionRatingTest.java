package session;

import com.crystal.jobs.utils.JdbcConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import utils.BeamUtils;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SessionRatingTest implements Serializable {

    private static final JdbcConnector JDBC_CONNECTOR = JdbcConnector.getInstance();


    @Test
    public void testRating() throws IOException, SQLException {

        final Instant NOW = new Instant(0);
        final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));
        Instant sec1Duration = NOW.plus(Duration.standardSeconds(1));
        Instant sec2Duration = NOW.plus(Duration.standardSeconds(2));
        Instant sec6Duration = NOW.plus(Duration.standardSeconds(6));
        Pipeline pipeline = BeamUtils.createPipeline("CreateSessionRating");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
                .addElements(TimestampedValue.of("21", SEC_1_DURATION))
                .addElements(TimestampedValue.of("21", SEC_1_DURATION))
                .addElements(TimestampedValue.of("22", SEC_1_DURATION))
                .addElements(TimestampedValue.of("23", SEC_1_DURATION))
                .addElements(TimestampedValue.of("24", SEC_1_DURATION))
                .addElements(TimestampedValue.of("24", SEC_1_DURATION))
//                .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(7)))
//                .advanceProcessingTime(Duration.standardSeconds(9))
//                .addElements(TimestampedValue.of("21", sec6Duration))
                .advanceWatermarkToInfinity();


        pipeline.apply(onTimeLetters)
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane())
                                .withLateFirings(AfterPane.elementCountAtLeast(1))
                        ).withAllowedLateness(Duration.standardSeconds(5))
                        .accumulatingFiredPanes())
                .apply(MapElements.via(new SimpleFunction<String, KV<String, Integer>>() {
                    @Override
                    public KV<String, Integer> apply(String integer) {
                        return KV.of(integer, 1);
                    }
                }))
                .apply(Count.perKey())
                .apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element().getKey());
                        c.output(c.element().getKey());
                    }
                }))
                .apply(Combine.globally(new SessionIdList()).withoutDefaults())
                .apply(JdbcIO.<List<Integer>, String>readAll()
                                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                        .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/conference")
                                        .withUsername("root")
                                        .withPassword("Shanti2022!"))
                                .withQuery("SELECT * FROM participant_session  where session_id in (?);")
                                .withParameterSetter(((element, preparedStatement) -> {
                                    String string = element.stream().map(Object::toString).collect(Collectors.joining("','", "", ""));
                    preparedStatement.setString(1, string);
//                                    System.out.println(element);
//                                    Integer[] objects = element.toArray(Integer[]::new);
//                                    preparedStatement.setArray(1, preparedStatement.getConnection()
//                                            .createArrayOf("VARCHAR", objects));
                                System.out.println(preparedStatement);
                                }))
                                .withRowMapper((JdbcIO.RowMapper<String>) resultSet ->
                                        resultSet.getString(1) + "," + resultSet.getString(2)
                                                + "," + resultSet.getString(3))
                                .withCoder(StringUtf8Coder.of())
                ).apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element());
                        c.output(c.element());
                    }
                }));


        pipeline.run();
    }

    public static class PrintData<T> extends DoFn<KV<T, T>, T> {

        @ProcessElement
        public void apply(ProcessContext c) {
            KV<T, T> element = c.element();
            System.out.println(element);
            c.output(c.element().getKey());
        }
    }

    public class SessionIdList extends Combine.CombineFn<String, List<String>, List<Integer>> implements Serializable {

        @Override
        public List<String> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<String> addInput(List<String> mutableAccumulator, String input) {
            mutableAccumulator.add(input);
            return mutableAccumulator;
        }

        @Override
        public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
            List<String> result = new ArrayList<>();
            for (List<String> list : accumulators) {
                result.addAll(list);
            }
            return result;
        }

        @Override
        public List<Integer> extractOutput(List<String> accumulator) {
            List<Integer> collect = accumulator.stream().map(Integer::parseInt).collect(Collectors.toList());
            return collect;
        }
    }
}
