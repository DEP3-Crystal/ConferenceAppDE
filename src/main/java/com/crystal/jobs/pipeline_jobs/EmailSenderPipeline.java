package com.crystal.jobs.pipeline_jobs;


import com.crystal.jobs.DTO.EmailInfoDTO;
import com.crystal.jobs.DTO.SizeOfCollection;
import com.crystal.jobs.utils.Log;
import com.crystal.jobs.utils.MailSender;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class EmailSenderPipeline {
    public static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String DB_URL = "jdbc:mysql://localhost:3306/conference";
    public static final String DB_USER_NAME = "root";
    public static final String DB_PASSWORD = "Shanti2022!";
    public static final SizeOfCollection SIZE = new SizeOfCollection();

    public static void main(String[] args) {

        String sendRemainderEmailToParticipantsForSessionOneDayBeforeStart =
                "SELECT \n" +
                        "u.id as par_id , u.first_name as userName,u.email as userEmail,\n" +
                        "e.id as event_id , e.title as ev_name,e.start_day as eventStartDay,\n" +
                        "s.id as sessionId , s.title as sessionTitle ,s.start_time as sessionStartTime,s.end_time as sessionEndTime\n" +
                        "FROM session s , participant_session ps ,events e,user u\n" +
                        "where e.start_day>=(now()+interval 1 day)and s.event_id=e.id and ps.session_id=s.id  and ps.user_id=u.id;\n";

        Pipeline p = Pipeline.create();

        PCollection<EmailInfoDTO> emailInfoDTOPCollection = p.apply("read from db", JdbcIO.<EmailInfoDTO>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                DRIVER_CLASS_NAME, DB_URL)
                        .withUsername(DB_USER_NAME)
                        .withPassword(DB_PASSWORD))
                .withQuery(sendRemainderEmailToParticipantsForSessionOneDayBeforeStart)
                .withCoder(SerializableCoder.of(EmailInfoDTO.class))
                .withRowMapper(
                        (JdbcIO.RowMapper<EmailInfoDTO>) resultSet -> new EmailInfoDTO(
                                resultSet.getString("userName"),
                                resultSet.getString("userEmail"),
                                "Conference start remainder",
                                resultSet.getString("ev_name"),
                                new Date(new SimpleDateFormat("yyyy-MM-dd").parse(resultSet.getString("eventStartDay").toString()).getTime()),
                                resultSet.getString(7),
                                LocalDateTime.parse(resultSet.getString("sessionStartTime"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                LocalDateTime.parse(resultSet.getString("sessionEndTime"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        )
                )
        );
        PCollectionView<Iterable<EmailInfoDTO>> emails = emailInfoDTOPCollection.apply(View.asIterable());

        emailInfoDTOPCollection.apply(Count.globally())
                .apply("print to console", ParDo.of(new DoFn<Long, Void>() {
                                                        @ProcessElement
                                                        public void processElement(ProcessContext c) {
                                                            if (c.element() > 0) {
                                                                c.sideInput(emails)
                                                                        .forEach(element -> {
                                                                            Log.logInfo(element + "");
                                                                            MailSender.getInstance().sendMail(element.getEmail(), element.getSubject(), element.getBody());
                                                                        });

                                                            } else Log.logInfo("PCollection is empty");

                                                        }
                                                    }
                        ).withSideInputs(emails)
                );


        p.run().waitUntilFinish();


    }
}
