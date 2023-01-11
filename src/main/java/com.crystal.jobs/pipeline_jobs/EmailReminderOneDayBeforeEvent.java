package com.crystal.jobs.pipeline_jobs;


import com.crystal.jobs.DTO.EmailInfoDTO;
import com.crystal.jobs.utils.JdbcConnector;
import com.crystal.jobs.utils.Log;
import com.crystal.jobs.utils.MailSender;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class EmailReminderOneDayBeforeEvent {
    private EmailReminderOneDayBeforeEvent() {
    }

    private static EmailReminderOneDayBeforeEvent INSTANCE;

    public static synchronized EmailReminderOneDayBeforeEvent getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new EmailReminderOneDayBeforeEvent();
        }
        return INSTANCE;
    }


    public static void main(String[] args) {
        sentEmailRemainderOneDayBefore();

    }

    private static EmailInfoDTO mapRow(ResultSet resultSet) throws SQLException, ParseException {
        return new EmailInfoDTO(
                resultSet.getString("userName"),
                resultSet.getString("userEmail"),
                "Conference start remainder",
                resultSet.getString("ev_name"),
                new Date(new SimpleDateFormat("yyyy-MM-dd").parse(resultSet.getString("eventStartDay")).getTime()),
                resultSet.getString(7),
                LocalDateTime.parse(resultSet.getString("sessionStartTime"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                LocalDateTime.parse(resultSet.getString("sessionEndTime"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        );
    }

    public static void sentEmailRemainderOneDayBefore() {
        String sendRemainderEmailToParticipantsForSessionOneDayBeforeStart =
                "SELECT \n" +
                        "u.id as par_id , u.first_name as userName,u.email as userEmail,\n" +
                        "e.id as event_id , e.title as ev_name,e.start_day as eventStartDay,\n" +
                        "s.id as sessionId , s.title as sessionTitle ,s.start_time as sessionStartTime,s.end_time as sessionEndTime\n" +
                        "FROM session s , participant_session ps ,events e,user u\n" +
                        "where e.start_day<=(now()+interval 1 day) and s.event_id=e.id and ps.session_id=s.id  and ps.user_id=u.id;\n";

        Pipeline pipeline = Pipeline.create();

        PCollection<EmailInfoDTO> emailInfoDTOPCollection = pipeline.apply("read from db", JdbcIO.<EmailInfoDTO>read()
                .withDataSourceConfiguration(JdbcConnector.getInstance().getDB_SOURCE_CONFIGURATION())
                .withQuery(sendRemainderEmailToParticipantsForSessionOneDayBeforeStart)
                .withCoder(SerializableCoder.of(TypeDescriptor.of(EmailInfoDTO.class)))
                .withRowMapper(
                        (RowMapper<EmailInfoDTO>) EmailReminderOneDayBeforeEvent::mapRow
                )
        );
        PCollectionView<Iterable<EmailInfoDTO>> emails = emailInfoDTOPCollection.apply("Convert ", View.asIterable());

        emailInfoDTOPCollection.apply("Count elements", Count.globally())
                .apply("Sent emails", ParDo.of(new DoFn<Long, Void>() {
                                                   @ProcessElement
                                                   public void processElement(ProcessContext c) {
                                                       if (c.element() > 0) {
                                                           c.sideInput(emails)
                                                                   .forEach(element -> {
                                                                       Log.logInfo(element.getEmailTo() + "");
                                                                       MailSender.getInstance().sendMail(element.getEmailTo(), element.getSubject(), element.getBody());
                                                                   });

                                                       } else {
                                                           Log.logInfo("PCollection is empty tomorrow don't have any event ");
                                                       }

                                                   }
                                               }
                        ).withSideInputs(emails)
                );

//        emailInfoDTOPCollection.apply(ParDo.of(new ObjectToString<>()))
//                .apply(TextIO.write().to(String.valueOf(EmailReminderOneDayBeforeEvent.class.getResource("")).concat("emailDTO"))
//                        .withoutSharding()
//                        .withSuffix(".csv"));


        pipeline.run().waitUntilFinish();

    }
}
