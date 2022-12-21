package com.crystal.jobs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class EmailSenderPipeline {
    public static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String DB_URL = "jdbc:mysql://localhost:3306/conference";
    public static final String DB_USER_NAME = "root";
    public static final String DB_PASSWORD = "Shanti2022!";

    public static void main(String[] args) {

        String sendRemainderEmailToParticipantsForSessionOneDayBeforeStart = "SELECT \n" +
                "u.id as par_id , u.first_name as userName,u.email as userEmail,\n" +
                "e.id as event_id , e.title as ev_name,e.start_day as eventStartDay,\n" +
                "s.id as sesionId , s.title as sessionTitle ,s.start_time as sessionStartTime,s.end_time as sessionEndTime\n" +
                "FROM session s , participant_session ps ,events e,user u\n" +
                "where e.start_day<=(now()+interval 1 day)and s.event_id=e.id and ps.session_id=s.id  and ps.user_id=u.id;\n";

        Pipeline p = Pipeline.create();

        p.apply("read from db", JdbcIO.<EmailInfoDTO>read()
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
                                        new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(resultSet.getString("eventStartDay").toString()).getTime()),
                                        resultSet.getString(7),
                                        LocalDateTime.parse(resultSet.getString("sessionStartTime"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                        LocalDateTime.parse(resultSet.getString("sessionEndTime"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                                )))
                .apply("print to console", ParDo.of(new DoFn<EmailInfoDTO, Void>() {
                                                        @ProcessElement
                                                        public void processElement(ProcessContext c) {
                                                            try {
                                                                Thread.sleep(5000);
                                                            } catch (InterruptedException e) {
                                                                throw new RuntimeException(e);
                                                            }
                                                            new EmailSender().sentEmail(Objects.requireNonNull(c.element()));
//
//                                                            new EmailSenderPipeline().sentEmail();

                                                            try {
                                                                Thread.sleep(10000);
                                                            } catch (InterruptedException e) {
                                                                throw new RuntimeException(e);
                                                            }
                                                            System.out.println(
                                                                    c.element().getName()
                                                                            + ","
                                                                            + c.element().getEmail()
                                                                            + ","
                                                                            + c.element().getSubject()
                                                                            + ","
                                                                            + c.element().getConferenceName()
                                                                            + ","
                                                                            + c.element().getConferenceStartDay()
                                                                            + ","
                                                                            + c.element().getSessionName()
                                                                            + ","
                                                                            + c.element().getSessionStartDate()
                                                                            + ","
                                                                            + c.element().getSessionEndDate()

                                                            );


//                                                            System.out.println("c.element()");
//                                                                System.out.println(c.element());
//                                                                System.out.println(c.element());

                                                        }
                                                    }
                ));
        p.run().waitUntilFinish();


    }

//    public void sentEmail(EmailInfoDTO emailInfoDTO) {
//        EmailSender subscriberEmailSender = new EmailSender();
////        emailInfoDTO.setEmail("stefanruci1997@gmail.com");
////        emailInfoDTO.setSubject("remainder for sessions");
////        emailInfoDTO.setConferenceName("testing conference");
////        emailInfoDTO.setSessionName("Integration testing 1");
////        emailInfoDTO.setSessionStartDate(new Timestamp(System.currentTimeMillis() + 86400000L));
////        emailInfoDTO.setName("stefan");
//        emailInfoDTO.setBody();
//
//        subscriberEmailSender.sentEmail(emailInfoDTO);
//    }
}
