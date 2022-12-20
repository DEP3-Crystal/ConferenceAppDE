package com.crystal.jobs;

import org.apache.beam.sdk.Pipeline;

import java.sql.Timestamp;

public class EmailSenderPipeline {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
//        PCollection<EmailInfoDTO> subscribers = p.apply(
//                JdbcIO.<EmailInfoDTO>read()
//                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
//                                        "com.mysql.cj.jdbc.Driver",
//                                        "jdbc:mysql://hostname/database_name")
//                                .withUsername("username")
//                                .withPassword("password"))
//                        .withQuery("SELECT name, email FROM subscribers s WHERE s.sented==1")
//                        .withCoder(SerializableCoder.of(EmailInfoDTO.class))
//                        .withRowMapper((JdbcIO.RowMapper<EmailInfoDTO>) resultSet -> new EmailInfoDTO(resultSet.getString(1), resultSet.getString(2))));

//        subscribers.apply(ParDo.of(new DoFn<EmailInfoDTO, Void>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                EmailInfoDTO emailInfoDTO = c.element();
//
//                // Send email to subscriber using their name and email address
       EmailInfoDTO emailInfoDTO=new EmailInfoDTO();
        sentEmail(emailInfoDTO);
//            }
//        }));
//
//        p.run();
    }

    static void sentEmail(EmailInfoDTO emailInfoDTO) {
        EmailSender subscriberEmailSender = new EmailSender();
        emailInfoDTO.setEmail("stefanruci1997@gmail.com");
        emailInfoDTO.setSubject("remainder for sessions");
        emailInfoDTO.setConferenceName("testing conference");
        emailInfoDTO.setSessionName("Integration testing 1");
        emailInfoDTO.setSessionStartDate(new Timestamp(System.currentTimeMillis() + 86400000L));
        emailInfoDTO.setName("stefan");
        emailInfoDTO.setBody();

        subscriberEmailSender.sentEmail(emailInfoDTO);
    }
}


