package com.crystal.jobs;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;


public class EmailSender {
//    private static final String SMTP_HOST = "smtp-relay.gmail.com";
//    private static final String SMTP_PORT = "465";
//    private static final String SENDER_EMAIL = "stefanruci2028@gmail.com";
//    private static final String SENDER_PASSWORD = "Helloworld1";

     private static final String  SMTP_HOST = "smtp.zoho.eu";
    private static final String SMTP_PORT = "465";
     private static   final String SENDER_EMAIL="info_conference_app@zohomail.eu";
     private static final String SENDER_PASSWORD="Hello19977";

    public EmailSender() {

    }

    public static  boolean sentEmail(EmailInfoDTO emailInfoDTO) {
        try {
            emailInfoDTO.setBody();
            // Set the email server properties
            Properties props = new Properties();
            props.setProperty("mail.pop3.socketFactory.class", "SSL_FACTORY");
            props.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
            props.setProperty("mail.smtp.socketFactory.fallback", "false");
            props.put("mail.smtp.host", SMTP_HOST);
            props.put("mail.smtp.port", SMTP_PORT);
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");

            // Create a session with the specified properties
            Session session = Session.getDefaultInstance(props,
                    new javax.mail.Authenticator() {
                        @Override
                        public PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(
                                    SENDER_EMAIL,
                                    SENDER_PASSWORD);
                        }
                    });

            // Create a message
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(SENDER_EMAIL));

            message.addRecipient(
                    Message.RecipientType.TO,
                    new InternetAddress(emailInfoDTO.getEmail()));
            message.setSubject(emailInfoDTO.getSubject());
            message.setText(emailInfoDTO.getBody());


            Transport transport = session.getTransport("smtp");
            transport.connect(SENDER_EMAIL, SENDER_PASSWORD);
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
        return true;
    }



}
