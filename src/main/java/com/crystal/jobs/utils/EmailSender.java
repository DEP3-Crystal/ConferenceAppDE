package com.crystal.jobs.utils;

import com.crystal.jobs.DTO.EmailInfoDTO;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

@Data
public class EmailSender {
    private static EmailSender emailSender =new EmailSender();
    public static synchronized  EmailSender getInstance(){
        if (emailSender == null) {
            emailSender = new EmailSender();
        }
        return emailSender;
    }
    private  EmailSender (){}

    //    private static final String SMTP_HOST = "smtp.mailgun.org";
//    private static final String API_KEY = "eb38c18d-4cdcbdb6";
//
//    private static final String SMTP_PORT = "465";
//    private static final String SENDER_EMAIL = "brad@sandboxcc540cf61b54437abaa35b63836725e8.mailgun.org";
//    private static final String SENDER_PASSWORD = "d13c5611487dc094326a87b5355e6fb1-eb38c18d-d4161b95";
    private static final String SMTP_HOST = "smtp.mailfence.com";
    private static final String SMTP_PORT = "465";
    private static final String SENDER_EMAIL = "con-app@mailfence.com";
    private static final String SENDER_PASSWORD = "Hello19977!";


    public void sentEmail(EmailInfoDTO emailInfoDTO) {

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
            props.put("mail.smtp.ssl.enable", "true");

            // Create a session with the specified properties
            Session session = Session.getDefaultInstance(props,
                    new javax.mail.Authenticator() {
                        @Override
                        public PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(
                                    SENDER_EMAIL,
                                    SENDER_PASSWORD);
                        }
                    }
            );

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

            Log.logger.info("Email sent successfully");
            Log.logInfo("Email sent successfully");

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

    }

}
