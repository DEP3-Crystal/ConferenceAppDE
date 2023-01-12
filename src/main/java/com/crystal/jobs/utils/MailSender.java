package com.crystal.jobs.utils;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

import java.io.Serializable;

public class MailSender implements Serializable {

    private static MailSender INSTANCE = new MailSender();

    private MailSender() {

    }

    public static synchronized MailSender getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new MailSender();
        }
        return INSTANCE;
    }


    public void sendMail(String participant, String subject, String content) {

        try {
            Email email = new SimpleEmail();
            email.setSubject(subject);
            email.addTo(participant);
            email.setFrom("lukabuziu42@gmail.com");
            email.setMsg(content);
            email.setHostName("smtp.gmail.com");
            email.setAuthenticator(new DefaultAuthenticator("lukabuziu42@gmail.com", "eydqycoahsuwrugh"));
            email.setTLS(true);
            email.send();
        } catch (EmailException e) {
            e.printStackTrace();
        }
    }

}
