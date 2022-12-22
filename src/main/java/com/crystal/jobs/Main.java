package com.crystal.jobs;

import com.crystal.jobs.DTO.EmailInfoDTO;
import com.crystal.jobs.utils.EmailSender;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Main {
    public static void main(String[] args) {
        System.out.println("De App");

        try {
           new EmailSender().sentEmail(new EmailInfoDTO(
                    "stefan",
                    "stefanruci1997@gmail.com",
                    "Conference start remainder",
                    "ev_name",
                    new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse("2022-10-10").getTime()),
                    "session name",
                    LocalDateTime.parse("2022-10-11 12:11:55", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                    LocalDateTime.parse("2021-11-11 13:11:55", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            ));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
//        sentEmail(new EmailInfoDTO());
    }


}
