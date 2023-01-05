package com.crystal.jobs.DTO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDateTime;
//import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@DefaultCoder(AvroCoder.class)
public class EmailInfoDTO
        implements Serializable {
    private String name;
    private String email;
    private String subject;
    private String body;
    private String conferenceName;
    private Date conferenceStartDay;
    private String sessionName;
    private LocalDateTime sessionStartDate;
    private LocalDateTime sessionEndDate;

    public EmailInfoDTO() {

    }

    public EmailInfoDTO(String name, String email, String subject, String conferenceName, Date conferenceStartDay, String sessionName, LocalDateTime sessionStartDate, LocalDateTime sessionEndDate) {
        this.name = name;
        this.email = email;
        this.subject = subject;
        this.conferenceName = conferenceName;
        this.conferenceStartDay = conferenceStartDay;
        this.sessionName = sessionName;
        this.sessionStartDate = sessionStartDate;
        this.sessionEndDate = sessionEndDate;
        setBody();
    }

    public void setName(String name) {
        this.name = name;
    }

//    public String getEmail() {
//        return email;
//    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setBody() {
        this.body = "Hello Mrs/Mis "
                + name.substring(0, 1).toUpperCase()
                + name.substring(1)
                + "\n We are remaindering   for the   the conference : "
                + conferenceName
                + "and  session : "
                + sessionName
                + "that you have participate starts tomorrow at :"
                + conferenceStartDay
                + "\n \n\n Hope see you there bye !";
    }


}


