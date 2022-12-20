package com.crystal.jobs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

@Data
@Builder
@AllArgsConstructor
public class EmailInfoDTO implements Serializable {
    private String name;
    private String email;
    private String subject;

    private   String body ;
    private String conferenceName;
    private String sessionName;
    private Timestamp sessionStartDate;
public EmailInfoDTO(){

}
    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
    public void setBody() {
        this.body= "Hello Mrs/Mis " + name.substring(0,1).toUpperCase()+name.substring(1)
                + "\n We are remaindering   the "+ sessionName+"  session : "
                + " of the conference : " +conferenceName
                + "that  you have participate starts tomorrow at :" + new Date(sessionStartDate.getTime()).toLocalDate() + "\n " +
                "\n\n Hope see you there bye !";
    }


    @Override
    public String toString() {
        return "Subscriber{" +
                "name='" + name + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}


