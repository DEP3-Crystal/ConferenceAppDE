package com.crystal.jobs;

public class EmailSender <O>{
    private O o ;


    public EmailSender(O o) {
        this.o = o;
    }
    public   boolean sentEmail(String email,String body){
        //todo : implement sentEmail method
        System.out.println( "sented");
        return  true;
    }

    public O getO() {
        return o;
    }
}
