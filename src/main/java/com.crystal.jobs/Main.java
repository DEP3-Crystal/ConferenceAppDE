package com.crystal.jobs;

public class Main {
    public static void main(String[] args) {
        System.out.println("De App");

        EmailSender<Subscriber> subscriberEmailSender = new EmailSender<>(new Subscriber("name", "email"));
        subscriberEmailSender.sentEmail(subscriberEmailSender.getO().getEmail(), subscriberEmailSender.getO().getName());

    }
}
