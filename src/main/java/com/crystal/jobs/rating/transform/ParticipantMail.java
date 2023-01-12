package com.crystal.jobs.rating.transform;

import com.crystal.jobs.model.Participant;
import com.crystal.jobs.utils.MailSender;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.Serializable;

public class ParticipantMail extends DoFn<Participant, Void> implements Serializable {
    private final MailSender sender=MailSender.getInstance();
    @ProcessElement
    public void processElement(ProcessContext c) {
        Participant participant = c.element();
        sender.sendMail(participant.getEmail(),"Conference App",participant.getMailContent());
        c.output(null);
    }


}
