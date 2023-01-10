package com.crystal.jobs.pipeline_jobs;

import com.crystal.jobs.DTO.EventDTO;
import com.crystal.jobs.DTO.Participant;
import com.crystal.jobs.utils.JdbcConnector;
import com.crystal.jobs.utils.Log;
import com.crystal.jobs.utils.MailSender;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class EmailReminderOlderParticipantsForNewEvents {
    public interface EventRemainderOptions extends PipelineOptions {
        @Description("My Class")
        @Default.Class(EventDTO.class)
        EventDTO getEvent();

        void setEvent(EventDTO myClass);
    }

    public static void main(String[] args) {
        EventRemainderOptions options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
                .as(EventRemainderOptions.class);

        EventDTO eventDTO = new EventDTO(
                5L,
                "tittle",
                "decr ",
                LocalDate.parse("2023-01-22", DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                LocalDate.parse("2023-01-25", DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                "location",
                200);
        options.setEvent(eventDTO);

        sentEmailRemainderOneDayBefore(options);

    }

    public static void sentEmailRemainderOneDayBefore(EventRemainderOptions options) {
        EventDTO eventDTO = options.getEvent();

        String selectAllParticipantsQuery =
                "select  u.id as user_id , u.first_name as userName, u.last_name as last_Name,u.email as userEmail\n"
                        + "from conference.user u ,conference.participant p\n"
                        + "where  p.id=u.id";

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Participant> participantDTOPCollection = pipeline.apply("read participants from db", JdbcIO.<Participant>read()
                .withDataSourceConfiguration(JdbcConnector.getInstance().getDB_SOURCE_CONFIGURATION())
                .withQuery(selectAllParticipantsQuery)
                .withCoder(SerializableCoder.of(TypeDescriptor.of(Participant.class)))
                .withRowMapper(
                        (JdbcIO.RowMapper<Participant>) EmailReminderOlderParticipantsForNewEvents::participantMapRow
                )
        );

//        PCollectionView<Iterable<Participant>> olderParticipants = participantDTOPCollection.apply(View.asIterable());
        PCollectionView<Iterable<Participant>> participantsView = participantDTOPCollection.apply(View.asIterable());

        participantDTOPCollection.apply("Count elements", Count.globally())

                .apply("print to console", ParDo.of(new CheckIfHaveRows(eventDTO, participantsView))
                        .withSideInputs(participantsView)
                );


        pipeline.run().waitUntilFinish();

    }


    public static Participant participantMapRow(ResultSet resultSet) throws SQLException {
        return new Participant(
                resultSet.getString(1),
                resultSet.getString(2),
                resultSet.getString(3),
                resultSet.getString(4)
        );
    }

    public static class CheckIfHaveRows extends DoFn<Long, Void> {
        private EventDTO eventDTO;
        private PCollectionView<Iterable<Participant>> participantsView;

        public CheckIfHaveRows(EventDTO eventDTO, PCollectionView<Iterable<Participant>> participantsView) {
            this.eventDTO = eventDTO;
            this.participantsView = participantsView;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {

            if (c.element() > 0) {
                Iterable<Participant> participants = c.sideInput(participantsView);

                participants.forEach(participant -> {

                    Log.logInfo(participant.getEmail() + "");

                    String body = "Hello Ms/Mrs " + participant.getName() + " \n " +
                            "I wanted to let you know about an exciting new event that we're hosting  on " + eventDTO.getLocation()
                            + " at " + eventDTO.getStartDay() + " the "
                            + eventDTO.getTitle()
                            + " will be a  event featuring  " + eventDTO.getDescription()
                            + ".\n\n The event will take place on [date] at [location]. We hope to see you there! \n"
                            + "\n If you're interested in attending, you can purchase tickets through our website or by contacting us at info@conferenceapp.org.\n"
                            + "\n We hope to see you at the event!\n"
                            + "\n Best regards,\n" +
                            "CMS APP";


//                    MailSender.getInstance().sendMail(participant.getEmail(), "New Event Remainder", body);

                });


//                        ParDo.of(new SentEmails(eventDTO));


            } else {
                Log.logInfo("PCollection is empty tomorrow don't have any participant ");
            }

        }
    }

    public static class SentEmails extends DoFn<Participant, Void> {
        private EventDTO eventDTO;

        SentEmails(EventDTO eventDTO) {
            this.eventDTO = eventDTO;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Log.logInfo(c.element() + "");

            String body = "Hello Ms/Mrs " + c.element().getName() + " \n " +
                    "I wanted to let you know about an exciting new event that we're hosting  on " + eventDTO.getLocation()
                    + " at " + eventDTO.getStartDay() + " the "
                    + eventDTO.getTitle()
                    + " will be a  event featuring  " + eventDTO.getDescription()
                    + ".\n\n The event will take place on [date] at [location]. We hope to see you there! \n"
                    + "\n If you're interested in attending, you can purchase tickets through our website or by contacting us at info@conferenceapp.org.\n"
                    + "\n We hope to see you at the event!\n"
                    + "\n Best regards,\n" +
                    "CMS APP";


            MailSender.getInstance().sendMail(Objects.requireNonNull(c.element()).getEmail(), "New Event Remainder", body);
        }


    }
}

