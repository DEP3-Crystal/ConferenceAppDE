package com.crystal.jobs.pipeline_jobs;

import com.crystal.jobs.DTO.EventDTO;
import com.crystal.jobs.DTO.Participant;
import com.crystal.jobs.utils.JdbcConnector;
import com.crystal.jobs.utils.Log;
import com.crystal.jobs.utils.MailSender;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class EmailReminderOlderParticipantsForNewEvents {
    public interface EventRemainderOptions extends PipelineOptions {
        @Description("eventId")
        int getEventId();

        void setEventId(int eventId);
    }

    public static void main(String[] args) {
        EventRemainderOptions options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
                .as(EventRemainderOptions.class);
        options.setEventId(2);

        sentEmailRemainderOneDayBefore(options);

    }

    public static void sentEmailRemainderOneDayBefore(EventRemainderOptions options) {
        String selectEventQuery = "SELECT id as eventID ,title,description,start_day,end_day,location,capacity FROM conference.events e where e.id=" + options.getEventId();
        EventDTO eventDTO = null;
        try (Connection connection = JdbcConnector.getInstance().getDbConnection()) {
            ResultSet result;
            try (Statement statement = connection.createStatement()) {

                result = statement.executeQuery(selectEventQuery);
            }
            while (result.next()) {
                Long id = result.getLong("eventID");
                String title = result.getString("title");
                String description = result.getString("description");
                String startDay = result.getString("start_day");
                String endDay = result.getString("end_day");
                String location = result.getString("location");
                int capacity = result.getInt("capacity");

                eventDTO = new EventDTO(id, title, description,
                        LocalDate.parse(startDay, DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                        LocalDate.parse(endDay, DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                        location, capacity
                );
            }
        } catch (SQLException e) {
            Log.logInfo("Event id is not correct provided : " + e.getMessage());
        }


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

        PCollectionView<Iterable<Participant>> participantsView = participantDTOPCollection.apply(View.asIterable());

        participantDTOPCollection.apply("Count elements", Count.globally())

                .apply("check and sent emails", ParDo.of(new CheckAndSentEmails(eventDTO, participantsView))
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

    public static class CheckAndSentEmails extends DoFn<Long, Void> {
        private EventDTO eventDTO;
        private PCollectionView<Iterable<Participant>> participantsView;

        public CheckAndSentEmails(EventDTO eventDTO, PCollectionView<Iterable<Participant>> participantsView) {
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
                    MailSender.getInstance().sendMail(participant.getEmail(), "New Event Remainder", body);

                });

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

