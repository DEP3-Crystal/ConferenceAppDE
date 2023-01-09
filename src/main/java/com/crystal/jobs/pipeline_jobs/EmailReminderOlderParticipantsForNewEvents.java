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
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;

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
        new EmailReminderOlderParticipantsForNewEvents().sentEmailRemainderOneDayBefore(options);
    }

    public void sentEmailRemainderOneDayBefore(EventRemainderOptions options) {
        EventDTO eventDTo = options.getEvent();
        String selectAllParticipants =
                "SELECT \n"
                        + "u.id as user_id , u.first_name as userName, u.last_name as last_Name,u.email as userEmail,\n"
                        + "FROM user u ,participants p"
                        + "where  p.user_id=u.id;";

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Participant> participantDTOPCollection = pipeline.apply("read participants from db", JdbcIO.<Participant>read()
                .withDataSourceConfiguration(JdbcConnector.getInstance().getDB_SOURCE_CONFIGURATION())
                .withQuery(selectAllParticipants)
                .withCoder(SerializableCoder.of(TypeDescriptor.of(Participant.class)))
                .withRowMapper(
                        (JdbcIO.RowMapper<Participant>) EmailReminderOlderParticipantsForNewEvents::participantMapRow
                )
        );

        List<EventDTO> events = Collections.singletonList(eventDTo);
        PCollectionView<Iterable<Participant>> olderParticipants = participantDTOPCollection.apply(View.asIterable());
        PCollectionView<EventDTO> newEventDTO = pipeline.apply(Create.of(events)).apply(View.asSingleton());

        participantDTOPCollection.apply("Count elements", Count.globally())
                .apply("print to console", ParDo.of(new DoFn<Long, Void>() {
                                                        @ProcessElement
                                                        public void processElement(ProcessContext c) {
                                                            if (c.element() > 0) {
                                                                EventDTO eventDTO = events.get(0);
                                                                c.sideInput(olderParticipants)
                                                                        .forEach(element -> {
                                                                            Log.logInfo(element + "");
                                                                            String body = "Hello Ms/Mrs " + element.getName() + " \n " +
                                                                                    "I wanted to let you know about an exciting new event that we're hosting  on " + eventDTO.getLocation()
                                                                                    + " at " + eventDTO.getStartDay() + " the "
                                                                                    + eventDTO.getTitle()
                                                                                    + " will be a  event featuring  " + eventDTO.getDescription()
                                                                                    + ".\n\n The event will take place on [date] at [location]. We hope to see you there! \n"
                                                                                    + "\n If you're interested in attending, you can purchase tickets through our website or by contacting us at info@conferenceapp.org.\n"
                                                                                    + "\n We hope to see you at the event!\n"
                                                                                    + "\n Best regards,\n" +
                                                                                    "CMS APP";
                                                                            MailSender.getInstance().sendMail(element.getEmail(), "New Event Remainder", body);
                                                                        });

                                                            } else {
                                                                Log.logInfo("PCollection is empty tomorrow don't have any participant ");
                                                            }

                                                        }
                                                    }
                        ).withSideInputs(olderParticipants, newEventDTO)
                );


        pipeline.run().waitUntilFinish();

    }

    private static Participant participantMapRow(ResultSet resultSet) throws SQLException, ParseException {
        return new Participant(
                resultSet.getString(1),
                resultSet.getString(2),
                resultSet.getString(3),
                resultSet.getString(4)
        );
    }
}
