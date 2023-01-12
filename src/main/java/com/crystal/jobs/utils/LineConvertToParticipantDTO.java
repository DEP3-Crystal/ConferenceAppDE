package com.crystal.jobs.utils;

import com.crystal.jobs.DTO.ParticipantDTO;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Objects;

public class LineConvertToParticipantDTO extends DoFn<String, ParticipantDTO> {
    private String header;

    public LineConvertToParticipantDTO(String header) {
        this.header = header;
    }

    @ProcessElement
   public void apply(ProcessContext c) {
        String[] line = Objects.requireNonNull(c.element()).split(",");
        if (line.length == 8) {
            if (!Objects.requireNonNull(c.element()).contains(header.substring(0, 4))) {
                c.output(new ParticipantDTO(
                                Integer.parseInt(line[0]),
                        Integer.parseInt(line[4]),
                                Integer.parseInt(line[5]),
                                line[6],
                        Integer.parseInt(line[7])
                        )
                );
            }
        }
        else {

            Log.logger.error("Line is not good format ".concat(Objects.requireNonNull(c.element())));
        }

    }

}
