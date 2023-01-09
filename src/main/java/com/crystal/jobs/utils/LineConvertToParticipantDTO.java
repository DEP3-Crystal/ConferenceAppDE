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
    void apply(ProcessContext c) {
        String[] line = Objects.requireNonNull(c.element()).split(",");
        if (line.length == 8) {
            if (!Objects.requireNonNull(c.element()).contains(header.substring(0, 3))) {
                c.output(new ParticipantDTO(
                                line[0],
                                line[1],
                                line[2],
                                line[3],
                                line[4],
                                Integer.parseInt(line[5]),
                                line[6],
                                line[7]
                        )
                );
            }
        }
        else {

            Log.logger.error("Line is not good format ".concat(Objects.requireNonNull(c.element())));
        }

    }

}
