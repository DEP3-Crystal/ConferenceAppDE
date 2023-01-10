package com.crystal.jobs.utils;

import com.crystal.jobs.DTO.UserDTO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.Objects;

public class ConvertUserDTOToKV extends DoFn<UserDTO, KV<String, UserDTO>> {
    @ProcessElement
    public void apply(ProcessContext c) {
        c.output(KV.of(Objects.requireNonNull(c.element()).getId(), c.element()));
    }

}
