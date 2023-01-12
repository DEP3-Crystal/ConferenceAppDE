package com.crystal.jobs.rating.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class KeyValueFn<T> extends DoFn<T, KV<Integer,T>> implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output(KV.of(1,c.element()));
    }

}
