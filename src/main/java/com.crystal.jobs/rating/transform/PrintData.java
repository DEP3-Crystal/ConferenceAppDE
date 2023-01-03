package com.crystal.jobs.rating.transform;

import org.apache.beam.sdk.transforms.DoFn;

public class PrintData<T> extends DoFn<T,T> {

    @ProcessElement
    public void apply(ProcessContext c) {
        T element = c.element();
        System.out.println(element.toString());
        c.output(c.element());
    }
}

