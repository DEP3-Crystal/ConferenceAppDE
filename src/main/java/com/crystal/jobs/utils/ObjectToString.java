package com.crystal.jobs.utils;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.Objects;

public class ObjectToString<T extends Object>  extends DoFn<T,String> {


    public ObjectToString() {
    }
    @ProcessElement
   public void  processElement(ProcessContext c){
        c.output(Objects.requireNonNull(c.element()).toString());
    }
}
