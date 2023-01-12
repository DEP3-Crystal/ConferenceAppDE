package com.crystal.jobs.utils;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CsvDTOToDatabaseOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Validation.Required
    String getInputFileCSV();
    void setInputFileCSV(String value);
}