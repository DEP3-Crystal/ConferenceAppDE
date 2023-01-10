package utils;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class BeamUtils {
    public static Pipeline createPipeline(String jobName) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName(jobName);
        // DirectRunner is the default one, so we don't need to specify it explicitly

        return Pipeline.create(options);
    }

    public static Pipeline createPipeline(String jobName, int workers) {
        DirectOptions options = PipelineOptionsFactory.create().as(DirectOptions.class);
        options.setJobName(jobName);
        options.setTargetParallelism(workers);
        // DirectRunner is the default one, so we don't need to specify it explicitly
        return Pipeline.create(options);
    }
}