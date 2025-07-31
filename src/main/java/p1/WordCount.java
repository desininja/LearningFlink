package p1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;


import java.io.OutputStream;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // Parse arguments using ParameterTool
        final ParameterTool params = ParameterTool.fromArgs(args);
        // --- Hard-coded file paths ---
        final String inputPath = params.get("input","");                      //"/Users/himanshu/Desktop/LearningFlinkUdemy/inputFile.txt";
        final String outputPath = params.get("output","");                   //"/Users/himanshu/Desktop/LearningFlinkUdemy/output";


        // 1. Set up the environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // -----------------------------

        // Set execution mode to BATCH
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        // 3. Build a FileSource using the hard-coded input path.
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
                .build();

        // 4. Create the initial DataStream from the FileSource
        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "File Source");


        // 5. Filter the words that Starts with N
        DataStream<String> filter = text.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("N");
            }
        });
        // 6. Tokenize words, group by word, and sum counts
        DataStream<Tuple2<String, Integer>> counts = filter
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        // 7. Write to the hard-coded output path
        final FileSink<Tuple2<String, Integer>> sink = FileSink
                .forRowFormat(new Path(outputPath),
                        (Tuple2<String, Integer> element, OutputStream stream) -> {
                            stream.write((element.f0 + ": " + element.f1 + "\n").getBytes());
                        })
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        counts.sinkTo(sink);

        // 7. Execute the job
        env.execute("WordCount Flink 2.0");
    }

    /**
     * Implements the string tokenizer that splits sentences into words and converts them to (word, 1) tuples.
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.split("\\W+");

            // Emit the words as (word, 1) tuples
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
