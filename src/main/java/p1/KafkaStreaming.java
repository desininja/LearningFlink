package p1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;


import java.io.OutputStream;

public class KafkaStreaming {


    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String outputPath ="/Users/himanshu/Desktop/LearningFlinkUdemy/output";
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("my-topic")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaData = env.fromSource(source, WatermarkStrategy.noWatermarks(),"Kafka Source");

        DataStream<Tuple2<String, Integer>> counts = kafkaData
                .flatMap(new WordCount.Tokenizer())
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
        env.execute("Kafka Streaming");
    }




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
