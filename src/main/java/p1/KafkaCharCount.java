package p1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class KafkaCharCount {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // The output path where Flink will write the results.
        // Flink creates subdirectories and part files within this path.
        final String outputPath ="/Users/himanshu/Desktop/LearningFlinkUdemy/output";
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // It's a good practice to set a checkpoint interval for production jobs.
        // This makes the job stateful and fault-tolerant.
        env.enableCheckpointing(1000); // Checkpoint every 1 second

        // Configure the Kafka source to read from our local broker.
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("my-topic")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a DataStream from the Kafka source.
        DataStream<String> kafkaData = env.fromSource(source, WatermarkStrategy.noWatermarks(),"Kafka Source");

        // The main logic: Count the characters of each incoming string.
        DataStream<Tuple2<String, Integer>> characterCounts = kafkaData
                .map(new CharacterCounterWithTuple());

        // Configure the file sink to write the character counts to the output path.
        // The output will be formatted as "message count".
        final FileSink<Tuple2<String, Integer>> sink = FileSink
                .forRowFormat(new Path(outputPath),
                        (Tuple2<String, Integer> element, OutputStream stream) -> {
                            stream.write((element.f0 + " " + element.f1 + "\n").getBytes(StandardCharsets.UTF_8));
                        })
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        // Connect the character count stream to the file sink.
        characterCounts.sinkTo(sink);

        // Execute the Flink job.
        env.execute("Kafka Streaming Character Count");
    }

    // A simple MapFunction that takes a string and outputs a Tuple2
    // containing the original string and its character count.
    public static class CharacterCounterWithTuple implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String value) {
            // The length of the string is the character count.
            return new Tuple2<>(value, value.length());
        }
    }
}
