package p1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.ParameterTool;

import java.io.OutputStream;

import static org.apache.flink.table.api.Expressions.$;

public class FullOuterJoin {

    public static void main(String[] args) throws Exception {

        // Parse input arguments (required)
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String personPath = params.getRequired("person");
        final String locationPath = params.getRequired("location");
        final String outputPath = params.getRequired("output");

        // Set up batch execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Create TableEnvironment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // --- Read Person file ---
        FileSource<String> personSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(personPath))
                .build();

        DataStream<String> personLines = env.fromSource(personSource, WatermarkStrategy.noWatermarks(), "person-source");

        DataStream<Tuple2<Integer, String>> personStream = personLines.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String line) {
                String[] parts = line.split(",");
                return new Tuple2<>(Integer.parseInt(parts[0].trim()), parts[1].trim());
            }
        });

        // --- Read Location file ---
        FileSource<String> locationSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(locationPath))
                .build();

        DataStream<String> locationLines = env.fromSource(locationSource, WatermarkStrategy.noWatermarks(), "location-source");

        DataStream<Tuple2<Integer, String>> locationStream = locationLines.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String line) {
                String[] parts = line.split(",");
                return new Tuple2<>(Integer.parseInt(parts[0].trim()), parts[1].trim());
            }
        });

        // --- Convert DataStreams to Tables with meaningful column names ---
        Table personTable = tableEnv.fromDataStream(personStream, $("id"), $("name"));
        Table locationTable = tableEnv.fromDataStream(locationStream, $("id"), $("state"));

        // --- Perform inner join on 'id' column ---
        Table joinedTable = personTable.as("p_id", "p_name")
                .fullOuterJoin(locationTable.as("l_id","l_state"),$("p_id").isEqual($("l_id")))
                .select($("p_id"),$("p_name"),$("l_state"));


        DataStream<Row> joinedStream = tableEnv.toDataStream(joinedTable);

        final FileSink<Row> sink = FileSink.forRowFormat(
                        new Path(outputPath),
                        (Row row, OutputStream stream) -> {
                            String line = row.getField(0) + "," + row.getField(1) + "," + row.getField(2) + "\n";
                            stream.write(line.getBytes());
                        }
                )
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        joinedStream.sinkTo(sink);

        // Execute Flink batch job
        env.execute("Flink 2.0 Table API Inner Join Example");
    }
}

