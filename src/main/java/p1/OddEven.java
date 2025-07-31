package p1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OddEven {


    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        final String evenOutputPath = "/Users/himanshu/Desktop/LearningFlinkUdemy/even_numbers";
        final String oddOutputPath = "/Users/himanshu/Desktop/LearningFlinkUdemy/odd_numbers";

        DataStream<String> number = env.socketTextStream("127.0.0.1",9998);

        final OutputTag<String> oddOutputTag = new OutputTag<String>("odd-numbers"){};

        SingleOutputStreamOperator<String> evenStream = number.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                try {
                    int num = Integer.parseInt(value.trim());

                    if (num %2==0){
                        out.collect(value);
                    }else{
                        ctx.output(oddOutputTag, value);
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid input, not a number: " + value);
                }
            }
        });

        DataStream<String> oddStream = evenStream.getSideOutput(oddOutputTag);


        final FileSink<String> evenSink = FileSink.forRowFormat(new Path(evenOutputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        final FileSink<String> oddSink = FileSink.forRowFormat(new Path(oddOutputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        evenStream.sinkTo(evenSink).name("EvenNumberSink");
        oddStream.sinkTo(oddSink).name("OddNumberSink");

        env.execute("Odd Even Splitter Job");

    }
}
