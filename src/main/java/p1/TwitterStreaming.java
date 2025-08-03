//package p1;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.twitter.TwitterSource;
//
//import javax.xml.transform.Source;
//import java.util.Properties;
//
//public class TwitterStreaming {
//    public static void main(String[] args) throws Exception{
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        Properties twitterCredentials = new Properties();
//        twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "");
//        twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "");
//        twitterCredentials.setProperty(TwitterSource.TOKEN, "");
//        twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "");
//
//        Source mySource = new MySource(new TwitterSource(twitterCredentials));
//
//        DataStream<String> twitterData = env.fromSource(mySource, WatermarkStrategy.noWatermarks(),"TwitterSource");
//
//
//
//    }
//}
