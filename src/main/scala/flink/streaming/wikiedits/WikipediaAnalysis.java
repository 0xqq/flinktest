package flink.streaming.wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @Author:chenchen
 * @Description:
 * @Date:2018/10/17
 * @Project:flinktest
 * @Package:flink.streaming.wikiedits
 */
public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception{
        //1、创建执行环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建数据源读取源数据
        DataStreamSource<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
        //3、确定流数据（k-v)的 key 值
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent event) throws Exception {
                return event.getUser();
            }
        });
        //4、数据分析
        DataStream<Tuple2<String, Long>> result = keyedEdits.timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) throws Exception {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });
        result.map(new MapFunction<Tuple2<String,Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> tuple) throws Exception {
                return tuple.toString();
            }
        }).addSink(new FlinkKafkaProducer08<String>("localhost:9092", "mywikiedits", new SimpleStringSchema()));


        see.execute();

    }
}
