package com.iicanf.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Splitter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketStramHandleer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args) throws Exception {
        new SocketStramHandleer().start();
    }

    public void start() throws Exception {
        logger.info("start job");
        final Bootstrap bootstrap = Bootstrap.getInstance();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<Tuple2<String, Integer>> dataStream = env
//                .socketTextStream("localhost", 9999)
//                .flatMap(new Splitter())
//                .keyBy(0)
//                .timeWindow(Time.seconds(5))
//                .sum(1);
//
//        dataStream.print();
//
//        env.execute("Window WordCount");
    }
}
