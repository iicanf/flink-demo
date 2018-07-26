package com.iicanf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketStramHandler {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args) throws Exception {
        new SocketStramHandler().start();
    }

    public void start() throws Exception {
        logger.info("start job");
        final Bootstrap bootstrap = Bootstrap.getInstance();
//
        logger.info("start job success");

        StreamExecutionEnvironment env = bootstrap.createStreamExecutionEnvironment();

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
