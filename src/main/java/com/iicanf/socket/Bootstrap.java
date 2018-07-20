/*
 * Copyright 2017 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iicanf.socket;

import com.iicanf.socket.config.FlinkConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author minwoo.jung
 */
public class Bootstrap {

    private final static Bootstrap INSTANCE = new Bootstrap();


    private final ClassPathXmlApplicationContext applicationContext;
    private final FlinkConfiguration flinkConfiguration;


    private Bootstrap() {
        String[] SPRING_CONFIG_XML = new String[]{"applicationContext-flink.xml", "applicationContext-cache.xml"};
        applicationContext = new ClassPathXmlApplicationContext(SPRING_CONFIG_XML);
        flinkConfiguration = applicationContext.getBean("flinkConfiguration", FlinkConfiguration.class);

    }

    public static Bootstrap getInstance() {
        return INSTANCE;
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public StreamExecutionEnvironment createStreamExecutionEnvironment() {
        if (flinkConfiguration.isLocalforFlinkStreamExecutionEnvironment()) {
            LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
            localEnvironment.setParallelism(1);
            return localEnvironment;
        } else {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }

    public void setSourceFunctionParallel(DataStreamSource rawData) {
        int parallel = flinkConfiguration.getFlinkSourceFunctionParallel();
        rawData.setParallelism(parallel);
    }
}
