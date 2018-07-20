/*
 * Copyright 2017 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iicanf.socket.config;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * @author minwoo.jung
 */
public class FlinkConfiguration {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private boolean flinkClusterEnable;
    private String flinkClusterZookeeperAddress;
    private int flinkClusterSessionTimeout;
    private int flinkRetryInterval;
    private int flinkClusterTcpPort;
    private String flinkStreamExecutionEnvironment;

    private int flinkSourceFunctionParallel;

    public boolean isFlinkClusterEnable() {
        return flinkClusterEnable;
    }

    public String getFlinkClusterZookeeperAddress() {
        return flinkClusterZookeeperAddress;
    }

    public int getFlinkClusterTcpPort() {
        return flinkClusterTcpPort;
    }

    public int getFlinkClusterSessionTimeout() {
        return flinkClusterSessionTimeout;
    }

    public int getFlinkRetryInterval() {
        return flinkRetryInterval;
    }

    public int getFlinkSourceFunctionParallel() {
        return flinkSourceFunctionParallel;
    }

    public boolean isLocalforFlinkStreamExecutionEnvironment() {
        return "local".equals(flinkStreamExecutionEnvironment) ? true : false;
    }


    protected void readPropertyValues(Properties properties) {
        logger.info("pinpoint-flink.properties read.");

        this.flinkClusterEnable = readBoolean(properties, "flink.cluster.enable");
        this.flinkClusterZookeeperAddress = readString(properties, "flink.cluster.zookeeper.address", "");
        this.flinkClusterSessionTimeout = readInt(properties, "flink.cluster.zookeeper.sessiontimeout", -1);
        this.flinkRetryInterval = readInt(properties, "flink.cluster.zookeeper.retry.interval", 60000);
        this.flinkClusterTcpPort = readInt(properties, "flink.cluster.tcp.port", 19994);
        this.flinkStreamExecutionEnvironment = readString(properties, "flink.StreamExecutionEnvironment", "server");
        this.flinkSourceFunctionParallel = readInt(properties, "flink.sourceFunction.Parallel", 1);
    }

    private String readString(Properties properties, String propertyName, String defaultValue) {
        final String result = properties.getProperty(propertyName, defaultValue);
        if (logger.isInfoEnabled()) {
            logger.info("{}={}", propertyName, result);
        }
        return result;
    }

    private int readInt(Properties properties, String propertyName, int defaultValue) {
        final String value = properties.getProperty(propertyName);
        final int result = NumberUtils.toInt(value, defaultValue);
        if (logger.isInfoEnabled()) {
            logger.info("{}={}", propertyName, result);
        }
        return result;
    }

    private long readLong(Properties properties, String propertyName, long defaultValue) {
        final String value = properties.getProperty(propertyName);
        final long result = NumberUtils.toLong(value, defaultValue);
        if (logger.isInfoEnabled()) {
            logger.info("{}={}", propertyName, result);
        }
        return result;
    }

    private boolean readBoolean(Properties properties, String propertyName) {
        final String value = properties.getProperty(propertyName);

        // if a default value will be needed afterwards, may match string value instead of Utils.
        // for now stay unmodified because of no need.

        final boolean result = Boolean.valueOf(value);
        if (logger.isInfoEnabled()) {
            logger.info("{}={}", propertyName, result);
        }
        return result;
    }
}
