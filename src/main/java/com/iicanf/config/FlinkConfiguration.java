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
package com.iicanf.config;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Properties;

/**
 * @author minwoo.jung
 */
@Component("flinkConfiguration")
public class FlinkConfiguration implements InitializingBean {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String evironment;
    @Autowired
    private Properties properties;

    public String getEvironment() {
        return evironment;
    }

    protected void readPropertyValues(Properties properties) {
        logger.info("flink.properties read.");

        this.evironment = readString(properties, "envirment", GlobolEnum.SERVER_ENVIRMENT);
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

    @Override
    public void afterPropertiesSet() throws Exception {
        final Properties properties = Objects.requireNonNull(this.properties, "properties must not be null");
        readPropertyValues(properties);
    }
}
