/*
 * Copyright 2016-2017 Leon Chen
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

package com.tair.cli.monitor.gateway.impl;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tair.cli.monitor.gateway.MetricGateway;
import com.tair.cli.monitor.points.DoubleCounterPoint;
import com.tair.cli.monitor.points.DoubleGaugePoint;
import com.tair.cli.monitor.points.LongCounterPoint;
import com.tair.cli.monitor.points.LongGaugePoint;
import com.tair.cli.monitor.points.StringGaugePoint;

/**
 * @author Baoyi Chen
 */
public class LogGateway implements MetricGateway {
    
    private static final Logger logger = LoggerFactory.getLogger("METRIC_LOGGER");

    @Override
    public void reset(String measurement) {
        
    }

    @Override
    public boolean save(List<DoubleCounterPoint> dcpoints, List<LongCounterPoint> lcpoints, List<StringGaugePoint> spoints, List<DoubleGaugePoint> dpoints, List<LongGaugePoint> lpoints) {
        for (DoubleCounterPoint point : dcpoints) {
            logger.info(point.toString());
        }
        for (LongCounterPoint point : lcpoints) {
            logger.info(point.toString());
        }
        for (LongGaugePoint point : lpoints) {
            logger.info(point.toString());
        }
        for (StringGaugePoint point : spoints) {
            logger.info(point.toString());
        }
        for (DoubleGaugePoint point : dpoints) {
            logger.info(point.toString());
        }
        return true;
    }

    @Override
    public void close() throws IOException {

    }
}
