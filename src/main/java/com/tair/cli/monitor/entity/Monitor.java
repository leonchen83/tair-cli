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

package com.tair.cli.monitor.entity;

import java.util.Map;

import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Baoyi Chen
 */
public interface Monitor {
    interface Factory {
        Monitor create(String name);
    }

    String getName();
    
    Map<String, ? extends Gauge> getGauges();

    Map<String, ? extends Counter> getCounters();
    
    Map<Tuple2<String, String>, ? extends Meter<Long>> getLongMeters();
    
    Map<Tuple2<String, String>, ? extends Meter<Double>> getDoubleMeters();
    
    Map<Tuple2<String, String>, ? extends Meter<String>> getStringMeters();

    /**
     * Gauge
     */
    void set(String key, long value);
    
    /**
     * Counter
     */
    void add(String key, long count);

    void add(String key, long count, long time);
    
    /**
     * Meter
     */
    void setLong(String measurement, long value);
    
    void setDouble(String measurement, double value);
    
    void setString(String measurement, String value);
    
    /**
     * Meter
     */
    void setLong(String measurement, String property, long value);
    
    void setDouble(String measurement, String property, double value);
    
    void setString(String measurement, String property, String value);

}
