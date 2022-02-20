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
    
    Map<Tuple2<String, String>, ? extends Counter<Long>> getLongCounters();
    
    Map<Tuple2<String, String>, ? extends Counter<Double>> getDoubleCounters();
    
    Map<Tuple2<String, String>, ? extends Gauge<Long>> getLongGauges();
    
    Map<Tuple2<String, String>, ? extends Gauge<Double>> getDoubleGauges();
    
    Map<Tuple2<String, String>, ? extends Gauge<String>> getStringGauges();

    /**
     * Counter
     */
    void add(String measurement, long value);

    void add(String measurement, long value, long time);
    
    void add(String measurement, String property, long value);
    
    void add(String measurement, String property, long value, long time);
    
    void add(String measurement, double value);
    
    void add(String measurement, double value, long time);
    
    void add(String measurement, String property, double value);
    
    void add(String measurement, String property, double value, long time);
    
    /**
     * Gauge
     */
    void set(String measurement, long value);
    
    void set(String measurement, double value);
    
    void set(String measurement, String value);
    
    void set(String measurement, String property, long value);
    
    void set(String measurement, String property, double value);
    
    void set(String measurement, String property, String value);

}
