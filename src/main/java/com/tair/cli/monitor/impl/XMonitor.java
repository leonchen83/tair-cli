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

package com.tair.cli.monitor.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;
import com.tair.cli.monitor.entity.Counter;
import com.tair.cli.monitor.entity.Gauge;
import com.tair.cli.monitor.entity.Meter;
import com.tair.cli.monitor.entity.Monitor;

/**
 * @author Jingqi Xu
 */
public class XMonitor implements Monitor {
    protected final String name;
    private final Map<String, XGauge> gauges = new ConcurrentHashMap<>(8);
    private final Map<String, XCounter> counters = new ConcurrentHashMap<>(8);
    
    //
    private final Map<Tuple2<String, String>, XLongMeter> longMeters = new ConcurrentHashMap<>(8);
    private final Map<Tuple2<String, String>, XDoubleMeter> doubleMeters = new ConcurrentHashMap<>(8);
    private final Map<Tuple2<String, String>, XStringMeter> stringMeters = new ConcurrentHashMap<>(8);

    public XMonitor(String name) {
        this.name = name;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final void set(String key, long value) {
        this.doSet(key, value);
    }
    
    @Override
    public void setLong(String measurement, long value) {
        this.doSetLong(measurement, null, value);
    }
    
    @Override
    public void setDouble(String measurement, double value) {
        this.doSetDouble(measurement, null, value);
    }
    
    @Override
    public void setString(String measurement, String value) {
        this.doSetString(measurement, null, value);
    }
    
    @Override
    public void setLong(String measurement, String property, long value) {
        this.doSetLong(measurement, property, value);
    }
    
    @Override
    public void setDouble(String measurement, String property, double value) {
        this.doSetDouble(measurement, property, value);
    }
    
    @Override
    public void setString(String measurement, String property, String value) {
        this.doSetString(measurement, property, value);
    }
    
    @Override
    public final void add(String key, long count) {
        this.doAdd(key, count, -1);
    }

    @Override
    public final void add(String key, long count, long time) {
        this.doAdd(key, count, time);
    }

    @Override
    public Map<String, ? extends Gauge> getGauges() {
        return this.gauges;
    }

    @Override
    public Map<String, ? extends Counter> getCounters() {
        return this.counters;
    }
    
    @Override
    public Map<Tuple2<String, String>, ? extends Meter<Long>> getLongMeters() {
        return this.longMeters;
    }
    
    @Override
    public Map<Tuple2<String, String>, ? extends Meter<Double>> getDoubleMeters() {
        return this.doubleMeters;
    }
    
    @Override
    public Map<Tuple2<String, String>, ? extends Meter<String>> getStringMeters() {
        return this.stringMeters;
    }

    public static final class FactoryImpl implements Monitor.Factory {
        @Override
        public Monitor create(String name) {
            return new XMonitor(name);
        }
    }
    
    protected void doSet(String k, final long v) {
        XGauge x = this.gauges.get(k);
        if (x == null) x = putIfAbsent(gauges, k, new XGauge());
        x.set(v);
    }

    protected void doAdd(String k, long c, long t) {
        XCounter x = counters.get(k);
        if (x == null) x = putIfAbsent(counters, k, new XCounter());
        x.add(c, t);
    }
    
    protected void doSetLong(String k, String p, final long v) {
        Tuple2<String, String> key = Tuples.of(k, p);
        XLongMeter x = this.longMeters.get(key);
        if (x == null) x = putIfAbsent(longMeters, key, new XLongMeter());
        x.set(v); if (p != null) x.setProperty(p);
    }
    
    protected void doSetDouble(String k, String p, final double v) {
        Tuple2<String, String> key = Tuples.of(k, p);
        XDoubleMeter x = this.doubleMeters.get(key);
        if (x == null) x = putIfAbsent(doubleMeters, key, new XDoubleMeter());
        x.set(v); if (p != null) x.setProperty(p);
    }
    
    protected void doSetString(String k, String p, final String v) {
        Tuple2<String, String> key = Tuples.of(k, p);
        XStringMeter x = this.stringMeters.get(key);
        if (x == null) x = putIfAbsent(stringMeters, key, new XStringMeter());
        x.set(v); if (p != null) x.setProperty(p);
    }

    private static final <K, V> V putIfAbsent(Map<K, V> m, K k, V v) {
        final V r = m.putIfAbsent(k, v); return r != null ? r : v;
    }
}
