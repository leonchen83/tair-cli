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

package com.tair.cli.monitor.points;

import java.util.Arrays;

import com.moilioncircle.redis.replicator.util.type.Tuple2;
import com.tair.cli.monitor.Counter;
import com.tair.cli.monitor.Monitor;
import com.tair.cli.monitor.MonitorKey;

/**
 * @author Baoyi Chen
 */
public class CounterPoint<T> {
	private long time;
	private T value;
	private long timestamp;
	private String[] properties;
	private String monitorName;
	
	public long getTime() {
		return time;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public T getValue() {
		return value;
	}
	
	public void setValue(T value) {
		this.value = value;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public String[] getProperties() {
		return properties;
	}
	
	public void setProperties(String[] properties) {
		this.properties = properties;
	}
	
	public String getMonitorName() {
		return monitorName;
	}
	
	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}
	
	public static <T> CounterPoint<T> valueOf(Monitor monitor, MonitorKey key, Counter<T> counter) {
		Tuple2<T, Long> tps = counter.getCounter();
		CounterPoint<T> point = new CounterPoint<>();
		point.monitorName = key.getKey();
		point.properties = key.getProperties();
		point.timestamp = System.currentTimeMillis();
		point.time = tps.getV2();
		point.value = tps.getV1();
		return point;
	}
	
	@Override
	public String toString() {
		return "CounterPoint{" +
				"monitorName='" + monitorName + '\'' +
				", properties=" + Arrays.toString(properties) +
				", timestamp=" + timestamp +
				", time=" + time +
				", value=" + value +
				'}';
	}
}
