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

import com.moilioncircle.redis.replicator.util.type.Tuple3;
import com.tair.cli.monitor.entity.Counter;
import com.tair.cli.monitor.entity.Monitor;

/**
 * @author Baoyi Chen
 */
public class LongCounterPoint {
	private long time;
	private long value;
	private long timestamp;
	private String property;
	private String monitorName;
	
	public long getTime() {
		return time;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public long getValue() {
		return value;
	}
	
	public void setValue(long value) {
		this.value = value;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public String getProperty() {
		return property;
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public String getMonitorName() {
		return monitorName;
	}
	
	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}
	
	public static LongCounterPoint valueOf(Monitor monitor, String key, Counter<Long> counter) {
		LongCounterPoint point = new LongCounterPoint();
		point.monitorName = key;
		Tuple3<Long, String, Long> tps = counter.getCounter();
		point.value = tps.getV1();
		point.property = tps.getV2();
		point.time = tps.getV3();
		point.timestamp = System.currentTimeMillis();
		return point;
	}
	
	@Override
	public String toString() {
		return "LongCounterPoint{" +
				"time=" + time +
				", value=" + value +
				", timestamp=" + timestamp +
				", property='" + property + '\'' +
				", monitorName='" + monitorName + '\'' +
				'}';
	}
}