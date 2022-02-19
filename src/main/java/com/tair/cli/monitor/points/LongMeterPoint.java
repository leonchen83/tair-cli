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

import com.tair.cli.monitor.entity.Meter;
import com.tair.cli.monitor.entity.Monitor;

/**
 * @author Baoyi Chen
 */
public class LongMeterPoint {
	private long value;
	private long timestamp;
	private String property;
	private String monitorName;
	
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
	
	public static LongMeterPoint valueOf(Monitor monitor, String key, Meter<Long> meter) {
		LongMeterPoint point = new LongMeterPoint();
		point.monitorName = key;
		point.value = meter.getMeter().getV1();
		point.property = meter.getMeter().getV2();
		point.timestamp = System.currentTimeMillis();
		return point;
	}
	
	@Override
	public String toString() {
		return "LongMeterPoint{" +
				"value=" + value +
				", timestamp=" + timestamp +
				", property='" + property + '\'' +
				", monitorName='" + monitorName + '\'' +
				'}';
	}
}
