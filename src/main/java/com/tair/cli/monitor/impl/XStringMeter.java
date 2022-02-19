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

import java.util.concurrent.atomic.AtomicReference;

import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;
import com.tair.cli.monitor.entity.Meter;

/**
 * @author Baoyi Chen
 */
public class XStringMeter implements Meter<String> {
	private final AtomicReference<String> gauge = new AtomicReference<>();
	private final AtomicReference<String> property = new AtomicReference<>();
	
	@Override
	public Tuple2<String, String> getMeter() {
		return Tuples.of(this.gauge.get(), this.property.get());
	}
	
	@Override
	public XStringMeter reset() {
		String v = gauge.getAndSet(null); String p = property.getAndSet(null);
		if (v == null) return null; else return new ImmutableXStringMeter(v, p);
	}
	
	void set(String value) {
		gauge.set(value);
	}
	
	void setProperty(String value) {
		property.set(value);
	}
	
	private static class ImmutableXStringMeter extends XStringMeter {
		private final String value;
		private final String property;
		
		public ImmutableXStringMeter(String v, String p) {
			this.value = v;
			this.property = p;
		}
		
		@Override
		public XStringMeter reset() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Tuple2<String, String> getMeter() {
			return Tuples.of(value, property);
		}
	}
}
