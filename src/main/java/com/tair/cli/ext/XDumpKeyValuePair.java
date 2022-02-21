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

package com.tair.cli.ext;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class XDumpKeyValuePair extends DumpKeyValuePair {
	private long memoryUsage;
	private int version;
	private Escaper escaper;
	
	public long getMemoryUsage() {
		return memoryUsage;
	}
	
	public void setMemoryUsage(long memoryUsage) {
		this.memoryUsage = memoryUsage;
	}
	
	public int getVersion() {
		return version;
	}
	
	public void setVersion(int version) {
		this.version = version;
	}
	
	public Escaper getEscaper() {
		return escaper;
	}
	
	public void setEscaper(Escaper escaper) {
		this.escaper = escaper;
	}
}
