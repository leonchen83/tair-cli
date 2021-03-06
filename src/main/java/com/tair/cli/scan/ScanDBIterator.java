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

package com.tair.cli.scan;

import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START_BINARY;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.tair.cli.ext.XDumpKeyValuePair;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * @author Baoyi Chen
 */
public class ScanDBIterator implements Iterator<XDumpKeyValuePair> {
	
	private DB db;
	private int index;
	private int count;
	private int rdbVersion;
	private List<Struct> structs;
	private ScanIterator iterator;
	private ScanResult<byte[]> keys;
	
	public ScanDBIterator(DB db, ScanIterator iterator, int count, int rdbVersion) {
		this.db = db;
		this.count = count;
		this.iterator = iterator;
		this.rdbVersion = rdbVersion;
		iterator.retry(e -> e.select((int)db.getDbNumber()));
		keys = iterator.retry(e -> e.scan(SCAN_POINTER_START_BINARY, new ScanParams().count(count)));
		
		iterator.retry(e -> {
			Pipeline pipeline = e.pipelined();
			structs = new ArrayList<>(keys.getResult().size());
			for (byte[] key : keys.getResult()) {
				if (rdbVersion >= 8) {
					structs.add(new Struct(key, pipeline.memoryUsage(key), pipeline.pttl(key), pipeline.dump(key)));
				} else {
					structs.add(new Struct(key, pipeline.pttl(key), pipeline.dump(key)));
				}
			}
			pipeline.sync();
			return null;
		});
		
	}
	
	@Override
	public boolean hasNext() {
		return keys != null && index < keys.getResult().size();
	}
	
	@Override
	public XDumpKeyValuePair next() {
		XDumpKeyValuePair kv = new XDumpKeyValuePair();
		kv.setDb(db);
		Struct struct = structs.get(index);
		kv.setKey(struct.key);
		kv.setValue(struct.dump.get());
		Long ttl = struct.ttl.get();
		if (ttl != null && ttl != -1) {
			kv.setExpiredType(ExpiredType.MS);
			kv.setExpiredValue(struct.now + struct.ttl.get());
		}
		kv.setValueRdbType(kv.getValue()[0]);
		if (struct.memoryUsage != null) {
			kv.setMemoryUsage(struct.memoryUsage.get());
		}
		kv.setVersion(rdbVersion);
		index++;
		if (index >= keys.getResult().size() && !keys.getCursor().equals(SCAN_POINTER_START)) {
			keys = iterator.retry(e -> e.scan(keys.getCursorAsBytes(), new ScanParams().count(count)));
			iterator.retry(e -> {
				Pipeline pipeline = e.pipelined();
				structs = new ArrayList<>(keys.getResult().size());
				for (byte[] key : keys.getResult()) {
					if (rdbVersion >= 8) {
						structs.add(new Struct(key, pipeline.memoryUsage(key), pipeline.pttl(key), pipeline.dump(key)));
					} else {
						structs.add(new Struct(key, pipeline.pttl(key), pipeline.dump(key)));
					}
				}
				pipeline.sync();
				return null;
			});
			index = 0;
		}
		return kv;
	}
	
	private static class Struct {
		
		private byte[] key;
		private Response<Long> ttl;
		private Response<byte[]> dump;
		private Response<Long> memoryUsage;
		private long now = System.currentTimeMillis();
		
		public Struct(byte[] key, Response<Long> ttl, Response<byte[]> dump) {
			this(key, null, ttl, dump);
		}
		
		public Struct(byte[] key, Response<Long> memoryUsage, Response<Long> ttl, Response<byte[]> dump) {
			this.key = key;
			this.ttl = ttl;
			this.dump = dump;
			this.memoryUsage = memoryUsage;
		}
	}
}
