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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.tair.cli.ext.XDumpKeyValuePair;

import redis.clients.jedis.Jedis;

/**
 * @author Baoyi Chen
 */
public class ScanIterator implements Iterator<XDumpKeyValuePair> {
	
	private int index;
	private int count;
	private Jedis jedis;
	private List<DB> dbs;
	private int rdbVersion;
	private ScanDBIterator iterator;
	
	private static Map<String, Integer> VERSIONS = new HashMap<>();
	
	static {
		//# Redis-7.0.x : 10
		//# Redis-6.2 : 9
		//# Redis-6.0.x : 9
		//# Redis-5.0.x : 9
		//# Redis-4.0.x : 8
		//# Redis-3.2 : 7
		//# Redis-3.0 : 6
		//# Redis-2.8 : 6
		//# Redis-2.6 : 6
		VERSIONS.put("2.6", 6);
		VERSIONS.put("2.8", 6);
		VERSIONS.put("3.0", 6);
		VERSIONS.put("3.2", 7);
		VERSIONS.put("4.0", 8);
		VERSIONS.put("5.0", 9);
		VERSIONS.put("6.0", 9);
		VERSIONS.put("6.2", 9);
		VERSIONS.put("7.0", 10);
	}
	
	public int getRdbVersion() {
		return rdbVersion;
	}
	
	public ScanIterator(List<Integer> dbs, Jedis jedis, int count) {
		this.jedis = jedis;
		this.count = count;
		String keyspace = jedis.info("keyspace");
		String[] line = keyspace.split("\n");
		this.dbs = new ArrayList<>();
		for (int i = 1; i < line.length; i++) {
			// db0:keys=1003,expires=0,avg_ttl=0
			String[] ary = line[i].split(":");
			Integer db = Integer.parseInt(ary[0].substring(2));
			ary = ary[1].split(",");
			long dbsize = Long.parseLong(ary[0].split("=")[1]);
			long expires = Long.parseLong(ary[1].split("=")[1]);
			if (dbs == null || dbs.isEmpty() || dbs.contains(db)) {
				this.dbs.add(new DB(db, dbsize, expires));
			}
		}
		
		String server = jedis.info("server");
		line = server.split("\n");
		String version = line[1].split(":")[1];
		version = version.substring(0, version.lastIndexOf('.'));
		this.rdbVersion = VERSIONS.containsKey(version) ? VERSIONS.get(version) : 10;
		if (!this.dbs.isEmpty()) {
			this.iterator = new ScanDBIterator(this.dbs.get(index++), jedis, count, this.rdbVersion);
		}
	}
	
	public static void main(String[] args) {
		ScanIterator it = new ScanIterator(null, new Jedis("127.0.0.1", 6328), 128);
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}
	
	@Override
	public boolean hasNext() {
		return index < dbs.size() || (iterator != null && iterator.hasNext());
	}
	
	@Override
	public XDumpKeyValuePair next() {
		XDumpKeyValuePair key = iterator.next();
		if (!iterator.hasNext() && index < this.dbs.size()) {
			iterator = new ScanDBIterator(this.dbs.get(index++), jedis, count, this.rdbVersion);
		}
		return key;
	}
}
