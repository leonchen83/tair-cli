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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.tair.cli.ext.XDumpKeyValuePair;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author Baoyi Chen
 */
public class ScanIterator implements Iterator<XDumpKeyValuePair>, Closeable {
	
	private int index;
	private int count;
	private int port;
	private String host;
	private List<DB> dbs;
	private int rdbVersion;
	private int retries = 5;
	private volatile Jedis jedis;
	private ScanDBIterator iterator;
	private JedisClientConfig config;
	
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
		VERSIONS.put("6.9", 10);
		VERSIONS.put("7.0", 10);
	}
	
	public int getRdbVersion() {
		return rdbVersion;
	}
	
	private void renew() {
		if (this.jedis != null) {
			try {
				this.jedis.close();
			} catch (Throwable ignore) {
			}
		}
		try {
			this.jedis = new Jedis(host, port, config);
		} catch (JedisConnectionException e) {
		}
	}
	
	<T> T retry(Function<Jedis, T> func) {
		JedisConnectionException exception = null;
		for (int i = 0; i < retries; i++) {
			try {
				return func.apply(jedis);
			} catch (JedisConnectionException e) {
				exception = e;
				renew();
			}
		}
		throw exception;
	}
	
	public ScanIterator(List<Integer> dbs, String host, int port, JedisClientConfig config, int count) {
		this.host = host;
		this.port = port;
		this.count = count;
		this.config = config;
		try {
			this.jedis = new Jedis(host, port, config);
		} catch (JedisConnectionException e) {
			System.err.println("failed to connect to [" + host + ":" + port + "]");
			System.exit(-1);
		}
		String keyspace = retry(e -> e.info("keyspace"));
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
		
		String server = retry(e -> e.info("server"));
		line = server.split("\n");
		String version = line[1].split(":")[1];
		version = version.substring(0, version.lastIndexOf('.'));
		if (VERSIONS.containsKey(version)) {
			this.rdbVersion = VERSIONS.get(version);
		} else {
			throw new UnsupportedOperationException("unsupported source redis version :" + version);
		}
		if (!this.dbs.isEmpty()) {
			this.iterator = new ScanDBIterator(this.dbs.get(index++), this, count, this.rdbVersion);
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
			iterator = new ScanDBIterator(this.dbs.get(index++), this, count, this.rdbVersion);
		}
		return key;
	}
	
	@Override
	public void close() throws IOException {
		try {
			if (jedis != null) {
				jedis.close();
			}
		} catch (Throwable e) {
			throw new IOException(e);
		}
	}
}
