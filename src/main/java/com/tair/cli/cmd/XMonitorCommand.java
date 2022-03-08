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

package com.tair.cli.cmd;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.tair.cli.conf.Configure;
import com.tair.cli.monitor.Monitor;
import com.tair.cli.monitor.MonitorFactory;
import com.tair.cli.monitor.MonitorManager;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author Baoyi Chen
 */
public class XMonitorCommand implements Runnable, Closeable {
	
	private static Logger logger = LoggerFactory.getLogger(XMonitorCommand.class);
	
	private final String host;
	private final int port;
	private Configure configure;
	private final int retries = 5;
	private MonitorManager manager;
	protected volatile Jedis jedis;
	protected final DefaultJedisClientConfig config;
	protected Map<String, Map<String, String>> prev;
	private static final Monitor monitor = MonitorFactory.getMonitor("tair_monitor");
	
	public XMonitorCommand(RedisURI uri, Configure configure) {
		this.configure = configure;
		this.manager = new MonitorManager(configure);
		this.manager.open();
		Configuration configuration = configure.merge(uri, true);
		this.host = uri.getHost();
		this.port = uri.getPort();
		DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder();
		builder.user(configuration.getAuthUser()).password(configuration.getAuthPassword());
		builder.ssl(configuration.isSsl()).sslSocketFactory(configuration.getSslSocketFactory());
		builder.connectionTimeoutMillis(configure.getTimeout());
		this.config = builder.build();
		try {
			this.jedis = new Jedis(host, port, config);
		} catch (JedisConnectionException e) {
			System.err.println("failed to connect to [" + host + ":" + port + "]");
			System.exit(-1);
		}
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
	
	@Override
	public void run() {
		try {
			String info = retry(e -> e.info());
			Map<String, Map<String, String>> map = convert(info);
			
			// Server
			long now = System.currentTimeMillis();
			monitor.set("monitor", configure.get("instance"), now);
			setLong("Server", "uptime_in_seconds", map);
			setString("Server", "redis_version", map);
			setString("Replication", "role", map);
			
			// Clients
			setLong("Clients", "connected_clients", map);
			setLong("Clients", "client_recent_max_input_buffer", map);
			setLong("Clients", "client_recent_max_output_buffer", map);
			setLong("Clients", "blocked_clients", map);
			setLong("Clients", "tracking_clients", map); // ?
			
			// Memory
			setLong("Memory", "maxmemory", map);
			setLong("Memory", "used_memory", map);
			setLong("Memory", "used_memory_lua", map);
			setLong("Memory", "total_system_memory", map); // ?
			
			// Stats
			setLong("Stats", "total_connections_received", map);
			setLong("Stats", "total_commands_processed", map);
			
			setLong("Stats", "total_reads_processed", map);
			setLong("Stats", "total_writes_processed", map);
			setLong("Stats", "total_error_replies", map);
			
			setLong("Stats", "total_net_input_bytes", map);
			setLong("Stats", "total_net_output_bytes", map);
			setLong("Stats", "evicted_keys_per_sec", map);
			setLong("Stats", "instantaneous_ops_per_sec", map);
			setLong("Stats", "instantaneous_write_ops_per_sec", map);
			setLong("Stats", "instantaneous_read_ops_per_sec", map);
			setLong("Stats", "instantaneous_other_ops_per_sec", map);
			setLong("Stats", "instantaneous_sync_write_ops_per_sec", map);
			setDouble("Stats", "instantaneous_input_kbps", map);
			setDouble("Stats", "instantaneous_output_kbps", map);
			
			// CPU
			setDouble("CPU", "used_cpu_sys", map);
			setDouble("CPU", "used_cpu_user", map);
			setDouble("CPU", "used_cpu_sys_children", map);
			setDouble("CPU", "used_cpu_user_children", map);
			monitorDB(map);
			
			// diff
			diffLong("Stats", "expired_keys", prev, map);
			diffLong("Stats", "evicted_keys", prev, map);
			diffLong("Stats", "total_connections_received", prev, map, "diff_");
			diffLong("Stats", "total_commands_processed", prev, map, "diff_");
			diffLong("Stats", "total_net_input_bytes", prev, map, "diff_");
			diffLong("Stats", "total_net_output_bytes", prev, map, "diff_");
			
			diffLong("Stats", "total_reads_processed", prev, map, "diff_");
			diffLong("Stats", "total_writes_processed", prev, map, "diff_");
			diffLong("Stats", "total_error_replies", prev, map, "diff_");
			
			prev = map;
			delay(10, TimeUnit.SECONDS);
		} catch (JedisConnectionException e) {
			System.err.println("failed to connect to [" + host + ":" + port + "]");
			System.exit(-1);
		}
	}
	
	private void setLong(String key, String field, Map<String, Map<String, String>> map) {
		Long value = getLong(key, field, map);
		if (value != null) {
			monitor.set(field, value);
		}
	}
	
	private void setDouble(String key, String field, Map<String, Map<String, String>> map) {
		Double value = getDouble(key, field, map);
		if (value != null) {
			monitor.set(field, value);
		}
	}
	
	private void setString(String key, String field, Map<String, Map<String, String>> map) {
		String value = getString(key, field, map);
		if (value != null) {
			monitor.set(field, value);
		}
	}
	
	private String getString(String key, String field, Map<String, Map<String, String>> map) {
		if (map == null) return null;
		if (map.containsKey(key) && map.get(key).containsKey(field)) {
			return map.get(key).get(field);
		}
		return null;
	}
	
	private Double getDouble(String key, String field, Map<String, Map<String, String>> map) {
		if (map == null) return null;
		if (map.containsKey(key) && map.get(key).containsKey(field)) {
			String value = map.get(key).get(field);
			try {
				return Double.valueOf(value);
			} catch (NumberFormatException e) {
				logger.error("failed to monitor double attribute [{}]", field);
			}
		}
		return null;
	}
	
	private Long getLong(String key, String field, Map<String, Map<String, String>> map) {
		if (map == null) return null;
		if (map.containsKey(key) && map.get(key).containsKey(field)) {
			String value = map.get(key).get(field);
			try {
				return Long.valueOf(value);
			} catch (NumberFormatException e) {
				logger.error("failed to monitor double attribute [{}]", field);
			}
		}
		return null;
	}
	
	private void diffLong(String key, String field, Map<String, Map<String, String>> prev, Map<String, Map<String, String>> next) {
		diffLong(key, field, prev, next, "");
	}
	
	private void diffLong(String key, String field, Map<String, Map<String, String>> prev, Map<String, Map<String, String>> next, String prefix) {
		Long pv = getLong(key, field, prev);
		Long nv = getLong(key, field, next);
		if (pv != null && nv != null) {
			monitor.set(prefix + field, nv - pv);
		}
	}
	
	private void monitorDB(Map<String, Map<String, String>> map) {
		try {
			Map<String, String> value = map.get("Keyspace");
			long totalCount = 0L;
			long totalExpire = 0L;
			for (Map.Entry<String, String> entry : value.entrySet()) {
				String key = entry.getKey();
				String[] ary = entry.getValue().split(",");
				long dbsize = Long.parseLong(ary[0].split("=")[1]);
				long expires = Long.parseLong(ary[1].split("=")[1]);
				monitor.set("dbnum", key, dbsize);
				monitor.set("dbexp", key, expires);
				totalCount += dbsize;
				totalExpire += expires;
			}
			monitor.set("total_dbnum", totalCount);
			monitor.set("total_dbexp", totalExpire);
		} catch (NumberFormatException e) {
			logger.error("failed to monitor db info");
		}
	}
	
	private Map<String, Map<String, String>> convert(String info) {
		Map<String, Map<String, String>> map = new HashMap<>(16);
		String[] lines = info.split("\n");
		Map<String, String> value = null;
		for (String line : lines) {
			line = line == null ? "" : line.trim();
			if (line.startsWith("#")) {
				String key = line.substring(1).trim();
				value = new HashMap<>(128);
				map.put(key, value);
			} else if(line.length() != 0) {
				String[] ary = line.split(":");
				if (ary.length == 2) {
					if (value != null) value.put(ary[0], ary[1]);
				}
			}
		}
		return map;
	}
	
	private void delay(long time, TimeUnit unit) {
		try {
			unit.sleep(time);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	public void close() {
		try {
			if (jedis != null) {
				jedis.close();
			}
		} catch (Throwable e) {
		}
		MonitorManager.closeQuietly(manager);
	}
}
