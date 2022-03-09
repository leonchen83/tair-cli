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

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;

import java.io.Closeable;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.cmd.RedisCodec;
import com.tair.cli.conf.Configure;
import com.tair.cli.monitor.Monitor;
import com.tair.cli.monitor.MonitorFactory;
import com.tair.cli.monitor.MonitorManager;
import com.tair.cli.util.Strings;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.resps.Slowlog;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class XMonitorCommand implements Runnable, Closeable {
	
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	private static Logger logger = LoggerFactory.getLogger(XMonitorCommand.class);
	
	private final String host;
	private final int port;
	private Configure configure;
	private final int retries = 5;
	private MonitorManager manager;
	protected volatile Jedis jedis;
	protected List<Slowlog> prevLogs;
	protected RedisCodec codec = new RedisCodec();
	protected final DefaultJedisClientConfig config;
	protected Map<String, Map<String, String>> prevInfo;
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
			Map<String, Map<String, String>> nextInfo = convert(info);
			
			// Server
			long now = System.currentTimeMillis();
			monitor.set("monitor", configure.get("instance"), now);
			setLong("Server", "uptime_in_seconds", nextInfo);
			setString("Server", "redis_version", nextInfo);
			setString("Replication", "role", nextInfo);
			
			// Clients
			setLong("Clients", "connected_clients", nextInfo);
			setLong("Clients", "client_recent_max_input_buffer", nextInfo);
			setLong("Clients", "client_recent_max_output_buffer", nextInfo);
			setLong("Clients", "blocked_clients", nextInfo);
			setLong("Clients", "tracking_clients", nextInfo); // ?
			
			// Memory
			setLong("Memory", "maxmemory", nextInfo);
			setLong("Memory", "used_memory", nextInfo);
			setLong("Memory", "used_memory_lua", nextInfo);
			setLong("Memory", "total_system_memory", nextInfo); // ?
			
			// Stats
			setLong("Stats", "total_connections_received", nextInfo);
			setLong("Stats", "total_commands_processed", nextInfo);
			
			setLong("Stats", "total_reads_processed", nextInfo);
			setLong("Stats", "total_writes_processed", nextInfo);
			setLong("Stats", "total_error_replies", nextInfo);
			
			setLong("Stats", "total_net_input_bytes", nextInfo);
			setLong("Stats", "total_net_output_bytes", nextInfo);
			setLong("Stats", "evicted_keys_per_sec", nextInfo);
			setLong("Stats", "instantaneous_ops_per_sec", nextInfo);
			setLong("Stats", "instantaneous_write_ops_per_sec", nextInfo);
			setLong("Stats", "instantaneous_read_ops_per_sec", nextInfo);
			setLong("Stats", "instantaneous_other_ops_per_sec", nextInfo);
			setLong("Stats", "instantaneous_sync_write_ops_per_sec", nextInfo);
			setDouble("Stats", "instantaneous_input_kbps", nextInfo);
			setDouble("Stats", "instantaneous_output_kbps", nextInfo);
			
			// CPU
			setDouble("CPU", "used_cpu_sys", nextInfo);
			setDouble("CPU", "used_cpu_user", nextInfo);
			setDouble("CPU", "used_cpu_sys_children", nextInfo);
			setDouble("CPU", "used_cpu_user_children", nextInfo);
			monitorDB(nextInfo);
			
			// diff
			diffLong("Stats", "expired_keys", prevInfo, nextInfo);
			diffLong("Stats", "evicted_keys", prevInfo, nextInfo);
			diffLong("Stats", "total_connections_received", prevInfo, nextInfo, "diff_");
			diffLong("Stats", "total_commands_processed", prevInfo, nextInfo, "diff_");
			diffLong("Stats", "total_net_input_bytes", prevInfo, nextInfo, "diff_");
			diffLong("Stats", "total_net_output_bytes", prevInfo, nextInfo, "diff_");
			
			diffLong("Stats", "total_reads_processed", prevInfo, nextInfo, "diff_");
			diffLong("Stats", "total_writes_processed", prevInfo, nextInfo, "diff_");
			diffLong("Stats", "total_error_replies", prevInfo, nextInfo, "diff_");
			
			prevInfo = nextInfo;
			
			// slow latency
			long len = retry(e -> e.slowlogLen());
			List<Object> binaryLogs = retry(e -> e.slowlogGetBinary(128)); // configurable size ?
			List<Slowlog> nextLogs = Slowlog.from(binaryLogs);
			long nextId = isEmpty(nextLogs) ? 0 : nextLogs.get(0).getId();
			long prevId = isEmpty(prevLogs) ? nextId : prevLogs.get(0).getId();
			
			monitor.set("total_slow_log", nextId);
			monitor.set("diff_total_slow_log", nextId - prevId);
			
			int count = (int) Math.min(nextId - prevId, len);
			long totalExecutionTime = 0L;
			for (int i = 0; i < count; i++) {
				List<Object> binaryLog = (List<Object>) binaryLogs.get(i);
				List<byte[]> bargs = (List<byte[]>) binaryLog.get(3);
				Slowlog slowlog = nextLogs.get(i);
				totalExecutionTime += slowlog.getExecutionTime();
				slowlog.getId();
				slowlog.getArgs();
				slowlog.getTimeStamp();
				slowlog.getClientIpPort();
				slowlog.getClientName();
				String[] properties = new String[5];
				properties[0] = String.valueOf(slowlog.getId());
				properties[1] = FORMATTER.format(ofEpochMilli(slowlog.getTimeStamp() * 1000).atZone(systemDefault()));
				properties[2] = bargs.stream().map(e -> quote(new String(codec.encode(e)))).collect(Collectors.joining(" "));
				properties[3] = Strings.isEmpty(slowlog.getClientName()) ? "" : slowlog.getClientName();
				properties[4] = slowlog.getClientIpPort() == null ? "" : slowlog.getClientIpPort().toString();
				monitor.set("slow_log", properties, slowlog.getExecutionTime());
			}
			
			if (count > 0) {
				monitor.set("slow_log_latency", (totalExecutionTime / (count * 1d)));
			} else {
				monitor.set("slow_log_latency", 0d);
			}
			
			prevLogs = nextLogs;
			delay(15, TimeUnit.SECONDS);
		} catch (JedisConnectionException e) {
			System.err.println("failed to connect to [" + host + ":" + port + "]");
			System.exit(-1);
		}
	}
	
	private static <T> boolean isEmpty(List<T> prevLogs) {
		return prevLogs == null || prevLogs.isEmpty();
	}
	
	private String quote(String name) {
		return new StringBuilder().append('"').append(name).append('"').toString();
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
