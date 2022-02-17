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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.tair.cli.conf.Configure;
import com.tair.cli.monitor.MonitorFactory;
import com.tair.cli.monitor.MonitorManager;
import com.tair.cli.monitor.entity.Monitor;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;

/**
 * @author Baoyi Chen
 */
public class XMonitorCommand implements Runnable, Closeable {
	
	private static Logger logger = LoggerFactory.getLogger(XMonitorCommand.class);
	
	private final String host;
	private final int port;
	private MonitorManager manager;
	protected final DefaultJedisClientConfig config;
	private static final Monitor monitor = MonitorFactory.getMonitor("tair_monitor");
	
	public XMonitorCommand(RedisURI uri, Configure configure) {
		this.manager = new MonitorManager(configure);
		this.manager.open("tair_monitor");
		Configuration configuration = configure.merge(uri, true);
		this.host = uri.getHost();
		this.port = uri.getPort();
		DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder();
		builder.user(configuration.getAuthUser()).password(configuration.getAuthPassword());
		builder.ssl(configuration.isSsl()).sslSocketFactory(configuration.getSslSocketFactory());
		builder.connectionTimeoutMillis(configure.getTimeout());
		this.config = builder.build();
	}
	
	@Override
	public void run() {
		try(Jedis jedis = new Jedis(host, port, config)) {
			String info = jedis.info();
			Map<String, Map<String, String>> map = convert(info);
			set("Server", "uptime_in_seconds", map);
			//
			set("Clients", "connected_clients", map);
			set("Clients", "client_recent_max_input_buffer", map);
			set("Clients", "client_recent_max_output_buffer", map);
			set("Clients", "blocked_clients", map);
			set("Clients", "tracking_clients", map);
			//
			set("Memory", "used_memory", map);
			set("Memory", "maxmemory", map);
			set("Memory", "total_system_memory", map);
			//
			set("Persistence", "rdb_last_save_time", map);
			//
			set("Stats", "total_connections_received", map);
			set("Stats", "total_commands_processed", map);
			set("Stats", "total_net_input_bytes", map);
			set("Stats", "total_net_output_bytes", map);
			set("Stats", "expired_keys", map);
			set("Stats", "evicted_keys", map);
			
			monitorDB(map);
		}
		delay(30, TimeUnit.SECONDS);
	}
	
	private void set(String key, String field, Map<String, Map<String, String>> map) {
		String value = map.get(key).get(field);
		try {
			monitor.set(field, Long.parseLong(value));
		} catch (NumberFormatException e) {
			logger.error("failed to monitor attribute [{}]", field);
		}
	}
	
	private void monitorDB(Map<String, Map<String, String>> map) {
		try {
			Map<String, String> value = map.get("Keyspace");
			for (Map.Entry<String, String> entry : value.entrySet()) {
				String key = entry.getKey().substring(2);
				String[] ary = entry.getValue().split(",");
				long dbsize = Long.parseLong(ary[0].split("=")[1]);
				long expires = Long.parseLong(ary[1].split("=")[1]);
				monitor.set("dbnum_" + key, dbsize);
				monitor.set("dbexp_" + key, expires);
			}
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
		MonitorManager.closeQuietly(manager);
	}
}
