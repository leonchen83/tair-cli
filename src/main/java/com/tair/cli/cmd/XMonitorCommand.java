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

import static com.tair.cli.rinfo.XStandaloneRedisInfo.EMPTY;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.tair.cli.conf.Configure;
import com.tair.cli.monitor.Monitor;
import com.tair.cli.monitor.MonitorFactory;
import com.tair.cli.monitor.MonitorManager;
import com.tair.cli.rinfo.XStandaloneRedisInfo;
import com.tair.cli.rinfo.support.XSlowLog;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class XMonitorCommand implements Runnable, Closeable {
	
	private static final Monitor monitor = MonitorFactory.getMonitor("tair_monitor");
	
	private final String host;
	private final int port;
	private Configure configure;
	private final int retries = 5;
	private MonitorManager manager;
	protected volatile Jedis jedis;
	protected XStandaloneRedisInfo prev = EMPTY;
	protected final DefaultJedisClientConfig config;
	
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
			List<String> maxclients = retry(e -> e.configGet("maxclients"));
			long len = retry(e -> e.slowlogLen());
			List<Object> binaryLogs = retry(e -> e.slowlogGetBinary(128));
			XStandaloneRedisInfo next = XStandaloneRedisInfo.valueOf(info, maxclients, len, binaryLogs);
			next = XStandaloneRedisInfo.diff(prev, next);
			
			// server
			long now = System.currentTimeMillis();
			monitor.set("monitor", configure.get("instance"), now);
			setLong("uptime_in_seconds", next.getUptimeInSeconds());
			setString("redis_version", next.getRedisVersion());
			setString("role", next.getRole());
			
			// clients
			setLong("connected_clients", next.getConnectedClients());
			setLong("blocked_clients", next.getBlockedClients());
			setLong("tracking_clients", next.getTrackingClients());
			setLong("maxclients", next.getMaxclients());
			
			// memory
			setLong("maxmemory", next.getMaxmemory());
			setLong("used_memory", next.getUsedMemory());
			setLong("used_memory_rss", next.getUsedMemoryRss());
			setLong("used_memory_peak", next.getUsedMemoryPeak());
			setLong("used_memory_dataset", next.getUsedMemoryDataset());
			setLong("used_memory_lua", next.getUsedMemoryLua());
			setLong("used_memory_functions", next.getUsedMemoryFunctions());
			setLong("used_memory_scripts", next.getUsedMemoryScripts());
			setLong("total_system_memory", next.getTotalSystemMemory()); // ?
			setDouble("mem_fragmentation_ratio", next.getMemFragmentationRatio());
			setLong("mem_fragmentation_bytes", next.getMemFragmentationBytes());

			// command
			setLong("total_connections_received", next.getTotalConnectionsReceived());
			setLong("total_commands_processed", next.getTotalCommandsProcessed());
			
			setLong("total_reads_processed", next.getTotalReadsProcessed());
			setLong("total_writes_processed", next.getTotalWritesProcessed());
			setLong("total_error_replies", next.getTotalErrorReplies());
			
			Long hits = next.getKeyspaceHits();
			Long misses = next.getKeyspaceMisses();
			if (hits != null && misses != null) {
				monitor.set("keyspace_hit_rate", hits * 1d / (hits + misses));
			}
			
			// ops
			setLong("total_net_input_bytes", next.getTotalNetInputBytes());
			setLong("total_net_output_bytes", next.getTotalNetOutputBytes());
			setDouble("evicted_keys_per_sec", next.getEvictedKeysPerSec());
			setDouble("instantaneous_ops_per_sec", next.getInstantaneousOpsPerSec());
			setDouble("instantaneous_write_ops_per_sec", next.getInstantaneousWriteOpsPerSec());
			setDouble("instantaneous_read_ops_per_sec", next.getInstantaneousReadOpsPerSec());
			setDouble("instantaneous_other_ops_per_sec", next.getInstantaneousOtherOpsPerSec());
			setDouble("instantaneous_sync_write_ops_per_sec", next.getInstantaneousSyncWriteOpsPerSec());
			setDouble("instantaneous_input_kbps", next.getInstantaneousInputKbps());
			setDouble("instantaneous_output_kbps", next.getInstantaneousOutputKbps());
			
			// cpu
			setDouble("used_cpu_sys", next.getUsedCpuSys());
			setDouble("used_cpu_user", next.getUsedCpuUser());
			setDouble("used_cpu_sys_children", next.getUsedCpuSysChildren());
			setDouble("used_cpu_user_children", next.getUsedCpuUserChildren());
			
			// db
			for (Map.Entry<String, Long> entry : next.getDbInfo().entrySet()) {
				monitor.set("dbnum", entry.getKey(), entry.getValue());
			}
			for (Map.Entry<String, Long> entry : next.getDbExpireInfo().entrySet()) {
				monitor.set("dbexp", entry.getKey(), entry.getValue());
			}
			monitor.set("total_dbnum", next.getTotalDBCount());
			monitor.set("total_dbexp", next.getTotalExpireCount());
			
			// diff
			setLong("expired_keys", next.getExpiredKeys());
			setLong("evicted_keys", next.getEvictedKeys());
			
			// slow log
			setLong("total_slow_log", next.getTotalSlowLog());
			
			List<XSlowLog> slowLogs = next.getDiffSlowLogs();
			for (XSlowLog slowLog : slowLogs) {
				String[] properties = new String[5];
				properties[0] = String.valueOf(slowLog.getId());
				properties[1] = slowLog.getTimestamp();
				properties[2] = slowLog.getCommand();
				properties[3] = slowLog.getClientName();
				properties[4] = slowLog.getHostAndPort() == null ? "" : slowLog.getHostAndPort().toString();
				monitor.set("slow_log", properties, slowLog.getExecutionTime());
			}
			
			if (next.getDiffTotalSlowLog() > 0) {
				monitor.set("slow_log_latency", (next.getDiffTotalSlowLogExecutionTime() / (next.getDiffTotalSlowLog() * 1d)));
			} else {
				monitor.set("slow_log_latency", 0d);
			}
			
			prev = next;
			delay(15, TimeUnit.SECONDS);
		} catch (JedisConnectionException e) {
			System.err.println("failed to connect to [" + host + ":" + port + "]");
			System.exit(-1);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	private void setLong(String field, Long value) {
		if (value != null) {
			monitor.set(field, value);
		}
	}
	
	private void setDouble(String field, Double value) {
		if (value != null) {
			monitor.set(field, value);
		}
	}
	
	private void setString(String field, String value) {
		if (value != null) {
			monitor.set(field, value);
		}
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
