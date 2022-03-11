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

import static com.tair.cli.rinfo.XClusterRedisInfo.EMPTY_CLUSTER;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.tair.cli.conf.Configure;
import com.tair.cli.monitor.Monitor;
import com.tair.cli.monitor.MonitorFactory;
import com.tair.cli.monitor.MonitorManager;
import com.tair.cli.rinfo.XClusterRedisInfo;
import com.tair.cli.rinfo.XStandaloneRedisInfo;
import com.tair.cli.rinfo.support.XClusterNodes;
import com.tair.cli.rinfo.support.XSlowLog;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class XClusterMonitorCommand implements Runnable, Closeable {
	
	private static final Monitor monitor = MonitorFactory.getMonitor("tair_monitor");
	
	private Configure configure;
	private MonitorManager manager;
	protected volatile JedisCluster jedisCluster;
	protected XClusterRedisInfo prev = EMPTY_CLUSTER;
	protected final DefaultJedisClientConfig config;
	
	public XClusterMonitorCommand(RedisURI uri, Configure configure) {
		this.configure = configure;
		this.manager = new MonitorManager(configure);
		this.manager.open();
		Configuration configuration = configure.merge(uri, true);
		DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder();
		builder.user(configuration.getAuthUser()).password(configuration.getAuthPassword());
		builder.ssl(configuration.isSsl()).sslSocketFactory(configuration.getSslSocketFactory());
		builder.connectionTimeoutMillis(configure.getTimeout());
		this.config = builder.build();
		HostAndPort hp = new HostAndPort(uri.getHost(), uri.getPort());
		try {
			this.jedisCluster = new JedisCluster(Collections.singleton(hp), config);
		} catch (JedisConnectionException e) {
			System.err.println("failed to connect to [" + hp + "]");
			System.exit(-1);
		}
	}
	
	@Override
	public void run() {
		try {
			List<String> clusterInfos = new ArrayList<>();
			AtomicReference<String> clusterNodes = new AtomicReference<>();
			List<XStandaloneRedisInfo> xinfos = new ArrayList<>();
			jedisCluster.getClusterNodes().forEach((k, v) -> {
				try (Jedis jedis = new Jedis(v.getResource())) {
					clusterInfos.add(jedis.clusterInfo());
					clusterNodes.compareAndSet(null, jedis.clusterNodes());
					String info = jedis.info();
					List<String> maxclients = jedis.configGet("maxclients");
					long len = jedis.slowlogLen();
					List<Object> binaryLogs = jedis.slowlogGetBinary(128);
					XStandaloneRedisInfo next = XStandaloneRedisInfo.valueOf(info, maxclients, len, binaryLogs, k);
					xinfos.add(next);
				} catch (Throwable e) {
				}
			});
			XClusterRedisInfo next = XClusterRedisInfo.valueOf(xinfos, clusterNodes.get(), clusterInfos);
			next = XClusterRedisInfo.diff(prev, next);
			
			setString("cluster_state", next.getClusterInfo().getClusterState());
			setLong("cluster_slot_assigned", next.getClusterInfo().getClusterSlotsAssigned());
			setLong("cluster_slot_ok", next.getClusterInfo().getClusterSlotsOk());
			setLong("cluster_slot_fail", next.getClusterInfo().getClusterSlotsFail());
			setLong("cluster_slot_pfail", next.getClusterInfo().getClusterSlotsPfail());
			setLong("cluster_known_nodes", next.getClusterInfo().getClusterKnownNodes());
			setLong("cluster_size", next.getClusterInfo().getClusterSize());
			setLong("cluster_current_epoch", next.getClusterInfo().getClusterCurrentEpoch());
			setLong("cluster_stats_messages_received", next.getClusterInfo().getClusterStatsMessagesReceived());
			setLong("cluster_stats_messages_sent", next.getClusterInfo().getClusterStatsMessagesSent());
			
			for (XClusterNodes nodes : next.getClusterNodes()) {
				if (nodes.isMaster()) {
					String[] properties = new String[]{"master", nodes.getHostAndPort()};
					setLong("cluster_nodes_slot", properties, (long) nodes.getSlots().size());
					setLong("cluster_nodes_migrating_slot", properties, (long) nodes.getMigratingSlots().size());
				}
			}
			
			// server
			setLong("cluster_uptime_in_seconds", next.getMaster().getUptimeInSeconds());
			setString("cluster_redis_version", next.getMaster().getRedisVersion());
			setLong("cluster_maxclients", next.getMaster().getMaxclients());
			setLong("cluster_total_system_memory", next.getMaster().getTotalSystemMemory());
			
			for (Map.Entry<String, XStandaloneRedisInfo> entry : next.getMasters().entrySet()) {
				String ip = entry.getKey();
				XStandaloneRedisInfo value = entry.getValue();
				setMonitor(ip, value, "master");
			}
			
			for (Map.Entry<String, XStandaloneRedisInfo> entry : next.getSlaves().entrySet()) {
				String ip = entry.getKey();
				XStandaloneRedisInfo value = entry.getValue();
				setMonitor(ip, value, "slave");
			}
			
			prev = next;
			delay(15, TimeUnit.SECONDS);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	private void setMonitor(String ip, XStandaloneRedisInfo value, String role) {
		String[] properties = new String[]{role, ip};
		
		setString("cluster_nodes", ip, role);
		setLong("cluster_connected_clients", properties, value.getConnectedClients());
		setLong("cluster_blocked_clients", properties, value.getBlockedClients());
		setLong("cluster_tracking_clients", properties, value.getTrackingClients());
		setLong("cluster_maxmemory", properties, value.getMaxmemory());
		setLong("cluster_used_memory", properties, value.getUsedMemory());
		setLong("cluster_used_memory_rss", properties, value.getUsedMemoryRss());
		setLong("cluster_used_memory_peak", properties, value.getUsedMemoryPeak());
		setLong("cluster_used_memory_dataset", properties, value.getUsedMemoryDataset());
		setLong("cluster_used_memory_lua", properties, value.getUsedMemoryLua());
		setLong("cluster_used_memory_functions", properties, value.getUsedMemoryFunctions());
		setLong("cluster_used_memory_scripts", properties, value.getUsedMemoryScripts());
		setLong("cluster_mem_fragmentation_bytes", properties, value.getMemFragmentationBytes());
		setLong("cluster_total_connections_received", properties, value.getTotalConnectionsReceived());
		setLong("cluster_total_commands_processed", properties, value.getTotalCommandsProcessed());
		setLong("cluster_total_reads_processed", properties, value.getTotalReadsProcessed());
		setLong("cluster_total_writes_processed", properties, value.getTotalWritesProcessed());
		setLong("cluster_total_error_replies", properties, value.getTotalErrorReplies());
		Long hits = value.getKeyspaceHits();
		Long misses = value.getKeyspaceMisses();
		if (hits != null && misses != null) {
			monitor.set("cluster_keyspace_hit_rate", properties, hits * 1d / (hits + misses));
		}
		
		setDouble("cluster_used_cpu_sys", properties, value.getUsedCpuSys());
		setDouble("cluster_used_cpu_user", properties, value.getUsedCpuUser());
		setDouble("cluster_used_cpu_sys_children", properties, value.getUsedCpuSysChildren());
		setDouble("cluster_used_cpu_user_children", properties, value.getUsedCpuUserChildren());
		
		for (Map.Entry<String, Long> e : value.getDbInfo().entrySet()) {
			monitor.set("cluster_dbnum", properties, e.getValue());
		}
		for (Map.Entry<String, Long> e : value.getDbExpireInfo().entrySet()) {
			monitor.set("cluster_dbexp", properties, e.getValue());
		}
		
		setLong("cluster_expired_keys", properties, value.getExpiredKeys());
		setLong("cluster_evicted_keys", properties, value.getEvictedKeys());
		setLong("cluster_total_slow_log", properties, value.getTotalSlowLog());
		
		List<XSlowLog> slowLogs = value.getDiffSlowLogs();
		for (XSlowLog slowLog : slowLogs) {
			String[] properties1 = new String[7];
			properties1[0] = String.valueOf(slowLog.getId());
			properties1[1] = slowLog.getTimestamp();
			properties1[2] = slowLog.getCommand();
			properties1[3] = slowLog.getClientName();
			properties1[4] = slowLog.getHostAndPort();
			properties1[5] = properties[0];
			properties1[6] = properties[1];
			monitor.set("cluster_slow_log", properties1, slowLog.getExecutionTime());
		}
		
		if (value.getDiffTotalSlowLog() > 0) {
			monitor.set("cluster_slow_log_latency", properties, (value.getDiffTotalSlowLogExecutionTime() / (value.getDiffTotalSlowLog() * 1d)));
		} else {
			monitor.set("cluster_slow_log_latency", properties, 0d);
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
	
	private void setLong(String field, String property, Long value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setDouble(String field, String property, Double value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setString(String field, String property, String value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setLong(String field, String[] property, Long value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setDouble(String field, String[] property, Double value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setString(String field, String[] property, String value) {
		if (value != null) {
			monitor.set(field, property, value);
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
			if (jedisCluster != null) {
				jedisCluster.close();
			}
		} catch (Throwable e) {
		}
		MonitorManager.closeQuietly(manager);
	}
}
