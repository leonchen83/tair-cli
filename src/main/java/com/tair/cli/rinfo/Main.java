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

package com.tair.cli.rinfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

/**
 * @author Baoyi Chen
 */
public class Main {
	public static void main(String[] args) {
		JedisCluster cluster = new JedisCluster(new HostAndPort("127.0.0.1", 30001));
		XClusterRedisInfo prev = XClusterRedisInfo.EMPTY_CLUSTER;
		
		for (int i = 0; i < 3; i++) {
			List<String> clusterInfos = new ArrayList<>();
			AtomicReference<String> clusterNodes = new AtomicReference<>();
			List<XStandaloneRedisInfo> xinfos = new ArrayList<>();
			cluster.getClusterNodes().forEach((k, v) -> {
				try(Jedis jedis = new Jedis(v.getResource())) {
					clusterInfos.add(jedis.clusterInfo());
					clusterNodes.compareAndSet(null, jedis.clusterNodes());
					String info = jedis.info();
					List<String> maxclients = jedis.configGet("maxclients");
					long len = jedis.slowlogLen();
					List<Object> binaryLogs = jedis.slowlogGetBinary(128);
					XStandaloneRedisInfo next = XStandaloneRedisInfo.valueOf(info, maxclients, len, binaryLogs, k);
					xinfos.add(next);
				}
			});
			XClusterRedisInfo next = XClusterRedisInfo.valueOf(xinfos, clusterNodes.get(), clusterInfos);
			next = XClusterRedisInfo.diff(prev, next);
			prev = next;
			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(next);
		}
		cluster.close();
	}
}
