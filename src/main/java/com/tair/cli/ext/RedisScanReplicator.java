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

import static com.moilioncircle.redis.replicator.Status.CONNECTED;
import static com.moilioncircle.redis.replicator.Status.DISCONNECTED;
import static com.moilioncircle.redis.replicator.Status.DISCONNECTING;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import com.moilioncircle.redis.replicator.AbstractReplicator;
import com.moilioncircle.redis.replicator.DefaultExceptionListener;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.util.Strings;
import com.tair.cli.conf.Configure;
import com.tair.cli.glossary.DataType;
import com.tair.cli.scan.ScanIterator;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;

/**
 * @author Baoyi Chen
 */
public class RedisScanReplicator extends AbstractReplicator {
	protected Jedis jedis;
	protected final int port;
	protected final String host;
	protected final Filter filter;
	protected final Configure configure;
	protected final DefaultJedisClientConfig config;
	
	public RedisScanReplicator(String uri, Configure configure, Filter filter) throws URISyntaxException {
		this(new RedisURI(uri), configure, filter);
	}
	
	public RedisScanReplicator(RedisURI uri, Configure configure, Filter filter) {
		this.filter = filter;
		this.configure = configure;
		this.configuration = configure.merge(uri, true);
		this.host = uri.getHost();
		this.port = uri.getPort();
		DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder();
		builder.user(configuration.getAuthUser()).password(configuration.getAuthPassword());
		builder.ssl(configuration.isSsl()).sslSocketFactory(configuration.getSslSocketFactory());
		builder.connectionTimeoutMillis(configure.getTimeout());
		this.config = builder.build();
		if (configuration.isUseDefaultExceptionListener())
			addExceptionListener(new DefaultExceptionListener());
	}
	
	public String getHost() {
		return this.host;
	}
	
	public int getPort() {
		return this.port;
	}
	
	protected boolean containsType(int type) {
		return DataType.contains(filter.types, type);
	}
	
	protected boolean containsKey(String key) {
		for (Pattern pattern : filter.regexs) {
			if (pattern.matcher(key).matches()) return true;
		}
		return false;
	}
	
	protected boolean contains(XDumpKeyValuePair kv) {
		return containsType(kv.getValueRdbType()) && containsKey(Strings.toString(kv.getKey()));
	}
	
	@Override
	protected void doOpen() throws IOException {
		try {
			this.jedis = new Jedis(host, port, config);
			ScanIterator iterator = new ScanIterator(filter.dbs, jedis, configure.getBatchSize());
			submitEvent(new PreRdbSyncEvent());
			while (iterator.hasNext() && getStatus() == CONNECTED) {
				XDumpKeyValuePair kv = iterator.next();
				if (contains(kv)) submitEvent(kv);
			}
			submitEvent(new PostRdbSyncEvent());
		} catch (Throwable e) {
			throw new IOException(e);
		}
	}
	
	@Override
	protected void doClose() throws IOException {
		compareAndSet(CONNECTED, DISCONNECTING);
		try {
			if (jedis != null) jedis.close();
		} catch (Throwable ignore) {
			/*NOP*/
		} finally {
			setStatus(DISCONNECTED);
		}
	}
}
