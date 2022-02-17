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

package com.tair.cli.ext.listener;

import java.io.IOException;
import java.util.function.Consumer;

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.tair.cli.conf.Configure;
import com.tair.cli.escape.JsonEscaper;
import com.tair.cli.ext.XDumpKeyValuePair;
import com.tair.cli.glossary.DataType;
import com.tair.cli.monitor.MonitorFactory;
import com.tair.cli.monitor.MonitorManager;
import com.tair.cli.monitor.entity.Monitor;
import com.tair.cli.util.CmpHeap;
import com.tair.cli.util.OutputStreams;
import com.tair.cli.util.Tuple2Ex;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class MemoryEventListener extends AbstractEventListener implements Consumer<Tuple2Ex> {
	
	private static final Monitor monitor = MonitorFactory.getMonitor("memory_statistics");
	
	private Long bytes;
	private Configure configure;
	private MonitorManager manager;
	private long totalMemory;
	private final CmpHeap<Tuple2Ex> heap;
	private JsonEscaper escaper = new JsonEscaper();
	
	//
	private DB db;
	private boolean firstkey = true;
	
	public MemoryEventListener(Integer limit, Long bytes, Configure configure) {
		this.bytes = bytes;
		this.configure = configure;
		this.manager = new MonitorManager(configure);
		this.manager.open("memory_statistics");
		this.heap = new CmpHeap<>(limit == null ? -1 : limit.intValue());
		this.heap.setConsumer(this);
	}
	
	private void emitString(byte[] str) {
		OutputStreams.write('"', out);
		escaper.encode(str, out);
		OutputStreams.write('"', out);
	}
	
	private void emitNumber(long number) {
		escaper.encode(number, out);
	}
	
	private void emitField(String field, byte[] value) {
		emitField(field.getBytes(), value);
	}
	
	private void emitField(String field, String value) {
		emitField(field.getBytes(), value.getBytes());
	}
	
	private void emitField(byte[] field, byte[] value) {
		emitString(field);
		OutputStreams.write(':', out);
		emitString(value);
	}
	
	private void emitField(String field, long value) {
		emitString(field.getBytes());
		OutputStreams.write(':', out);
		escaper.encode(String.valueOf(value).getBytes(), out);
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		if (event instanceof PreRdbSyncEvent) {
			manager.reset("memory_statistics");
		} else if (event instanceof XDumpKeyValuePair) {
			XDumpKeyValuePair dkv = (XDumpKeyValuePair) event;
			setContext(dkv);
			apply(dkv);
			if (db == null || db.getDbNumber() != dkv.getDb().getDbNumber()) {
				db = dkv.getDb();
				monitor.set("dbnum_" + db.getDbNumber(), db.getDbsize());
				monitor.set("dbexp_" + db.getDbNumber(), db.getExpires());
			}
			totalMemory += dkv.getMemoryUsage();
			
			if (bytes == null || dkv.getMemoryUsage() >= bytes) {
				Tuple2Ex tuple = new Tuple2Ex(dkv.getMemoryUsage(), dkv);
				heap.add(tuple);
			}
		} else if (event instanceof PostRdbSyncEvent) {
			for (Tuple2Ex tuple : heap.get(true)) {
				accept(tuple);
			}
			
			OutputStreams.closeQuietly(out);
			monitor.set("total_memory", totalMemory);
			MonitorManager.closeQuietly(manager);
		}
	}
	
	protected void separator() {
		OutputStreams.write('\n', out);
	}
	
	protected void json(XDumpKeyValuePair context) {
		if (!firstkey) {
			separator();
		}
		firstkey = false;
		OutputStreams.write('{', out);
		emitField("key", context.getKey());
		OutputStreams.write(',', out);
		emitField("memory", context.getMemoryUsage());
		OutputStreams.write(',', out);
		emitField("db", context.getDb().getDbNumber());
		OutputStreams.write(',', out);
		emitField("type", DataType.parse(context.getValueRdbType()).getValue());
		ExpiredType expiry = context.getExpiredType();
		if (expiry != ExpiredType.NONE) {
			OutputStreams.write(',', out);
			if (expiry == ExpiredType.SECOND) {
				emitField("expiry", context.getExpiredValue() * 1000);
			} else {
				emitField("expiry", context.getExpiredValue());
			}
		}
		OutputStreams.write('}', out);
	}
	
	@Override
	public void accept(Tuple2Ex objects) {
		json(objects.getV2());
	}
	
	@Override
	public  <T> T applyString(RedisInputStream in, int version) throws IOException {
		monitor.add("count_string", 1);
		monitor.add("memory_string", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyList(RedisInputStream in, int version) throws IOException {
		monitor.add("count_list", 1);
		monitor.add("memory_list", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applySet(RedisInputStream in, int version) throws IOException {
		monitor.add("count_set", 1);
		monitor.add("memory_set", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
		monitor.add("count_zset", 1);
		monitor.add("memory_zset", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
		monitor.add("count_zset", 1);
		monitor.add("memory_zset", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHash(RedisInputStream in, int version) throws IOException {
		monitor.add("count_hash", 1);
		monitor.add("memory_hash", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
		monitor.add("count_hash", 1);
		monitor.add("memory_hash", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
		monitor.add("count_list", 1);
		monitor.add("memory_list", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
		monitor.add("count_set", 1);
		monitor.add("memory_set", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
		monitor.add("count_zset", 1);
		monitor.add("memory_zset", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
		monitor.add("count_zset", 1);
		monitor.add("memory_zset", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
		monitor.add("count_hash", 1);
		monitor.add("memory_hash", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
		monitor.add("count_hash", 1);
		monitor.add("memory_hash", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
		monitor.add("count_list", 1);
		monitor.add("memory_list", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
		monitor.add("count_list", 1);
		monitor.add("memory_list", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
		monitor.add("count_module", 1);
		monitor.add("memory_module", getContext().getMemoryUsage());
		return (T) getContext();
	}
	
	@Override
	public <T> T applyStreamListPacks(RedisInputStream in, int version) throws IOException {
		monitor.add("count_stream", 1);
		monitor.add("memory_stream", getContext().getMemoryUsage());
		return (T) getContext();
	}
}
