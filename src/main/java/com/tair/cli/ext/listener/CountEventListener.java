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

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.tair.cli.conf.Configure;
import com.tair.cli.escape.JsonEscaper;
import com.tair.cli.ext.XDumpKeyValuePair;
import com.tair.cli.util.OutputStreams;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class CountEventListener extends AbstractEventListener {
	
	private long count;
	private long strCount;
	private long setCount;
	private long listCount;
	private long hashCount;
	private long zsetCount;
	private long moduleCount;
	private long streamCount;
	private Configure configure;
	private Escaper escaper = new JsonEscaper();
	
	public CountEventListener(Configure configure) {
		this.configure = configure;
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		if (event instanceof XDumpKeyValuePair) {
			XDumpKeyValuePair dkv = (XDumpKeyValuePair) event;
			setContext(dkv);
			apply(dkv);
		} else if (event instanceof PostRdbSyncEvent) {
			json();
			OutputStreams.flushQuietly(out);
			OutputStreams.closeQuietly(out);
		}
	}
	
	protected void json() {
		OutputStreams.write('{', out);
		emitField("total", count);
		if (strCount > 0) {
			OutputStreams.write(',', out);
			emitField("string", strCount);
		}
		if (setCount > 0) {
			OutputStreams.write(',', out);
			emitField("set", setCount);
		}
		if (listCount > 0) {
			OutputStreams.write(',', out);
			emitField("list", listCount);
		}
		if (hashCount > 0) {
			OutputStreams.write(',', out);
			emitField("hash", hashCount);
		}
		if (zsetCount > 0) {
			OutputStreams.write(',', out);
			emitField("zset", zsetCount);
		}
		if (moduleCount > 0) {
			OutputStreams.write(',', out);
			emitField("module", moduleCount);
		}
		if (streamCount > 0) {
			OutputStreams.write(',', out);
			emitField("stream", streamCount);
		}
		OutputStreams.write('}', out);
		separator();
	}
	
	private void emitField(String field, long value) {
		emitString(field.getBytes());
		OutputStreams.write(':', out);
		escaper.encode(String.valueOf(value).getBytes(), out);
	}
	
	private void emitString(byte[] str) {
		OutputStreams.write('"', out);
		escaper.encode(str, out);
		OutputStreams.write('"', out);
	}
	
	protected void separator() {
		OutputStreams.write('\n', out);
	}
	
	@Override
	public  <T> T applyString(RedisInputStream in, int version) throws IOException {
		count += 1;
		strCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyList(RedisInputStream in, int version) throws IOException {
		count += 1;
		listCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applySet(RedisInputStream in, int version) throws IOException {
		count += 1;
		setCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
		count += 1;
		zsetCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
		count += 1;
		zsetCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHash(RedisInputStream in, int version) throws IOException {
		count += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
		count += 1;
		hashCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
		count += 1;
		listCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
		count += 1;
		setCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
		count += 1;
		zsetCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
		count += 1;
		zsetCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
		count += 1;
		hashCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
		count += 1;
		hashCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
		count += 1;
		listCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
		count += 1;
		listCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
		count += 1;
		moduleCount += 1;
		return (T) getContext();
	}
	
	@Override
	public <T> T applyStreamListPacks(RedisInputStream in, int version) throws IOException {
		count += 1;
		streamCount += 1;
		return (T) getContext();
	}
}
