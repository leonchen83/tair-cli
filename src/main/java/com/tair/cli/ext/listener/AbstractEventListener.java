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

import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_LISTPACK;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPMAP;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_QUICKLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_QUICKLIST_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET_INTSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STRING;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_LISTPACK;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_ZIPLIST;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.RdbValueVisitor;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.tair.cli.ext.XDumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public abstract class AbstractEventListener extends RdbValueVisitor implements EventListener {
	
	protected int output = 8192;
	
	protected XDumpKeyValuePair context;
	
	public XDumpKeyValuePair getContext() {
		return context;
	}
	
	public void setContext(XDumpKeyValuePair context) {
		this.context = context;
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		if (event instanceof XDumpKeyValuePair) {
			XDumpKeyValuePair dkv = (XDumpKeyValuePair) event; 
			setContext(dkv);
			apply(dkv);
		}
	}
	
	public void apply(XDumpKeyValuePair kv) {
		Objects.requireNonNull(kv);
		try (RedisInputStream in = new RedisInputStream(new ByteArray(kv.getValue()))) {
			int valueType = in.read();
			switch (valueType) {
				case RDB_TYPE_STRING:
					applyString(in, kv.getVersion());
					break;
				case RDB_TYPE_LIST:
					applyList(in, kv.getVersion());
					break;
				case RDB_TYPE_SET:
					applySet(in, kv.getVersion());
					break;
				case RDB_TYPE_ZSET:
					applyZSet(in, kv.getVersion());
					break;
				case RDB_TYPE_ZSET_2:
					applyZSet2(in, kv.getVersion());
					break;
				case RDB_TYPE_HASH:
					applyHash(in, kv.getVersion());
					break;
				case RDB_TYPE_HASH_ZIPMAP:
					applyHashZipMap(in, kv.getVersion());
					break;
				case RDB_TYPE_LIST_ZIPLIST:
					applyListZipList(in, kv.getVersion());
					break;
				case RDB_TYPE_SET_INTSET:
					applySetIntSet(in, kv.getVersion());
					break;
				case RDB_TYPE_ZSET_ZIPLIST:
					applyZSetZipList(in, kv.getVersion());
					break;
				case RDB_TYPE_ZSET_LISTPACK:
					applyZSetListPack(in, kv.getVersion());
					break;
				case RDB_TYPE_HASH_ZIPLIST:
					applyHashZipList(in, kv.getVersion());
					break;
				case RDB_TYPE_HASH_LISTPACK:
					applyHashListPack(in, kv.getVersion());
					break;
				case RDB_TYPE_LIST_QUICKLIST:
					applyListQuickList(in, kv.getVersion());
					break;
				case RDB_TYPE_LIST_QUICKLIST_2:
					applyListQuickList2(in, kv.getVersion());
					break;
				case RDB_TYPE_MODULE:
					applyModule(in, kv.getVersion());
					break;
				case RDB_TYPE_MODULE_2:
					applyModule2(in, kv.getVersion());
					break;
				case RDB_TYPE_STREAM_LISTPACKS:
					applyStreamListPacks(in, kv.getVersion());
					break;
				default:
					throw new AssertionError("unexpected value type:" + valueType);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	
	public <T> T applyString(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadEncodedStringObject();
		return (T) getContext();
	}
	
	public <T> T applyList(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long len = skipParser.rdbLoadLen().len;
		while (len > 0) {
			skipParser.rdbLoadEncodedStringObject();
			len--;
		}
		return (T) getContext();
	}
	
	public <T> T applySet(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long len = skipParser.rdbLoadLen().len;
		while (len > 0) {
			skipParser.rdbLoadEncodedStringObject();
			len--;
		}
		return (T) getContext();
	}
	
	public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long len = skipParser.rdbLoadLen().len;
		while (len > 0) {
			skipParser.rdbLoadEncodedStringObject();
			skipParser.rdbLoadDoubleValue();
			len--;
		}
		return (T) getContext();
	}
	
	public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long len = skipParser.rdbLoadLen().len;
		while (len > 0) {
			skipParser.rdbLoadEncodedStringObject();
			skipParser.rdbLoadBinaryDoubleValue();
			len--;
		}
		return (T) getContext();
	}
	
	public <T> T applyHash(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long len = skipParser.rdbLoadLen().len;
		while (len > 0) {
			skipParser.rdbLoadEncodedStringObject();
			skipParser.rdbLoadEncodedStringObject();
			len--;
		}
		return (T) getContext();
	}
	
	public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadPlainStringObject();
		return (T) getContext();
	}
	
	public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadPlainStringObject();
		return (T) getContext();
	}
	
	public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadPlainStringObject();
		return (T) getContext();
	}
	
	public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadPlainStringObject();
		return (T) getContext();
	}
	
	public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadPlainStringObject();
		return (T) getContext();
	}
	
	public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadPlainStringObject();
		return (T) getContext();
	}
	
	public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
		new SkipRdbParser(in).rdbLoadPlainStringObject();
		return (T) getContext();
	}
	
	public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long len = skipParser.rdbLoadLen().len;
		for (long i = 0; i < len; i++) {
			skipParser.rdbGenericLoadStringObject();
		}
		return (T) getContext();
	}
	
	public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long len = skipParser.rdbLoadLen().len;
		for (long i = 0; i < len; i++) {
			skipParser.rdbLoadLen();
			skipParser.rdbGenericLoadStringObject();
		}
		return (T) getContext();
	}
	
	public <T> T applyModule(RedisInputStream in, int version) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipRdbParser = new SkipRdbParser(in);
		skipRdbParser.rdbLoadCheckModuleValue();
		return (T) getContext();
	}
	
	public <T> T applyStreamListPacks(RedisInputStream in, int version) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long listPacks = skipParser.rdbLoadLen().len;
		while (listPacks-- > 0) {
			skipParser.rdbLoadPlainStringObject();
			skipParser.rdbLoadPlainStringObject();
		}
		skipParser.rdbLoadLen();
		skipParser.rdbLoadLen();
		skipParser.rdbLoadLen();
		long groupCount = skipParser.rdbLoadLen().len;
		while (groupCount-- > 0) {
			skipParser.rdbLoadPlainStringObject();
			skipParser.rdbLoadLen();
			skipParser.rdbLoadLen();
			long groupPel = skipParser.rdbLoadLen().len;
			while (groupPel-- > 0) {
				in.skip(16);
				skipParser.rdbLoadMillisecondTime();
				skipParser.rdbLoadLen();
			}
			long consumerCount = skipParser.rdbLoadLen().len;
			while (consumerCount-- > 0) {
				skipParser.rdbLoadPlainStringObject();
				skipParser.rdbLoadMillisecondTime();
				long consumerPel = skipParser.rdbLoadLen().len;
				while (consumerPel-- > 0) {
					in.skip(16);
				}
			}
		}
		return (T) getContext();
	}
}
