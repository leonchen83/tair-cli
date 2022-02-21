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

import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.Constants.STAR;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static com.tair.cli.ext.RedisConstants.REPLACE_BUF;
import static com.tair.cli.ext.RedisConstants.RESTORE_BUF;
import static com.tair.cli.ext.RedisConstants.SELECT;
import static com.tair.cli.ext.RedisConstants.ZERO_BUF;
import static java.nio.ByteBuffer.wrap;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.ByteBufferOutputStream;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;
import com.tair.cli.conf.Configure;
import com.tair.cli.escape.RawEscaper;
import com.tair.cli.ext.XDumpKeyValuePair;
import com.tair.cli.ext.enterprise.Tair;
import com.tair.cli.ext.enterprise.Tairs;
import com.tair.cli.io.CRCOutputStream;
import com.tair.cli.io.LayeredOutputStream;
import com.tair.cli.util.ByteBuffers;
import com.tair.cli.util.OutputStreams;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class DumpEventListener extends AbstractEventListener {
	
	private static Logger logger = LoggerFactory.getLogger(DumpEventListener.class);
	
	private boolean replace;
	private boolean convert;
	private Integer rdbVersion;
	private Configure configure;
	private Escaper escaper = new RawEscaper();
	
	//
	private DB db;
	
	public DumpEventListener(boolean replace, Integer rdbVersion, boolean convert, Configure configure) {
		this.replace = replace;
		this.rdbVersion = rdbVersion;
		this.convert = convert;
		this.configure = configure;
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		if (event instanceof XDumpKeyValuePair) {
			XDumpKeyValuePair dkv = (XDumpKeyValuePair) event;
			if (db == null || db.getDbNumber() != dkv.getDb().getDbNumber()) {
				db = dkv.getDb();
				emit(this.out, SELECT, String.valueOf(db.getDbNumber()).getBytes());
			}
			setContext(dkv);
			apply(dkv);
		} else if (event instanceof PostRdbSyncEvent) {
			OutputStreams.flushQuietly(out);
			OutputStreams.closeQuietly(out);
		}
	}
	
	private int getVersion(int version) {
		return this.rdbVersion == -1 ? version : rdbVersion;
	}
	
	@Override
	public <T> T applyString(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyString(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyString(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyList(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyList(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyList(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applySet(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applySet(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applySet(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyZSet(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyZSet(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyZSet2(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			if (getVersion(version) < 8 /* since redis rdb version 8 */) {
				// downgrade to RDB_TYPE_ZSET
				BaseRdbParser parser = new BaseRdbParser(in);
				BaseRdbEncoder encoder = new BaseRdbEncoder();
				ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
				long len = parser.rdbLoadLen().len;
				long temp = len;
				while (len > 0) {
					ByteArray element = parser.rdbLoadEncodedStringObject();
					encoder.rdbGenericSaveStringObject(element, out1);
					double score = parser.rdbLoadBinaryDoubleValue();
					encoder.rdbSaveDoubleValue(score, out1);
					len--;
				}
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper, false)) {
					listener.write((byte) RDB_TYPE_ZSET);
					listener.handle(encoder.rdbSaveLen(temp));
					listener.handle(out1.toByteBuffer());
				}
			} else {
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
					listener.write((byte) context.getValueRdbType());
					super.applyZSet2(in, version);
				}
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyHash(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyHash(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyHash(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyHashZipMap(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyHashZipMap(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyListZipList(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyListZipList(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applySetIntSet(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applySetIntSet(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyZSetZipList(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyZSetZipList(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyZSetListPack(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			if (getVersion(version) < 10 /* since redis rdb version 10 */) {
				// downgrade to RDB_TYPE_ZSET
				BaseRdbParser parser = new BaseRdbParser(in);
				BaseRdbEncoder encoder = new BaseRdbEncoder();
				ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
				RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
				listPack.skip(4); // total-bytes
				int len = listPack.readInt(2);
				long targetLen = len / 2;
				while (len > 0) {
					byte[] element = listPackEntry(listPack);
					encoder.rdbGenericSaveStringObject(new ByteArray(element), out1);
					len--;
					double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
					encoder.rdbSaveDoubleValue(score, out1);
					len--;
				}
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper, false)) {
					listener.write((byte) RDB_TYPE_ZSET);
					listener.handle(encoder.rdbSaveLen(targetLen));
					listener.handle(out1.toByteBuffer());
				}
			} else {
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
					listener.write((byte) context.getValueRdbType());
					super.applyZSetListPack(in, version);
				}
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyHashZipList(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyHashZipList(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyHashListPack(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			if (getVersion(version) < 10 /* since redis rdb version 10 */) {
				// downgrade to RDB_TYPE_HASH
				BaseRdbParser parser = new BaseRdbParser(in);
				BaseRdbEncoder encoder = new BaseRdbEncoder();
				ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
				RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
				listPack.skip(4); // total-bytes
				int len = listPack.readInt(2);
				long targetLen = len / 2;
				while (len > 0) {
					byte[] field = listPackEntry(listPack);
					encoder.rdbGenericSaveStringObject(new ByteArray(field), out1);
					len--;
					byte[] value = listPackEntry(listPack);
					encoder.rdbGenericSaveStringObject(new ByteArray(value), out1);
					len--;
				}
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper, false)) {
					listener.write((byte) RDB_TYPE_HASH);
					listener.handle(encoder.rdbSaveLen(targetLen));
					listener.handle(out1.toByteBuffer());
				}
			} else {
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
					listener.write((byte) context.getValueRdbType());
					super.applyHashListPack(in, version);
				}
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyListQuickList(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			if (getVersion(version) < 7 /* since redis rdb version 7 */) {
				BaseRdbParser parser = new BaseRdbParser(in);
				BaseRdbEncoder encoder = new BaseRdbEncoder();
				ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
				int total = 0;
				long len = parser.rdbLoadLen().len;
				for (long i = 0; i < len; i++) {
					RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
					
					BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
					BaseRdbParser.LenHelper.zltail(stream); // zltail
					int zllen = BaseRdbParser.LenHelper.zllen(stream);
					for (int j = 0; j < zllen; j++) {
						byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
						encoder.rdbGenericSaveStringObject(new ByteArray(e), out1);
						total++;
					}
					int zlend = BaseRdbParser.LenHelper.zlend(stream);
					if (zlend != 255) {
						throw new AssertionError("zlend expect 255 but " + zlend);
					}
				}
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper, false)) {
					listener.write((byte) RDB_TYPE_LIST);
					listener.handle(encoder.rdbSaveLen(total));
					listener.handle(out1.toByteBuffer());
				}
			} else {
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
					listener.write((byte) context.getValueRdbType());
					super.applyListQuickList(in, version);
				}
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyListQuickList2(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			if (getVersion(version) < 10 /* since redis rdb version 10 */) {
				// downgrade to RDB_TYPE_LIST
				BaseRdbParser parser = new BaseRdbParser(in);
				BaseRdbEncoder encoder = new BaseRdbEncoder();
				ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
				int total = 0;
				long len = parser.rdbLoadLen().len;
				for (long i = 0; i < len; i++) {
					long container = parser.rdbLoadLen().len;
					ByteArray bytes = parser.rdbLoadPlainStringObject();
					if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
						encoder.rdbGenericSaveStringObject(new ByteArray(bytes.first()), out1);
						total++;
					} else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
						RedisInputStream listPack = new RedisInputStream(bytes);
						listPack.skip(4); // total-bytes
						int innerLen = listPack.readInt(2);
						for (int j = 0; j < innerLen; j++) {
							byte[] e = listPackEntry(listPack);
							encoder.rdbGenericSaveStringObject(new ByteArray(e), out1);
							total++;
						}
						int lpend = listPack.read(); // lp-end
						if (lpend != 255) {
							throw new AssertionError("listpack expect 255 but " + lpend);
						}
					} else {
						throw new UnsupportedOperationException(String.valueOf(container));
					}
				}
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper, false)) {
					listener.write((byte) RDB_TYPE_LIST);
					listener.handle(encoder.rdbSaveLen(total));
					listener.handle(out1.toByteBuffer());
				}
			} else {
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
					listener.write((byte) context.getValueRdbType());
					super.applyListQuickList2(in, version);
				}
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
	}
	
	@Override
	public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyModule2(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		if (!convert) {
			try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
				try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
					listener.write((byte) context.getValueRdbType());
					super.applyModule2(in, version);
				}
				emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
				return (T) getContext();
			}
		} else {
			Tair tair = Tairs.get(in);
			try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
				CRCOutputStream crcOut = new CRCOutputStream(out, escaper);
				OutputStreams.writeQuietly(tair.type(), crcOut);
				tair.convertToRdbValue(in, crcOut);
				OutputStreams.writeQuietly(getVersion(version), crcOut);
				OutputStreams.writeQuietly(0x00, crcOut);
				OutputStreams.writeQuietly(crcOut.getCRC64(), crcOut);
				emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
				return (T) getContext();
			}
		}
	}
	
	@Override
	public <T> T applyStreamListPacks(RedisInputStream in, int version) throws IOException {
		ByteBuffer ex = ZERO_BUF;
		if (context.getExpiredValue() != null) {
			long ms = context.getExpiredValue() - System.currentTimeMillis();
			if (ms <= 0) {
				return super.applyStreamListPacks(in, version);
			} else {
				ex = wrap(String.valueOf(ms).getBytes());
			}
		}
		
		try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
			try (DumpRawByteListener listener = new DumpRawByteListener(in, getVersion(version), out, escaper)) {
				listener.write((byte) context.getValueRdbType());
				super.applyStreamListPacks(in, version);
			}
			if (getVersion(version) >= 9) {
				emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			} else {
				logger.error("skip generate stream type key [{}] to dump format. target rdb version [{}] too small", Strings.toString(getContext().getKey()), getVersion(version));
			}
			return (T) getContext();
		}
	}
	
	protected void emit(OutputStream out, ByteBuffer command, ByteBuffer key, ByteBuffer ex, ByteBuffers value, boolean replace) {
		OutputStreams.write(STAR, out);
		if (replace) {
			OutputStreams.write("5".getBytes(), out);
		} else {
			OutputStreams.write("4".getBytes(), out);
		}
		
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		
		emitArg(command);
		emitArg(key);
		emitArg(ex);
		emitArg(value);
		if (replace) {
			emitArg(REPLACE_BUF);
		}
	}
	
	protected void emitArg(ByteBuffer arg) {
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(arg.remaining()).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(arg.array(), arg.position(), arg.limit(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
	}
	
	protected void emitArg(ByteBuffers value) {
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(value.getSize()).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		while (value.getBuffers().hasNext()) {
			ByteBuffer buf = value.getBuffers().next();
			OutputStreams.write(buf.array(), buf.position(), buf.limit(), out);
		}
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
	}
	
	protected void emit(OutputStream out, byte[] command, byte[]... ary) {
		OutputStreams.write(STAR, out);
		OutputStreams.write(String.valueOf(ary.length + 1).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(command.length).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(command, out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		for (final byte[] arg : ary) {
			OutputStreams.write(DOLLAR, out);
			OutputStreams.write(String.valueOf(arg.length).getBytes(), out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
			OutputStreams.write(arg, out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
		}
	}
}
