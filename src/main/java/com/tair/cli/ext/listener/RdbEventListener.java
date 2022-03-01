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

import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.RDB_OPCODE_EXPIRETIME_MS;
import static com.moilioncircle.redis.replicator.Constants.RDB_OPCODE_RESIZEDB;
import static com.moilioncircle.redis.replicator.Constants.RDB_OPCODE_SELECTDB;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.MS;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.ByteBufferOutputStream;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.tair.cli.conf.Configure;
import com.tair.cli.escape.RawEscaper;
import com.tair.cli.ext.XDumpKeyValuePair;
import com.tair.cli.ext.enterprise.Tair;
import com.tair.cli.ext.enterprise.Tairs;
import com.tair.cli.io.CRCOutputStream;
import com.tair.cli.util.OutputStreams;
import com.tair.cli.util.Strings;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class RdbEventListener extends AbstractEventListener {
	
	private static Logger logger = LoggerFactory.getLogger(RdbEventListener.class);
	
	private boolean convert;
	private Integer rdbVersion;
	private Configure configure;
	private CRCOutputStream crcOut;
	private boolean writeVersion = false;
	private BaseRdbEncoder encoder = new BaseRdbEncoder();
	
	private DB db;
	
	public RdbEventListener(Integer rdbVersion, boolean convert, Configure configure) {
		this.convert = convert;
		this.configure = configure;
		this.rdbVersion = rdbVersion;
		this.crcOut = new CRCOutputStream(out, new RawEscaper());
	}
	
	private int getVersion(int version) {
		return this.rdbVersion == -1 ? version : rdbVersion;
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		try {
			if (event instanceof PreRdbSyncEvent) {
				OutputStreams.writeQuietly("REDIS".getBytes(), crcOut);
			} else if (event instanceof XDumpKeyValuePair) {
				XDumpKeyValuePair dkv = (XDumpKeyValuePair) event;
				setContext(dkv);
				
				if (!writeVersion) {
					String version = Strings.lappend(getVersion(dkv.getVersion()), 4, '0');
					OutputStreams.writeQuietly(version.getBytes(), crcOut);
					writeVersion = true;
				}
				
				if (db == null || db.getDbNumber() != dkv.getDb().getDbNumber()) {
					db = dkv.getDb();
					OutputStreams.write(RDB_OPCODE_SELECTDB, crcOut);
					encoder.rdbSaveLen(db.getDbNumber(), crcOut);
					if (rdbVersion >= 7) {
						OutputStreams.write(RDB_OPCODE_RESIZEDB, crcOut);
						encoder.rdbSaveLen(db.getDbsize(), crcOut);
						encoder.rdbSaveLen(db.getExpires(), crcOut);
					}
				}
				if (dkv.getExpiredType() == MS) {
					OutputStreams.write(RDB_OPCODE_EXPIRETIME_MS, crcOut);
					encoder.rdbSaveMillisecondTime(dkv.getExpiredMs(), crcOut);
				}
				apply(dkv);
			} else if (event instanceof PostRdbSyncEvent) {
				OutputStreams.writeQuietly(Constants.RDB_OPCODE_EOF, crcOut);
				OutputStreams.writeQuietly(crcOut.getCRC64(), crcOut);
				OutputStreams.flushQuietly(crcOut);
				OutputStreams.closeQuietly(crcOut);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	
	private static class XRawByteListener implements RawByteListener {
		
		private OutputStream out;
		
		public XRawByteListener(OutputStream out) {
			this.out = out;
		}
		
		@Override
		public void handle(byte... rawBytes) {
			OutputStreams.writeQuietly(rawBytes, out);
		}
		
		public void handle(ByteBuffer buf) {
			OutputStreams.write(buf.array(), buf.position(), buf.limit(), out);
		}
	}
	
	private void applyKey(int type) throws IOException {
		OutputStreams.write(type, crcOut);
		encoder.rdbGenericSaveStringObject(new ByteArray(getContext().getKey()), crcOut);
	}
	
	public <T> T applyString(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyString(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyList(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyList(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applySet(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applySet(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyZSet(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
		if (getVersion(version) < 8 /* since redis rdb version 8 */) {
			// downgrade to RDB_TYPE_ZSET
			applyKey(RDB_TYPE_ZSET);
			BaseRdbParser parser = new BaseRdbParser(in);
			BaseRdbEncoder encoder = new BaseRdbEncoder();
			long len = parser.rdbLoadLen().len;
			encoder.rdbSaveLen(len, crcOut);
			while (len > 0) {
				ByteArray element = parser.rdbLoadEncodedStringObject();
				encoder.rdbGenericSaveStringObject(element, crcOut);
				double score = parser.rdbLoadBinaryDoubleValue();
				encoder.rdbSaveDoubleValue(score, crcOut);
				len--;
			}
		} else {
			applyKey(getContext().getValueRdbType());
			XRawByteListener listener = new XRawByteListener(crcOut);
			try {
				in.setRawByteListeners(Arrays.asList(listener));
				super.applyZSet2(in, version);
			} finally {
				in.setRawByteListeners(null);
			}
		}
		return (T) getContext();
	}
	
	public <T> T applyHash(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyHash(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyHashZipMap(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyListZipList(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applySetIntSet(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyZSetZipList(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
		if (getVersion(version) < 10 /* since redis rdb version 10 */) {
			// downgrade to RDB_TYPE_ZSET
			applyKey(RDB_TYPE_ZSET);
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
				double score = Double.valueOf(com.moilioncircle.redis.replicator.util.Strings.toString(listPackEntry(listPack)));
				encoder.rdbSaveDoubleValue(score, out1);
				len--;
			}
			XRawByteListener listener = new XRawByteListener(crcOut);
			listener.handle(encoder.rdbSaveLen(targetLen));
			listener.handle(out1.toByteBuffer());
		} else {
			applyKey(getContext().getValueRdbType());
			XRawByteListener listener = new XRawByteListener(crcOut);
			try {
				in.setRawByteListeners(Arrays.asList(listener));
				super.applyZSetListPack(in, version);
			} finally {
				in.setRawByteListeners(null);
			}
		}
		return (T) getContext();
	}
	
	public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
		applyKey(getContext().getValueRdbType());
		XRawByteListener listener = new XRawByteListener(crcOut);
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			super.applyHashZipList(in, version);
		} finally {
			in.setRawByteListeners(null);
		}
		return (T) getContext();
	}
	
	public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
		if (getVersion(version) < 10 /* since redis rdb version 10 */) {
			// downgrade to RDB_TYPE_HASH
			applyKey(RDB_TYPE_HASH);
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
			XRawByteListener listener = new XRawByteListener(crcOut);
			listener.handle(encoder.rdbSaveLen(targetLen));
			listener.handle(out1.toByteBuffer());
		} else {
			applyKey(getContext().getValueRdbType());
			XRawByteListener listener = new XRawByteListener(crcOut);
			try {
				in.setRawByteListeners(Arrays.asList(listener));
				super.applyHashListPack(in, version);
			} finally {
				in.setRawByteListeners(null);
			}
		}
		return (T) getContext();
	}
	
	public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
		if (getVersion(version) < 7 /* since redis rdb version 7 */) {
			// downgrade to RDB_TYPE_LIST
			applyKey(RDB_TYPE_LIST);
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
			XRawByteListener listener = new XRawByteListener(crcOut);
			listener.handle(encoder.rdbSaveLen(total));
			listener.handle(out1.toByteBuffer());
		} else {
			applyKey(getContext().getValueRdbType());
			XRawByteListener listener = new XRawByteListener(crcOut);
			try {
				in.setRawByteListeners(Arrays.asList(listener));
				super.applyListQuickList(in, version);
			} finally {
				in.setRawByteListeners(null);
			}
		}
		return (T) getContext();
	}
	
	public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
		if (getVersion(version) < 10 /* since redis rdb version 10 */) {
			// downgrade to RDB_TYPE_LIST
			applyKey(RDB_TYPE_LIST);
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
			XRawByteListener listener = new XRawByteListener(crcOut);
			listener.handle(encoder.rdbSaveLen(total));
			listener.handle(out1.toByteBuffer());
		} else {
			applyKey(getContext().getValueRdbType());
			XRawByteListener listener = new XRawByteListener(crcOut);
			try {
				in.setRawByteListeners(Arrays.asList(listener));
				super.applyListQuickList2(in, version);
			} finally {
				in.setRawByteListeners(null);
			}
		}
		return (T) getContext();
	}
	
	public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
		if (!convert) {
			if (getVersion(version) < 8) {
				logger.error("skip generate module type key [{}] to rdb format. target rdb version [{}] too small", com.moilioncircle.redis.replicator.util.Strings.toString(getContext().getKey()), getVersion(version));
			} else {
				applyKey(getContext().getValueRdbType());
				XRawByteListener listener = new XRawByteListener(crcOut);
				try {
					in.setRawByteListeners(Arrays.asList(listener));
					super.applyModule2(in, version);
				} finally {
					in.setRawByteListeners(null);
				}
			}
		} else {
			Tair tair = Tairs.get(in);
			applyKey(tair.type());
			tair.convertToRdbValue(in, crcOut);
			return (T) getContext();
		}
		
		return (T) getContext();
	}
	
	public <T> T applyStreamListPacks(RedisInputStream in, int version) throws IOException {
		if (getVersion(version) < 9 /* since redis rdb version 8 */) {
			logger.error("skip generate stream type key [{}] to rdb format. target rdb version [{}] too small", com.moilioncircle.redis.replicator.util.Strings.toString(getContext().getKey()), getVersion(version));
		} else {
			applyKey(getContext().getValueRdbType());
			XRawByteListener listener = new XRawByteListener(crcOut);
			try {
				in.setRawByteListeners(Arrays.asList(listener));
				super.applyStreamListPacks(in, version);
			} finally {
				in.setRawByteListeners(null);
			}
		}
		return (T) getContext();
	}
	
	public <T> T applyStreamListPacks2(RedisInputStream in, int version) throws IOException {
		if (getVersion(version) < 9 /* since redis rdb version 8 */) {
			logger.error("skip generate stream type key [{}] to rdb format. target rdb version [{}] too small", com.moilioncircle.redis.replicator.util.Strings.toString(getContext().getKey()), getVersion(version));
		} else {
			if (getVersion(version) < 10) {
				applyKey(RDB_TYPE_STREAM_LISTPACKS);
			} else {
				applyKey(getContext().getValueRdbType());
			}
			XRawByteListener listener = new XRawByteListener(crcOut);
			try {
				in.setRawByteListeners(Arrays.asList(listener));
				super.applyStreamListPacks2(in, version, listener);
			} finally {
				in.setRawByteListeners(null);
			}
		}
		return (T) getContext();
	}
}
