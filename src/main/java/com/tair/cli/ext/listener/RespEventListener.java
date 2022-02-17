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
import static com.moilioncircle.redis.replicator.Constants.STAR;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static com.tair.cli.ext.RedisConstants.DEL;
import static com.tair.cli.ext.RedisConstants.HMSET;
import static com.tair.cli.ext.RedisConstants.PEXPIREAT;
import static com.tair.cli.ext.RedisConstants.REPLACE_BUF;
import static com.tair.cli.ext.RedisConstants.RESTORE_BUF;
import static com.tair.cli.ext.RedisConstants.RPUSH;
import static com.tair.cli.ext.RedisConstants.SADD;
import static com.tair.cli.ext.RedisConstants.SET;
import static com.tair.cli.ext.RedisConstants.ZADD;
import static com.tair.cli.ext.RedisConstants.ZERO_BUF;
import static java.nio.ByteBuffer.wrap;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;
import com.tair.cli.conf.Configure;
import com.tair.cli.escape.RawEscaper;
import com.tair.cli.ext.XDumpKeyValuePair;
import com.tair.cli.io.BufferedOutputStream;
import com.tair.cli.io.LayeredOutputStream;
import com.tair.cli.util.ByteBuffers;
import com.tair.cli.util.OutputStreams;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class RespEventListener extends AbstractEventListener {
	
	private int batch;
	private boolean replace;
	private boolean convert;
	private Configure configure;
	private Escaper escaper = new RawEscaper();
	private OutputStream out = new BufferedOutputStream(System.out, output);
	
	public RespEventListener(boolean replace, boolean convert, Configure configure) {
		this.replace = replace;
		this.convert = convert;
		this.configure = configure;
		this.batch = configure.getBatchSize();
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		if (event instanceof XDumpKeyValuePair) {
			XDumpKeyValuePair dkv = (XDumpKeyValuePair) event;
			setContext(dkv);
			
			//
			ExpiredType type = dkv.getExpiredType();
			if (type == ExpiredType.MS) {
				long ms = dkv.getExpiredMs() - System.currentTimeMillis();
				if (ms > 0) {
					apply(dkv);
					emit(this.out, PEXPIREAT, dkv.getKey(), String.valueOf(dkv.getExpiredMs()).getBytes());
				}
			} else if (type == ExpiredType.NONE) {
				apply(dkv);
			}
		}
	}
	
	@Override
	public <T> T applyString(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		byte[] val = parser.rdbLoadEncodedStringObject().first();
		emit(this.out, SET, getContext().getKey(), val);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyList(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		List<byte[]> list = new ArrayList<>();
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			list.add(element);
			if (list.size() == batch) {
				emit(this.out, RPUSH, getContext().getKey(), list);
				list.clear();
			}
			len--;
		}
		if (!list.isEmpty()) emit(this.out, RPUSH, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applySet(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		List<byte[]> list = new ArrayList<>();
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			list.add(element);
			if (list.size() == batch) {
				emit(this.out, SADD, getContext().getKey(), list);
				list.clear();
			}
			len--;
		}
		if (!list.isEmpty()) emit(this.out, SADD, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		List<byte[]> list = new ArrayList<>();
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			double score = parser.rdbLoadDoubleValue();
			list.add(String.valueOf(score).getBytes());
			list.add(element);
			if (list.size() == 2 * batch) {
				emit(this.out, ZADD, getContext().getKey(), list);
				list.clear();
			}
			len--;
		}
		if (!list.isEmpty()) emit(this.out, ZADD, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		List<byte[]> list = new ArrayList<>();
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			double score = parser.rdbLoadBinaryDoubleValue();
			list.add(String.valueOf(score).getBytes());
			list.add(element);
			if (list.size() == 2 * batch) {
				emit(this.out, ZADD, getContext().getKey(), list);
				list.clear();
			}
			len--;
		}
		if (!list.isEmpty()) emit(this.out, ZADD, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHash(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		List<byte[]> list = new ArrayList<>();
		while (len > 0) {
			byte[] field = parser.rdbLoadEncodedStringObject().first();
			byte[] value = parser.rdbLoadEncodedStringObject().first();
			list.add(field);
			list.add(value);
			if (list.size() == 2 * batch) {
				emit(this.out, HMSET, getContext().getKey(), list);
				list.clear();
			}
			len--;
		}
		if (!list.isEmpty()) emit(this.out, HMSET, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
		BaseRdbParser.LenHelper.zmlen(stream); // zmlen
		List<byte[]> list = new ArrayList<>();
		while (true) {
			int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
			if (zmEleLen == 255) {
				if (!list.isEmpty()) {
					emit(this.out, HMSET, getContext().getKey(), list);
					list.clear();
				}
				return (T) getContext();
			}
			byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
			zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
			if (zmEleLen == 255) {
				list.add(field);
				list.add(null);
				emit(this.out, HMSET, getContext().getKey(), list);
				list.clear();
				return (T) getContext();
			}
			int free = BaseRdbParser.LenHelper.free(stream);
			byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
			BaseRdbParser.StringHelper.skip(stream, free);
			list.add(field);
			list.add(value);
			if (list.size() == 2 * batch) {
				emit(this.out, HMSET, getContext().getKey(), list);
				list = new ArrayList<>();
			}
		}
	}
	
	@Override
	public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
		
		BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
		BaseRdbParser.LenHelper.zltail(stream); // zltail
		int zllen = BaseRdbParser.LenHelper.zllen(stream);
		List<byte[]> list = new ArrayList<>();
		for (int i = 0; i < zllen; i++) {
			byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
			list.add(e);
			if (list.size() == batch) {
				emit(this.out, RPUSH, getContext().getKey(), list);
				list.clear();
			}
		}
		int zlend = BaseRdbParser.LenHelper.zlend(stream);
		if (zlend != 255) {
			throw new AssertionError("zlend expect 255 but " + zlend);
		}
		if (!list.isEmpty()) emit(this.out, RPUSH, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
		
		List<byte[]> list = new ArrayList<>();
		int encoding = BaseRdbParser.LenHelper.encoding(stream);
		long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
		for (long i = 0; i < lenOfContent; i++) {
			String element;
			switch (encoding) {
				case 2:
					element = String.valueOf(stream.readInt(2));
					break;
				case 4:
					element = String.valueOf(stream.readInt(4));
					break;
				case 8:
					element = String.valueOf(stream.readLong(8));
					break;
				default:
					throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
			}
			list.add(element.getBytes());
			if (list.size() == batch) {
				emit(this.out, SADD, getContext().getKey(), list);
				list.clear();
			}
		}
		if (!list.isEmpty()) emit(this.out, SADD, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
		List<byte[]> list = new ArrayList<>();
		BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
		BaseRdbParser.LenHelper.zltail(stream); // zltail
		int zllen = BaseRdbParser.LenHelper.zllen(stream);
		while (zllen > 0) {
			byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
			zllen--;
			double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
			zllen--;
			list.add(String.valueOf(score).getBytes());
			list.add(element);
			if (list.size() == 2 * batch) {
				emit(this.out, ZADD, getContext().getKey(), list);
				list.clear();
			}
		}
		int zlend = BaseRdbParser.LenHelper.zlend(stream);
		if (zlend != 255) {
			throw new AssertionError("zlend expect 255 but " + zlend);
		}
		if (!list.isEmpty()) emit(this.out, ZADD, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
		List<byte[]> list = new ArrayList<>();
		listPack.skip(4); // total-bytes
		int len = listPack.readInt(2);
		while (len > 0) {
			byte[] element = listPackEntry(listPack);
			len--;
			double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
			len--;
			list.add(String.valueOf(score).getBytes());
			list.add(element);
			if (list.size() == 2 * batch) {
				emit(this.out, ZADD, getContext().getKey(), list);
				list.clear();
			}
		}
		int lpend = listPack.read(); // lp-end
		if (lpend != 255) {
			throw new AssertionError("listpack expect 255 but " + lpend);
		}
		if (!list.isEmpty()) emit(this.out, ZADD, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
		BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
		BaseRdbParser.LenHelper.zltail(stream); // zltail
		List<byte[]> list = new ArrayList<>();
		int zllen = BaseRdbParser.LenHelper.zllen(stream);
		while (zllen > 0) {
			byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
			zllen--;
			byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
			zllen--;
			list.add(field);
			list.add(value);
			if (list.size() == 2 * batch) {
				emit(this.out, HMSET, getContext().getKey(), list);
				list.clear();
			}
		}
		int zlend = BaseRdbParser.LenHelper.zlend(stream);
		if (zlend != 255) {
			throw new AssertionError("zlend expect 255 but " + zlend);
		}
		if (!list.isEmpty()) emit(this.out, HMSET, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
		listPack.skip(4); // total-bytes
		int len = listPack.readInt(2);
		List<byte[]> list = new ArrayList<>();
		while (len > 0) {
			byte[] field = listPackEntry(listPack);
			len--;
			byte[] value = listPackEntry(listPack);
			len--;
			list.add(field);
			list.add(value);
			if (list.size() == 2 * batch) {
				emit(this.out, HMSET, getContext().getKey(), list);
				list.clear();
			}
		}
		int lpend = listPack.read(); // lp-end
		if (lpend != 255) {
			throw new AssertionError("listpack expect 255 but " + lpend);
		}
		if (!list.isEmpty()) emit(this.out, HMSET, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		List<byte[]> list = new ArrayList<>();
		long len = parser.rdbLoadLen().len;
		for (long i = 0; i < len; i++) {
			RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
			
			BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
			BaseRdbParser.LenHelper.zltail(stream); // zltail
			int zllen = BaseRdbParser.LenHelper.zllen(stream);
			for (int j = 0; j < zllen; j++) {
				byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
				list.add(e);
				if (list.size() == batch) {
					emit(this.out, RPUSH, getContext().getKey(), list);
					list.clear();
				}
			}
			int zlend = BaseRdbParser.LenHelper.zlend(stream);
			if (zlend != 255) {
				throw new AssertionError("zlend expect 255 but " + zlend);
			}
		}
		if (!list.isEmpty()) emit(this.out, RPUSH, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
		if (replace) emit(this.out, DEL, getContext().getKey());
		BaseRdbParser parser = new BaseRdbParser(in);
		List<byte[]> list = new ArrayList<>();
		long len = parser.rdbLoadLen().len;
		for (long i = 0; i < len; i++) {
			long container = parser.rdbLoadLen().len;
			ByteArray bytes = parser.rdbLoadPlainStringObject();
			if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
				list.add(bytes.first());
				if (list.size() == batch) {
					emit(this.out, RPUSH, getContext().getKey(), list);
					list.clear();
				}
			} else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
				RedisInputStream listPack = new RedisInputStream(bytes);
				listPack.skip(4); // total-bytes
				int innerLen = listPack.readInt(2);
				for (int j = 0; j < innerLen; j++) {
					byte[] e = listPackEntry(listPack);
					list.add(e);
					if (list.size() == batch) {
						emit(this.out, RPUSH, getContext().getKey(), list);
						list.clear();
					}
				}
				int lpend = listPack.read(); // lp-end
				if (lpend != 255) {
					throw new AssertionError("listpack expect 255 but " + lpend);
				}
			} else {
				throw new UnsupportedOperationException(String.valueOf(container));
			}
		}
		if (!list.isEmpty()) emit(this.out, RPUSH, getContext().getKey(), list);
		return (T) getContext();
	}
	
	@Override
	public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
		if (!convert) {
			ByteBuffer ex = ZERO_BUF;
			if (context.getExpiredValue() != null) {
				long ms = context.getExpiredValue() - System.currentTimeMillis();
				if (ms <= 0) {
					return super.applyModule2(in, version);
				} else {
					ex = wrap(String.valueOf(ms).getBytes());
				}
			}
			try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
				try (DumpRawByteListener listener = new DumpRawByteListener(in, version, out, escaper)) {
					listener.write((byte) getContext().getValueRdbType());
					super.applyModule2(in, version);
				}
				emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
				return (T) getContext();
			}
		} else {
			// TODO
			super.applyModule2(in, version);
			return (T) getContext();
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
			try (DumpRawByteListener listener = new DumpRawByteListener(in, version, out, escaper)) {
				listener.write((byte) getContext().getValueRdbType());
				super.applyStreamListPacks(in, version);
			}
			emit(this.out, RESTORE_BUF, wrap(getContext().getKey()), ex, out.toByteBuffers(), replace);
			return (T) getContext();
		}
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
	
	protected void emit(OutputStream out, byte[] command, byte[] key, List<byte[]> ary) {
		OutputStreams.write(STAR, out);
		OutputStreams.write(String.valueOf(ary.size() + 2).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(command.length).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(command, out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(key.length).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(key, out);
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
}
