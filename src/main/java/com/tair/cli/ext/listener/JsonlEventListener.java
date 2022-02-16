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
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;

import java.io.IOException;
import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;
import com.tair.cli.conf.Configure;
import com.tair.cli.escape.JsonEscaper;
import com.tair.cli.escape.RedisEscaper;
import com.tair.cli.ext.XDumpKeyValuePair;
import com.tair.cli.glossary.DataType;
import com.tair.cli.util.OutputStreams;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class JsonlEventListener extends AbstractEventListener {
	
	private boolean convert;
	private Configure configure;
	private boolean firstkey = true;
	private OutputStream out = System.out;
	private Escaper escaper = new JsonEscaper();
	
	public JsonlEventListener(boolean convert, Configure configure) {
		this.convert = convert;
		this.configure = configure;
	}
	
	private void emitString(byte[] str) {
		OutputStreams.write('"', out);
		escaper.encode(str, out);
		OutputStreams.write('"', out);
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
	
	private void emitNull(byte[] field) {
		emitString(field);
		OutputStreams.write(':', out);
		escaper.encode("null".getBytes(), out);
	}
	
	private void emitZSet(byte[] field, double value) {
		emitString(field);
		OutputStreams.write(':', out);
		escaper.encode(value, out);
	}
	
	private void emitField(String field, long value) {
		emitString(field.getBytes());
		OutputStreams.write(':', out);
		escaper.encode(String.valueOf(value).getBytes(), out);
	}
	
	/**
	 *
	 */
	protected void separator() {
		OutputStreams.write('\n', out);
	}
	
	public static interface Emitable {
		void emitValue() throws IOException;
	}
	
	protected void json(XDumpKeyValuePair context, Emitable emitable) throws IOException {
		if (!firstkey) {
			separator();
		}
		firstkey = false;
		OutputStreams.write('{', out);
		emitField("key", context.getKey());
		OutputStreams.write(',', out);
		emitString("value".getBytes());
		OutputStreams.write(':', out);
		emitable.emitValue();
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
	public  <T> T applyString(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] val = parser.rdbLoadEncodedStringObject().first();
			emitString(val);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyList(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('[', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			long len = parser.rdbLoadLen().len;
			boolean flag = true;
			while (len > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] element = parser.rdbLoadEncodedStringObject().first();
				emitString(element);
				flag = false;
				len--;
			}
			OutputStreams.write(']', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applySet(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('[', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			long len = parser.rdbLoadLen().len;
			boolean flag = true;
			while (len > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] element = parser.rdbLoadEncodedStringObject().first();
				emitString(element);
				flag = false;
				len--;
			}
			OutputStreams.write(']', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			long len = parser.rdbLoadLen().len;
			boolean flag = true;
			while (len > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] element = parser.rdbLoadEncodedStringObject().first();
				double score = parser.rdbLoadDoubleValue();
				emitZSet(element, score);
				flag = false;
				len--;
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			long len = parser.rdbLoadLen().len;
			boolean flag = true;
			while (len > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] element = parser.rdbLoadEncodedStringObject().first();
				double score = parser.rdbLoadBinaryDoubleValue();
				emitZSet(element, score);
				flag = false;
				len--;
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHash(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			long len = parser.rdbLoadLen().len;
			boolean flag = true;
			while (len > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] field = parser.rdbLoadEncodedStringObject().first();
				byte[] value = parser.rdbLoadEncodedStringObject().first();
				emitField(field, value);
				flag = false;
				len--;
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
			BaseRdbParser.LenHelper.zmlen(stream); // zmlen
			boolean flag = true;
			while (true) {
				int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
				if (zmEleLen == 255) {
					break;
				}
				if (!flag) {
					OutputStreams.write(',', out);
				}
				flag = false;
				byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
				zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
				if (zmEleLen == 255) {
					//value is null
					emitNull(field);
					break;
				}
				int free = BaseRdbParser.LenHelper.free(stream);
				byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
				BaseRdbParser.StringHelper.skip(stream, free);
				emitField(field, value);
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('[', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
			
			BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
			BaseRdbParser.LenHelper.zltail(stream); // zltail
			int zllen = BaseRdbParser.LenHelper.zllen(stream);
			boolean flag = true;
			for (int i = 0; i < zllen; i++) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
				emitString(e);
				flag = false;
			}
			int zlend = BaseRdbParser.LenHelper.zlend(stream);
			if (zlend != 255) {
				throw new AssertionError("zlend expect 255 but " + zlend);
			}
			OutputStreams.write(']', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('[', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
			boolean flag = true;
			int encoding = BaseRdbParser.LenHelper.encoding(stream);
			long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
			for (long i = 0; i < lenOfContent; i++) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
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
				emitString(element.getBytes());
				flag = false;
			}
			OutputStreams.write(']', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
			boolean flag = true;
			BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
			BaseRdbParser.LenHelper.zltail(stream); // zltail
			int zllen = BaseRdbParser.LenHelper.zllen(stream);
			while (zllen > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
				zllen--;
				double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
				zllen--;
				emitZSet(element, score);
				flag = false;
			}
			int zlend = BaseRdbParser.LenHelper.zlend(stream);
			if (zlend != 255) {
				throw new AssertionError("zlend expect 255 but " + zlend);
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
			boolean flag = true;
			listPack.skip(4); // total-bytes
			int len = listPack.readInt(2);
			while (len > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] element = listPackEntry(listPack);
				len--;
				double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
				len--;
				emitZSet(element, score);
				flag = false;
			}
			int lpend = listPack.read(); // lp-end
			if (lpend != 255) {
				throw new AssertionError("listpack expect 255 but " + lpend);
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
			BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
			BaseRdbParser.LenHelper.zltail(stream); // zltail
			boolean flag = true;
			int zllen = BaseRdbParser.LenHelper.zllen(stream);
			while (zllen > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
				zllen--;
				byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
				zllen--;
				emitField(field, value);
				flag = false;
			}
			int zlend = BaseRdbParser.LenHelper.zlend(stream);
			if (zlend != 255) {
				throw new AssertionError("zlend expect 255 but " + zlend);
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('{', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
			boolean flag = true;
			listPack.skip(4); // total-bytes
			int len = listPack.readInt(2);
			while (len > 0) {
				if (!flag) {
					OutputStreams.write(',', out);
				}
				byte[] field = listPackEntry(listPack);
				len--;
				byte[] value = listPackEntry(listPack);
				len--;
				emitField(field, value);
				flag = false;
			}
			int lpend = listPack.read(); // lp-end
			if (lpend != 255) {
				throw new AssertionError("listpack expect 255 but " + lpend);
			}
			OutputStreams.write('}', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('[', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			boolean flag = true;
			long len = parser.rdbLoadLen().len;
			for (long i = 0; i < len; i++) {
				RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
				
				BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
				BaseRdbParser.LenHelper.zltail(stream); // zltail
				int zllen = BaseRdbParser.LenHelper.zllen(stream);
				for (int j = 0; j < zllen; j++) {
					if (!flag) {
						OutputStreams.write(',', out);
					}
					byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
					emitString(e);
					flag = false;
				}
				int zlend = BaseRdbParser.LenHelper.zlend(stream);
				if (zlend != 255) {
					throw new AssertionError("zlend expect 255 but " + zlend);
				}
			}
			OutputStreams.write(']', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('[', out);
			BaseRdbParser parser = new BaseRdbParser(in);
			boolean flag = true;
			long len = parser.rdbLoadLen().len;
			for (long i = 0; i < len; i++) {
				long container = parser.rdbLoadLen().len;
				ByteArray bytes = parser.rdbLoadPlainStringObject();
				if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
					if (!flag) {
						OutputStreams.write(',', out);
					}
					emitString(bytes.first());
					flag = false;
				} else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
					RedisInputStream listPack = new RedisInputStream(bytes);
					listPack.skip(4); // total-bytes
					int innerLen = listPack.readInt(2);
					for (int j = 0; j < innerLen; j++) {
						if (!flag) {
							OutputStreams.write(',', out);
						}
						byte[] e = listPackEntry(listPack);
						emitString(e);
						flag = false;
					}
					int lpend = listPack.read(); // lp-end
					if (lpend != 255) {
						throw new AssertionError("listpack expect 255 but " + lpend);
					}
				} else {
					throw new UnsupportedOperationException(String.valueOf(container));
				}
			}
			OutputStreams.write(']', out);
		});
		return (T) getContext();
	}
	
	@Override
	public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
		if (!convert) {
			json(getContext(), () -> {
				OutputStreams.write('"', out);
				try (DumpRawByteListener listener = new DumpRawByteListener(in, version, out, new RedisEscaper((byte)',', (byte)'\"'))) {
					listener.write((byte) getContext().getValueRdbType());
					super.applyModule2(in, version);
				}
				OutputStreams.write('"', out);
			});
		} else {
			// TODO parse tair module
			super.applyModule2(in, version);
			return (T) getContext();
		}
		
		return (T) getContext();
	}
	
	@Override
	public <T> T applyStreamListPacks(RedisInputStream in, int version) throws IOException {
		json(getContext(), () -> {
			OutputStreams.write('"', out);
			try (DumpRawByteListener listener = new DumpRawByteListener(in, version, out, new RedisEscaper((byte)',', (byte)'\"'))) {
				listener.write((byte) getContext().getValueRdbType());
				super.applyStreamListPacks(in, version);
			}
			OutputStreams.write('"', out);
		});
		return (T) getContext();
	}
}
