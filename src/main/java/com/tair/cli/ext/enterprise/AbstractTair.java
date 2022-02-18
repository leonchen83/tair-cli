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

package com.tair.cli.ext.enterprise;

import static com.moilioncircle.redis.replicator.Constants.RDB_14BITLEN;
import static com.moilioncircle.redis.replicator.Constants.RDB_32BITLEN;
import static com.moilioncircle.redis.replicator.Constants.RDB_64BITLEN;
import static com.moilioncircle.redis.replicator.Constants.RDB_6BITLEN;
import static java.nio.ByteOrder.BIG_ENDIAN;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractTair implements Tair {
	
	protected long id;
	
	public void setModuleId(long id) {
		this.id = id;
	}
	
	public byte[] rdbSaveLen(long len) throws IOException {
		byte[] ary = toUnsigned(len);
		BigInteger value = new BigInteger(1, ary);
		if (value.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
			return ByteBuffer.allocate(9).order(BIG_ENDIAN).put((byte) RDB_64BITLEN).put(ary).array();
		} else if (len < (1 << 6)) {
			return new byte[]{(byte) ((len & 0xFF) | (RDB_6BITLEN << 6))};
		} else if (len < (1 << 14)) {
			/* Save a 14 bit len */
			return new byte[]{(byte) (((len >> 8) & 0xFF) | (RDB_14BITLEN << 6)), (byte) (len & 0xFF)};
		} else if (len <= Integer.MAX_VALUE) {
			/* Save a 32 bit len */
			return ByteBuffer.allocate(5).order(BIG_ENDIAN).put((byte) RDB_32BITLEN).putInt((int) len).array();
		} else {
			/* Save a 64 bit len */
			return ByteBuffer.allocate(9).order(BIG_ENDIAN).put((byte) RDB_64BITLEN).putLong(len).array();
		}
	}
	
	public byte[] toUnsigned(long value) {
		byte[] ary = new byte[8];
		for (int i = 0; i < 8; i++) {
			ary[7 - i] = (byte) ((value >>> (i << 3)) & 0xFF);
		}
		return ary;
	}
}
