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

import java.io.IOException;
import java.io.OutputStream;

import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.module.DefaultRdbModuleParser;
import com.moilioncircle.redis.replicator.util.ByteArray;

/**
 * @author Baoyi Chen
 */
public class TairHash extends AbstractTair {
	
	@Override
	public int type() {
		return Constants.RDB_TYPE_HASH;
	}
	
	@Override
	public void convertToRdbValue(RedisInputStream in, OutputStream out) throws IOException {
		DefaultRdbModuleParser parser = new DefaultRdbModuleParser(in);
		BaseRdbEncoder encoder = new BaseRdbEncoder();
		
		long len = parser.loadSigned(2);
		byte[] key = parser.loadStringBuffer(2); // skip key
		parser.loadSigned(2); // a wired value?
		
		encoder.rdbSaveLen(len, out);
		for (long i = 0; i < len; i++) {
			byte[] field = parser.loadStringBuffer(2);
			long version = parser.loadSigned(2);
			long timestamp = parser.loadSigned(2);
			byte[] value = parser.loadStringBuffer(2);
			encoder.rdbGenericSaveStringObject(new ByteArray(field), out);
			encoder.rdbGenericSaveStringObject(new ByteArray(value), out);
		}
		
//		BaseRdbParser parser = new BaseRdbParser(in);
//		int opcode;
//		while ((opcode = (int) parser.rdbLoadLen().len) != RDB_MODULE_OPCODE_EOF) {
//			if (opcode == RDB_MODULE_OPCODE_SINT || opcode == RDB_MODULE_OPCODE_UINT) {
//				System.out.println(parser.rdbLoadLen().len);
//			} else if (opcode == RDB_MODULE_OPCODE_STRING) {
//				System.out.println(new String(parser.rdbLoadPlainStringObject().first()));
//			} else if (opcode == RDB_MODULE_OPCODE_FLOAT) {
//				System.out.println(parser.rdbLoadBinaryFloatValue());
//			} else if (opcode == RDB_MODULE_OPCODE_DOUBLE) {
//				System.out.println(parser.rdbLoadBinaryDoubleValue());
//			}
//		}
	}
}
