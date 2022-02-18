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
import java.util.Arrays;

import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.ByteBuilder;

/**
 * @author Baoyi Chen
 */
public class TairBloom extends AbstractTair {
	
	@Override
	public int type() {
		return Constants.RDB_TYPE_STRING;
	}
	
	@Override
	public void convertToRdbValue(RedisInputStream in, OutputStream out) throws IOException {
		BaseRdbEncoder encoder = new BaseRdbEncoder();
		byte[] bytes = rdbSaveLen(id);
		ByteBuilder builder = ByteBuilder.allocate(64);
		builder.put((byte)Constants.RDB_TYPE_MODULE_2);
		builder.put(bytes);
		RawByteListener listener = new RawByteListener() {
			@Override
			public void handle(byte... rawBytes) {
				builder.put(rawBytes);
			}
		};
		try {
			in.setRawByteListeners(Arrays.asList(listener));
			SkipRdbParser parser = new SkipRdbParser(in);
			parser.rdbLoadCheckModuleValue();
		} finally {
			in.setRawByteListeners(null);
		}
		byte[] value = builder.array();
		encoder.rdbGenericSaveStringObject(new ByteArray(value), out);
	}
}
