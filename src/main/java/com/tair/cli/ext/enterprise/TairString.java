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
public class TairString extends AbstractTair {
	
	@Override
	public int type() {
		return Constants.RDB_TYPE_STRING;
	}
	
	@Override
	public void convertToRdbValue(RedisInputStream in, OutputStream out) throws IOException {
		DefaultRdbModuleParser moduleParser = new DefaultRdbModuleParser(in);
		long version = moduleParser.loadSigned(2);
		long skip = moduleParser.loadSigned(2);
		byte[] value = moduleParser.loadStringBuffer(2);
		BaseRdbEncoder encoder = new BaseRdbEncoder();
		encoder.rdbGenericSaveStringObject(new ByteArray(value), out);
	}
}
