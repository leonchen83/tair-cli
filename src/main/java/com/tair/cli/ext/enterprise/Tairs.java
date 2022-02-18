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

import static com.moilioncircle.redis.replicator.Constants.MODULE_SET;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.module.ModuleKey;

/**
 * @author Baoyi Chen
 */
public class Tairs {
	
	// weird name
	private static ModuleKey TAIR_STRING = ModuleKey.key("exstrtype", 1);
	private static ModuleKey TAIR_HASH = ModuleKey.key("exhash---", 0);
	private static ModuleKey TAIR_DOC = ModuleKey.key("tair-json", 0);
	private static ModuleKey TAIR_ZSET = ModuleKey.key("tairzset_", 0);
	private static ModuleKey TAIR_GIS = ModuleKey.key("exgistype", 0);
	private static ModuleKey TAIR_BLOOM = ModuleKey.key("exbloom--", 0);
	private static ModuleKey TAIR_ROARING = ModuleKey.key("tairroar-", 1);
	
	private static Map<ModuleKey, Tair> MAP = new HashMap<>();
	
	static {
		MAP.put(TAIR_STRING, new TairString());
		MAP.put(TAIR_HASH, new TairHash());
		MAP.put(TAIR_DOC, new TairDoc());
		MAP.put(TAIR_ZSET, new TairZSet());
		
		// store as string type
		MAP.put(TAIR_GIS, new TairGis());
		MAP.put(TAIR_BLOOM, new TairBloom());
		MAP.put(TAIR_ROARING, new TairRoaring());
	}
	
	public static Tair get(RedisInputStream in) throws IOException {
		BaseRdbParser parser = new BaseRdbParser(in);
		char[] c = new char[9];
		long moduleid = parser.rdbLoadLen().len;
		for (int i = 0; i < c.length; i++) {
			c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
		}
		String moduleName = new String(c);
		int moduleVersion = (int) (moduleid & 1023);
		ModuleKey key = ModuleKey.key(moduleName, moduleVersion);
		Tair tair = MAP.get(key);
		if (tair == null) {
			tair = new UnTairStructure();
		}
		tair.setModuleId(moduleid);
		return tair;
	}
}
