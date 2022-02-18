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

import com.moilioncircle.redis.replicator.io.RedisInputStream;

/**
 * @author Baoyi Chen
 */
public interface Tair {
	
	int type();
	
	void setModuleId(long id);
	
	void convertToRdbValue(RedisInputStream in, OutputStream out) throws IOException;
}
