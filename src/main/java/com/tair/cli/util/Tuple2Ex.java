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

package com.tair.cli.util;

import com.moilioncircle.redis.replicator.util.type.Tuple2;
import com.tair.cli.ext.XDumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class Tuple2Ex extends Tuple2<Long, XDumpKeyValuePair> implements Comparable<Tuple2Ex> {

    private static final long serialVersionUID = 1L;

    public Tuple2Ex(Long v1, XDumpKeyValuePair v2) {
        super(v1, v2);
    }

    @Override
    public int compareTo(Tuple2Ex that) {
        return Long.compare(this.getV1(), that.getV1());
    }
}
