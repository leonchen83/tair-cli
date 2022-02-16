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

package com.tair.cli.ext;

import java.nio.ByteBuffer;

/**
 * @author Baoyi Chen
 */
public class RedisConstants {
    
    public static final byte[] ONE = "1".getBytes();
    public static final byte[] AUTH = "auth".getBytes();
    public static final byte[] PING = "ping".getBytes();
    public static final byte[] ZERO = "0".getBytes();
    public static final byte[] DEL = "del".getBytes();
    public static final byte[] SET = "set".getBytes();
    public static final byte[] SADD = "sadd".getBytes();
    public static final byte[] ZADD = "zadd".getBytes();
    public static final byte[] LOAD = "load".getBytes();
    public static final byte[] RPUSH = "rpush".getBytes();
    public static final byte[] HMSET = "hmset".getBytes();
    public static final byte[] SELECT = "select".getBytes();
    public static final byte[] SCRIPT = "script".getBytes();
    public static final byte[] DELETE = "delete".getBytes();
    public static final byte[] EVALSHA = "evalsha".getBytes();
    public static final byte[] REPLACE = "replace".getBytes();
    public static final byte[] RESTORE = "restore".getBytes();
    public static final byte[] FUNCTION = "function".getBytes();
    public static final byte[] EXPIREAT = "expireat".getBytes();
    public static final byte[] DESCRIPTION = "description".getBytes();
    public static final byte[] RESTORE_ASKING = "restore-asking".getBytes();
    
    public static final ByteBuffer ONE_BUF = ByteBuffer.wrap(ONE);
    public static final ByteBuffer DEL_BUF = ByteBuffer.wrap(DEL);
    public static final ByteBuffer SET_BUF = ByteBuffer.wrap(SET);
    public static final ByteBuffer AUTH_BUF = ByteBuffer.wrap(AUTH);
    public static final ByteBuffer PING_BUF = ByteBuffer.wrap(PING);
    public static final ByteBuffer ZERO_BUF = ByteBuffer.wrap(ZERO);
    public static final ByteBuffer SADD_BUF = ByteBuffer.wrap(SADD);
    public static final ByteBuffer ZADD_BUF = ByteBuffer.wrap(ZADD);
    public static final ByteBuffer LOAD_BUF = ByteBuffer.wrap(LOAD);
    public static final ByteBuffer RPUSH_BUF = ByteBuffer.wrap(RPUSH);
    public static final ByteBuffer HMSET_BUF = ByteBuffer.wrap(HMSET);
    public static final ByteBuffer SELECT_BUF = ByteBuffer.wrap(SELECT);
    public static final ByteBuffer DELETE_BUF = ByteBuffer.wrap(DELETE);
    public static final ByteBuffer SCRIPT_BUF = ByteBuffer.wrap(SCRIPT);
    public static final ByteBuffer EVALSHA_BUF = ByteBuffer.wrap(EVALSHA);
    public static final ByteBuffer REPLACE_BUF = ByteBuffer.wrap(REPLACE);
    public static final ByteBuffer RESTORE_BUF = ByteBuffer.wrap(RESTORE);
    public static final ByteBuffer FUNCTION_BUF = ByteBuffer.wrap(FUNCTION);
    public static final ByteBuffer EXPIREAT_BUF = ByteBuffer.wrap(EXPIREAT);
    public static final ByteBuffer DESCRIPTION_BUF = ByteBuffer.wrap(DESCRIPTION);
    public static final ByteBuffer RESTORE_ASKING_BUF = ByteBuffer.wrap(RESTORE_ASKING);
}
