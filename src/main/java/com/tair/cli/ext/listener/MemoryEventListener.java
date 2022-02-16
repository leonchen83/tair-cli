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

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.tair.cli.conf.Configure;

/**
 * @author Baoyi Chen
 */
public class MemoryEventListener implements EventListener {
	
	private Long bytes;
	private Integer limit;
	private Configure configure;
	
	public MemoryEventListener(Integer limit, Long bytes, Configure configure) {
		this.limit = limit;
		this.bytes = bytes;
		this.configure = configure;
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		
	}
}
