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

import com.tair.cli.conf.Configure;

/**
 * @author Baoyi Chen
 */
public class RespEventListener extends AbstractEventListener {
	
	private Integer batch;
	private boolean replace;
	private boolean convert;
	private Configure configure;
	
	public RespEventListener(Integer batch, boolean replace, boolean convert, Configure configure) {
		this.batch = batch;
		this.replace = replace;
		this.convert = convert;
		this.configure = configure;
	}
}
