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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.tair.cli.glossary.DataType;

/**
 * @author Baoyi Chen
 */
public class Filter {
	
	public List<Integer> dbs;
	public List<Pattern> regexs;
	public List<DataType> types;
	
	public Filter() {
		this(null, null, null);
	}
	
	public Filter(List<Pattern> regexs, List<Integer> dbs, List<String> types) {
		this.dbs = dbs == null ? new ArrayList<>() : dbs;
		this.regexs = regexs == null ? new ArrayList<>() : regexs;
		this.types = DataType.parse(types == null ? new ArrayList<>() : types);
	}
}
