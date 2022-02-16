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

package com.tair.cli.cmd;

import java.util.concurrent.Callable;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
@CommandLine.Command(name = "rdb",
		descriptionHeading = "Description: ",
		description = "Convert source to rdb format.",
		separator = " ",
		synopsisHeading = "",
		optionListHeading = "Options:%n")
public class XRdbCommand implements Callable<Integer> {
	
	@CommandLine.Spec
	private CommandLine.Model.CommandSpec spec;
	
	@CommandLine.ParentCommand
	private XTairCli parent;
	
	@CommandLine.Option(names = {"--rdb-version"}, paramLabel = "<num>", description = {"Generate rdb version from 6 to 10. if not specified, use the source rdb version."}, type = Integer.class)
	private Integer rdbVersion;
	
	@Override
	public Integer call() throws Exception {
		return 0;
	}
}
