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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.tair.cli.cmd.support.XVersionProvider;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
@CommandLine.Command(name = "tair-cli",
		separator = " ",
		synopsisHeading = "",
		optionListHeading = "Options:%n",
		mixinStandardHelpOptions = true,
		versionProvider = XVersionProvider.class,
		subcommands = {CommandLine.HelpCommand.class, XRdbCommand.class, XRespCommand.class, XDumpCommand.class, XMemoryCommand.class, XJsonlCommand.class, XCountCommand.class})
public class XTairCli implements Callable<Integer> {
	
	@CommandLine.Spec
	private CommandLine.Model.CommandSpec spec;
	
	@CommandLine.Option(names = {"--version"}, versionHelp = true, description = "Print version information and exit.")
	private boolean versionInfoRequested;
	
	@CommandLine.Option(names = {"--source"}, required = true, description = {"Source uri. eg: redis://host:port?authPassword=foobar."}, scope = CommandLine.ScopeType.INHERIT)
	String source;
	
	@CommandLine.Option(names = {"--db"}, arity = "1..*", paramLabel = "<num>", description = {"Database number. multiple databases can be provided. if not specified, all databases will be returned."}, type = Integer.class, scope = CommandLine.ScopeType.INHERIT)
	List<Integer> db = new ArrayList<>();
	
	@CommandLine.Option(names = {"--key"}, arity = "1..*", paramLabel = "<regex>", description = {"Keys to export. this can be a regex. if not specified, all keys will be returned."}, scope = CommandLine.ScopeType.INHERIT)
	List<String> regexs = new ArrayList<>();
	
	@CommandLine.Option(names = {"--type"}, arity = "1..*", description = {"Data type to export. possible values are: string, hash, set, sortedset, list, module, stream. multiple types can be provided. if not specified, all data types will be returned."} , scope = CommandLine.ScopeType.INHERIT)
	List<String> type = new ArrayList<>();
	
	@Override
	public Integer call() throws Exception {
		return 0;
	}
}
