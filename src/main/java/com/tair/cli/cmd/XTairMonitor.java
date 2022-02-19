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

import com.moilioncircle.redis.replicator.RedisURI;
import com.tair.cli.cmd.support.XVersionProvider;
import com.tair.cli.conf.Configure;
import com.tair.cli.util.CloseableThread;

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
		subcommands = {CommandLine.HelpCommand.class})
public class XTairMonitor implements Callable<Integer> {
	
	@CommandLine.Spec
	private CommandLine.Model.CommandSpec spec;
	
	@CommandLine.Option(names = {"--version"}, versionHelp = true, description = "Print version information and exit.")
	private boolean versionInfoRequested;
	
	@CommandLine.Option(names = {"--source"}, required = true, description = {"Source uri. eg: redis://host:port?authPassword=foobar."}, scope = CommandLine.ScopeType.INHERIT)
	String source;
	
	@Override
	public Integer call() throws Exception {
		Configure configure = Configure.bind();
		RedisURI uri = new RedisURI(source);
		String hp = uri.getHost().replaceAll("\\.", "_") + "_" + uri.getPort();
		configure.set("instance", hp);
		XMonitorCommand command = new XMonitorCommand(uri, configure);
		CloseableThread thread = CloseableThread.open("tair_monitor", command, true);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			CloseableThread.close(thread);
			command.close();
		}));
		thread.join();
		return 0;
	}
}
