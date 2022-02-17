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

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.tair.cli.conf.Configure;
import com.tair.cli.ext.Filter;
import com.tair.cli.ext.RedisScanReplicator;
import com.tair.cli.ext.listener.DumpEventListener;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
@CommandLine.Command(name = "dump",
		descriptionHeading = "Description: ",
		description = "Convert source to resp format.",
		separator = " ",
		synopsisHeading = "",
		optionListHeading = "Options:%n")
public class XDumpCommand implements Callable<Integer> {
	
	@CommandLine.Spec
	private CommandLine.Model.CommandSpec spec;
	
	@CommandLine.ParentCommand
	private XTairCli parent;
	
	@CommandLine.Option(names = {"--replace"}, description = {"Whether the generated aof with <replace> parameter. if not specified, default value is false."})
	private boolean replace;
	
	@CommandLine.Option(names = {"--rdb-version"}, paramLabel = "<num>", description = {"Generate rdb version from 6 to 10. if not specified, use the source rdb version."}, type = Integer.class)
	private Integer rdbVersion = -1;
	
	@CommandLine.Option(names = {"--convert"}, description = {"Whether convert tair module to normal data structure."})
	private boolean convert;
	
	@Override
	public Integer call() throws Exception {
		Configure configure = Configure.bind();
		Filter filter = new Filter(parent.regexs, parent.db, parent.type);
		Replicator replicator = new RedisScanReplicator(parent.source, configure, filter);
		replicator.addEventListener(new DumpEventListener(replace, rdbVersion, convert, configure));
		replicator.addExceptionListener((rep, tx, e) -> {
			throw new RuntimeException(tx.getMessage(), tx);
		});
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			Replicators.closeQuietly(replicator);
		}));
		replicator.addEventListener((rep, event) -> {
			if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent)
				Replicators.closeQuietly(replicator);
		});
		replicator.open();
		return 0;
	}
}
