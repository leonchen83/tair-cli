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

package com.tair.cli;

import com.tair.cli.cmd.XTairCli;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
public class TairCli {
	public static void main(String[] args) {
		CommandLine commandLine = new CommandLine(new XTairCli());
		int r = commandLine.execute(args);
		if (r != 0) System.exit(r);
	}
}
