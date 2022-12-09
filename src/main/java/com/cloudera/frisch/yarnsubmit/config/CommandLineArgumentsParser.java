/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.yarnsubmit.config;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.util.EnumMap;
import java.util.Map;


public class CommandLineArgumentsParser {

    private static final Logger logger = Logger.getLogger(CommandLineArgumentsParser.class);

    public static Map<Arguments, String> getMainCommandLineArguments(String[] args) {
        Map<Arguments, String> arguments = new EnumMap<>(Arguments.class);

        Options options = new Options();

        Option input = new Option("c", "config-file", true, "path to configuration file");
        input.setRequired(true);
        options.addOption(input);

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            arguments.put(Arguments.CONFIG_FILE, cmd.getOptionValue("config-file"));
        } catch (ParseException e) {
            logger.error(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        arguments.forEach((arg, value) ->logger.info("Argument is : "  + arg.name() + " and value is: "  + value));

        return arguments;
    }

    public enum Arguments {
        CONFIG_FILE
    }
}
