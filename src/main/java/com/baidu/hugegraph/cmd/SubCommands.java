/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.cmd;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.util.E;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SubCommands {

    private Map<String, Object> commands;

    public SubCommands() {
        this.commands = new HashMap<>();
        this.initSubCommands();
    }

    private void initSubCommands() {
        this.commands.put("backup", new Backup());
        this.commands.put("restore", new Restore());
        this.commands.put("graph-list", new GraphList());
        this.commands.put("graph-get", new GraphGet());
        this.commands.put("graph-clear", new GraphClear());
        this.commands.put("graph-mode-set", new GraphModeSet());
        this.commands.put("graph-mode-get", new GraphModeGet());
        this.commands.put("gremlin", new Gremlin());
        this.commands.put("deploy", new Deploy());
        this.commands.put("start-all", new StartAll());
        this.commands.put("clear", new Clear());
        this.commands.put("stop-all", new StopAll());
        this.commands.put("help", new Help());
    }

    public Map<String, Object> commands() {
        return this.commands;
    }

    @Parameters(commandDescription = "Backup graph schema/data to files")
    public class Backup extends BackupRestore {
    }

    @Parameters(commandDescription = "Restore graph schema/data from files")
    public class Restore extends BackupRestore {
    }

    @Parameters(commandDescription = "List all graphs")
    public class GraphList {
    }

    @Parameters(commandDescription = "Get graph info")
    public class GraphGet {

        @ParametersDelegate
        private Graph graph = new Graph();

        public String graph() {
            return this.graph.graph;
        }
    }

    @Parameters(commandDescription = "Clear graph schema and data")
    public class GraphClear {

        @ParametersDelegate
        private Graph graph = new Graph();

        @ParametersDelegate
        private ConfirmMessage message = new ConfirmMessage();

        public String graph() {
            return this.graph.graph;
        }

        public String confirmMessage() {
            return this.message.confirmMessage;
        }
    }

    @Parameters(commandDescription = "Set graph mode")
    public class GraphModeSet {

        @ParametersDelegate
        private Graph graph = new Graph();

        @ParametersDelegate
        private RestoreFlag restoreFlag = new RestoreFlag();

        public String graph() {
            return this.graph.graph;
        }

        public boolean restoreFlag() {
            return this.restoreFlag.restoreFlag;
        }
    }

    @Parameters(commandDescription = "Get graph mode")
    public class GraphModeGet {

        @ParametersDelegate
        private Graph graph = new Graph();

        public String graph() {
            return this.graph.graph;
        }
    }

    @Parameters(commandDescription = "Execute Gremlin statements")
    public class Gremlin {

        @ParametersDelegate
        private GremlinScript script = new GremlinScript();

        @ParametersDelegate
        private Language language = new Language();

        @ParametersDelegate
        private Bindings bindings = new Bindings();

        @ParametersDelegate
        private Aliases aliases = new Aliases();

        public String script() {
            return this.script.script;
        }

        public String language() {
            return this.language.language;
        }

        public Map<String, String> bindings() {
            return this.bindings.bindings;
        }

        public Map<String, String> aliases() {
            return this.aliases.aliases;
        }
    }

    @Parameters(commandDescription = "Install HugeGraph-Server and " +
                                     "HugeGraph-Studio")
    public class Deploy {

        @ParametersDelegate
        private Version version = new Version();
    }

    @Parameters(commandDescription = "Start HugeGraph-Server and " +
                                     "HugeGraph-Studio")
    public class StartAll {

        @ParametersDelegate
        private Version version = new Version();
    }

    @Parameters(commandDescription = "Clear HugeGraph-Server and " +
                                     "HugeGraph-Studio")
    public class Clear {
    }

    @Parameters(commandDescription = "Stop HugeGraph-Server and " +
                                     "HugeGraph-Studio")
    public class StopAll {
    }

    @Parameters(commandDescription = "Print usage")
    public class Help {
    }

    public class BackupRestore {

        @ParametersDelegate
        private HugeTypes types = new HugeTypes();

        @ParametersDelegate
        private Directory directory = new Directory();

        public List<HugeType> types() {
            return this.types.types;
        }

        public String directory() {
            return this.directory.directory;
        }
    }

    public static class Url {

        @Parameter(names = {"--url"}, arity = 1,
                   validateWith = {UrlValidator.class},
                   description = "The URL of hugegraph url")
        public String url = "http://127.0.0.1:8080";
    }

    public static class HugeGraph {

        @Parameter(names = {"--hugegraph"}, arity = 1,
                   description = "Name of graph")
        public String graph = "hugegraph";
    }

    public static class Username {

        @Parameter(names = {"--user"}, arity = 1,
                   description = "User Name")
        public String username;
    }

    public static class Password {

        @Parameter(names = {"--password"}, arity = 1,
                   description = "Password of user")
        public String password;
    }

    public static class Graph {

        @Parameter(names = {"--graph"}, arity = 1,
                   description = "Name of graph")
        public String graph = "hugegraph";
    }

    public class HugeTypes {

        @Parameter(names = {"--huge-types", "-t"},
                   listConverter = HugeTypeListConverter.class,
                   required = true,
                   description = "Type of schema/data. " +
                                 "Concat with ',' if more than one. " +
                                 "'all' means 'vertex,edge,vertex_label," +
                                 "edge_label,property_key,index_label'")
        public List<HugeType> types;
    }

    public class Directory {

        @Parameter(names = {"--directory", "-d"}, arity = 1,
                   validateWith = {DirectoryValidator.class},
                   description = "Directory of graph schema/data")
        public String directory = "./";
    }

    public class ConfirmMessage {

        @Parameter(names = {"--confirm-message", "-c"}, arity = 1,
                   description = "Confirm message of graph clear",
                   required = true)
        public String confirmMessage;
    }

    public class RestoreFlag {

        @Parameter(names = {"--restore", "-r"}, arity = 1,
                   description = "Restore flag",
                   required = true)
        public boolean restoreFlag;
    }

    public class GremlinScript {

        @Parameter(names = {"--script", "-s"}, arity = 1,
                   required = true,
                   description = "Script to be executed")
        public String script;
    }

    public class Language {

        @Parameter(names = {"--language", "-l"}, arity = 1,
                   description = "Gremlin script language")
        public String language = "gremlin-groovy";
    }

    public class Bindings {

        @Parameter(names = {"--bindings", "-b"}, arity = 1,
                   converter = MapConverter.class,
                   description = "Gremlin bindings, valid format is: " +
                                 "'key1=value1,key2=value2...'")
        public Map<String, String> bindings = ImmutableMap.of();
    }

    public class Aliases {

        @Parameter(names = {"--aliases", "-a"}, arity = 1,
                   converter = MapConverter.class,
                   description = "Gremlin aliases, valid format is: " +
                                 "'key1=value1,key2=value2...'")
        public Map<String, String> aliases = ImmutableMap.of();
    }

    public class Version {

        @Parameter(arity = 1,
                   description = "version")
        public String version;
    }

    public static class HugeTypeListConverter
                  implements IStringConverter<List<HugeType>> {

        @Override
        public List<HugeType> convert(String value) {
            E.checkArgument(value != null && !value.isEmpty(),
                            "HugeType can't be null or empty");
            String[] types = value.split(",");
            if (types.length == 1 && types[0].equalsIgnoreCase("all")) {
                return ImmutableList.of(HugeType.PROPERTY_KEY,
                                        HugeType.VERTEX_LABEL,
                                        HugeType.EDGE_LABEL,
                                        HugeType.INDEX_LABEL,
                                        HugeType.VERTEX,
                                        HugeType.EDGE);
            }
            List<HugeType> hugeTypes = new ArrayList<>();
            for (String type : types) {
                try {
                    hugeTypes.add(HugeType.valueOf(type.toUpperCase()));
                } catch (IllegalArgumentException e) {
                    throw new ParameterException(String.format(
                              "Invalid --type '%s', valid value is 'all' or " +
                              "combination of 'vertex,edge,vertex_label," +
                              "edge_label,property_key,index_label'", type));
                }
            }
            return hugeTypes;
        }
    }

    public static class MapConverter
                  implements IStringConverter<Map<String, String>> {

        @Override
        public Map<String, String> convert(String value) {
            E.checkArgument(value != null && !value.isEmpty(),
                            "HugeType can't be null or empty");
            String[] equals = value.split(",");
            Map<String, String> result = new HashMap<>();
            for (String equal : equals) {
                String[] kv = equal.split("=");
                E.checkArgument(kv.length == 2,
                                "Map arguments format should be key=value, " +
                                "but got '%s'", equal);
                result.put(kv[0], kv[1]);
            }
            return result;
        }
    }

    public static class UrlValidator implements IParameterValidator {

        @Override
        public void validate(String name, String value) {
            String regex = "^((https|http|ftp|rtsp|mms)?://)"
                    + "?(([0-9a-z_!~*'().&=+$%-]+: )?[0-9a-z_!~*'().&=+$%-]+@)?"
                    + "(([0-9]{1,3}\\.){3}[0-9]{1,3}" // IP URL, like: 10.0.0.1
                    + "|" // Or domain name
                    + "([0-9a-z_!~*'()-]+\\.)*" // Third level, like: www.
                    + "([0-9a-z][0-9a-z-]{0,61})?[0-9a-z]\\." // Second level
                    + "[a-z]{2,6})" // First level, like: com or museum
                    + "(:[0-9]{1,4})?"; // Port, like: 8080
            if (!value.matches(regex)) {
                throw new ParameterException(String.format(
                          "Invalid value of argument '%s': '%s'", name, value));
            }
        }
    }

    public static class DirectoryValidator implements IParameterValidator {

        @Override
        public void validate(String name, String value) {
            File file = new File(value);
            if (!file.exists() || !file.isDirectory()) {
                throw new ParameterException(String.format(
                          "Invalid value of argument '%s': '%s'", name, value));
            }
        }
    }
}