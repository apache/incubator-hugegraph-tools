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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.baidu.hugegraph.base.Printer;
import com.baidu.hugegraph.base.ToolClient;
import com.baidu.hugegraph.base.ToolClient.ConnectionInfo;
import com.baidu.hugegraph.base.ToolManager;
import com.baidu.hugegraph.manager.BackupManager;
import com.baidu.hugegraph.manager.DumpGraphManager;
import com.baidu.hugegraph.manager.GraphsManager;
import com.baidu.hugegraph.manager.GremlinManager;
import com.baidu.hugegraph.manager.RestoreManager;
import com.baidu.hugegraph.manager.TasksManager;
import com.baidu.hugegraph.structure.Task;
import com.baidu.hugegraph.structure.constant.GraphMode;
import com.baidu.hugegraph.structure.gremlin.Result;
import com.baidu.hugegraph.structure.gremlin.ResultSet;
import com.baidu.hugegraph.util.E;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

import static com.baidu.hugegraph.manager.BackupManager.BACKUP_DEFAULT_TIMEOUT;

public class HugeGraphCommand {

    private SubCommands subCommands;

    @ParametersDelegate
    private SubCommands.Url url = new SubCommands.Url();

    @ParametersDelegate
    private SubCommands.Graph graph = new SubCommands.Graph();

    @ParametersDelegate
    private SubCommands.Username username = new SubCommands.Username();

    @ParametersDelegate
    private SubCommands.Password password = new SubCommands.Password();

    @ParametersDelegate
    private SubCommands.Timeout timeout = new SubCommands.Timeout();

    public HugeGraphCommand() {
        this.subCommands = new SubCommands();
    }

    public Map<String, Object> subCommands() {
        return this.subCommands.commands();
    }

    @SuppressWarnings("unchecked")
    public <T> T subCommand(String subCmd) {
        return (T) this.subCommands.commands().get(subCmd);
    }

    public String url() {
        return this.url.url;
    }

    private void url(String url) {
        this.url.url = url;
    }

    public String graph() {
        return this.graph.graph;
    }

    private void graph(String graph) {
        this.graph.graph = graph;
    }

    public String username() {
        return this.username.username;
    }

    public String password() {
        return this.password.password;
    }

    public int timeout() {
        return this.timeout.timeout;
    }

    public void timeout(int timeout) {
        this.timeout.timeout = timeout;
    }

    public JCommander jCommander() {
        JCommander.Builder builder = JCommander.newBuilder();

        // Add main command firstly
        builder.addObject(this);

        // Add sub-commands
        for (Map.Entry<String, Object> entry : this.subCommands().entrySet()) {
            builder.addCommand(entry.getKey(), entry.getValue());
        }

        JCommander jCommander = builder.build();
        jCommander.setProgramName("hugegraph");
        jCommander.setCaseSensitiveOptions(true);
        jCommander.setAllowAbbreviatedOptions(true);
        return jCommander;
    }

    private void execute(String subCmd, JCommander jCommander) {
        this.checkMainParams();
        switch (subCmd) {
            case "backup":
                if (this.timeout() < BACKUP_DEFAULT_TIMEOUT) {
                    this.timeout(BACKUP_DEFAULT_TIMEOUT);
                }
                Printer.print("Graph '%s' start backup!", this.graph());
                SubCommands.Backup backup = this.subCommand(subCmd);
                BackupManager backupManager = manager(BackupManager.class);

                backupManager.init(backup);
                backupManager.backup(backup.types());
                break;
            case "restore":
                GraphsManager graphsManager = manager(GraphsManager.class);
                GraphMode mode = graphsManager.mode(this.graph());
                E.checkState(mode.maintaining(),
                             "Invalid mode '%s' of graph '%s' for restore " +
                             "sub-command", mode, this.graph());
                Printer.print("Graph '%s' start restore in mode '%s'!",
                              this.graph(), mode);
                SubCommands.Restore restore = this.subCommand(subCmd);
                RestoreManager restoreManager = manager(RestoreManager.class);

                restoreManager.init(restore);
                restoreManager.mode(mode);
                restoreManager.restore(restore.types());
                break;
            case "migrate":
                SubCommands.Migrate migrate = this.subCommand(subCmd);
                Printer.print("Migrate graph '%s' from '%s' to '%s' as '%s'",
                              migrate.sourceGraph(), migrate.sourceUrl(),
                              migrate.targetUrl(), migrate.targetGraph());

                // Backup source graph
                if (this.timeout() < BACKUP_DEFAULT_TIMEOUT) {
                    this.timeout(BACKUP_DEFAULT_TIMEOUT);
                }
                this.url(migrate.sourceUrl());
                this.graph(migrate.sourceGraph());
                backup = convMigrate2Backup(migrate);
                backupManager = manager(BackupManager.class);
                backupManager.init(backup);
                backupManager.backup(backup.types());

                // Restore source graph to target graph
                this.url(migrate.targetUrl());
                this.graph(migrate.targetGraph());
                graphsManager = manager(GraphsManager.class);
                GraphMode origin = graphsManager.mode(this.graph());
                // Set target graph mode
                mode = migrate.mode();
                E.checkState(mode.maintaining(),
                             "Invalid mode '%s' of graph '%s' for restore",
                             mode, migrate.targetGraph());
                graphsManager.mode(migrate.targetGraph(), mode);
                // Restore
                Printer.print("Graph '%s' start restore in mode '%s'!",
                              migrate.targetGraph(), migrate.mode());
                String directory = backupManager.directory().directory();
                restore = convMigrate2Restore(migrate, directory);
                restoreManager = manager(RestoreManager.class);
                restoreManager.init(restore);
                restoreManager.mode(mode);

                restoreManager.restore(restore.types());
                // Restore target graph mode
                graphsManager.mode(migrate.targetGraph(), origin);
                break;
            case "dump":
                Printer.print("Graph '%s' start dump!", this.graph());
                SubCommands.DumpGraph dump = this.subCommand(subCmd);
                DumpGraphManager dumpManager = manager(DumpGraphManager.class);

                dumpManager.init(dump);
                dumpManager.dumpFormatter(dump.formatter());
                dumpManager.retry(dump.retry());
                dumpManager.dump(dump.directory());
                break;
            case "graph-list":
                graphsManager = manager(GraphsManager.class);
                Printer.printList("Graphs", graphsManager.list());
                break;
            case "graph-get":
                graphsManager = manager(GraphsManager.class);
                Printer.printMap("Graph info",
                                 graphsManager.get(this.graph()));
                break;
            case "graph-clear":
                SubCommands.GraphClear graphClear = this.subCommand(subCmd);
                graphsManager = manager(GraphsManager.class);
                graphsManager.clear(this.graph(), graphClear.confirmMessage());
                Printer.print("Graph '%s' is cleared", this.graph());
                break;
            case "graph-mode-set":
                SubCommands.GraphModeSet graphModeSet = this.subCommand(subCmd);
                graphsManager = manager(GraphsManager.class);
                graphsManager.mode(this.graph(), graphModeSet.mode());
                Printer.print("Set graph '%s' mode to '%s'",
                              this.graph(), graphModeSet.mode());
                break;
            case "graph-mode-get":
                graphsManager = manager(GraphsManager.class);
                Printer.printKV("Graph mode", graphsManager.mode(this.graph()));
                break;
            case "gremlin-execute":
                SubCommands.Gremlin gremlin = this.subCommand(subCmd);
                GremlinManager gremlinManager = manager(GremlinManager.class);
                Printer.print("Run gremlin script");
                ResultSet result = gremlinManager.execute(gremlin.script(),
                                                          gremlin.bindings(),
                                                          gremlin.language(),
                                                          gremlin.aliases());
                Iterator<Result> iterator = result.iterator();
                while (iterator.hasNext()) {
                    Printer.print(iterator.next().getString());
                }
                break;
            case "gremlin-schedule":
                SubCommands.GremlinJob job = this.subCommand(subCmd);
                gremlinManager = manager(GremlinManager.class);
                Printer.print("Run gremlin script as async job");
                long taskId = gremlinManager.executeAsTask(job.script(),
                                                           job.bindings(),
                                                           job.language());
                Printer.printKV("Task id", taskId);
                break;
            case "task-list":
                SubCommands.TaskList taskList = this.subCommand(subCmd);
                TasksManager tasksManager = manager(TasksManager.class);
                List<Task> tasks = tasksManager.list(taskList.status(),
                                                     taskList.limit());
                List<Object> results = tasks.stream().map(Task::asMap)
                                            .collect(Collectors.toList());
                Printer.printList("Tasks", results);
                break;
            case "task-get":
                SubCommands.TaskGet taskGet = this.subCommand(subCmd);
                tasksManager = manager(TasksManager.class);
                Printer.printKV("Task info",
                                tasksManager.get(taskGet.taskId()).asMap());
                break;
            case "task-delete":
                SubCommands.TaskDelete taskDelete = this.subCommand(subCmd);
                tasksManager = manager(TasksManager.class);
                tasksManager.delete(taskDelete.taskId());
                Printer.print("Task '%s' is deleted", taskDelete.taskId());
                break;
            case "task-cancel":
                SubCommands.TaskCancel taskCancel = this.subCommand(subCmd);
                tasksManager = manager(TasksManager.class);
                tasksManager.cancel(taskCancel.taskId());
                Printer.print("Task '%s' is cancelled", taskCancel.taskId());
                break;
            case "task-clear":
                SubCommands.TaskClear taskClear = this.subCommand(subCmd);
                tasksManager = manager(TasksManager.class);
                tasksManager.clear(taskClear.force());
                Printer.print("Tasks are cleared[force=%s]",
                              taskClear.force());
                break;
            case "help":
                jCommander.usage();
                break;
            default:
                throw new ParameterException(String.format(
                          "Invalid sub-command: %s", subCmd));
        }
    }

    private void checkMainParams() {
        E.checkArgument(this.url() != null, "Url can't be null");
        E.checkArgument(this.username() == null && this.password() == null ||
                        this.username() != null && this.password() != null,
                        "Both user name and password must be null or " +
                        "not null at same time");
    }

    private <T extends ToolManager> T manager(Class<T> clz) {
        try {
            ConnectionInfo info = new ConnectionInfo(this.url(), this.graph(),
                                                     this.username(),
                                                     this.password(),
                                                     this.timeout());
            return clz.getConstructor(ToolClient.ConnectionInfo.class)
                      .newInstance(info);
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                      "Construct manager failed for class '%s'", clz), e);
        }
    }

    private static SubCommands.Backup convMigrate2Backup(
                                      SubCommands.Migrate migrate) {
        SubCommands.Backup backup = new SubCommands.Backup();
        backup.splitSize(migrate.splitSize());
        backup.directory(migrate.directory());
        backup.logDir(migrate.logDir());
        backup.types(migrate.types());
        backup.retry(migrate.retry());
        backup.hdfsConf(migrate.hdfsConf());
        return backup;
    }

    private static SubCommands.Restore convMigrate2Restore(
                                       SubCommands.Migrate migrate,
                                       String directory) {
        SubCommands.Restore restore = new SubCommands.Restore();
        restore.remove(migrate.remove());
        restore.directory(directory);
        restore.logDir(migrate.logDir());
        restore.types(migrate.types());
        restore.retry(migrate.retry());
        restore.hdfsConf(migrate.hdfsConf());
        return restore;
    }

    private GraphMode mode() {
        GraphsManager graphsManager = manager(GraphsManager.class);
        GraphMode mode = graphsManager.mode(this.graph());
        E.checkState(mode.maintaining(),
                     "Invalid mode '%s' of graph '%s' for restore " +
                     "sub-command", mode, this.graph());
        return mode;
    }

    public static void main(String[] args) {
        HugeGraphCommand cmd = new HugeGraphCommand();
        JCommander jCommander = cmd.jCommander();

        if (args.length == 0) {
            jCommander.usage();
            System.exit(-1);
        }
        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            Printer.print(e.getMessage());
            System.exit(-1);
        }

        String subCommand = jCommander.getParsedCommand();
        if (subCommand == null) {
            Printer.print("Must provide one sub-command");
            jCommander.usage();
            System.exit(-1);
        }

        cmd.execute(subCommand, jCommander);
        System.exit(0);
    }
}
