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

package com.baidu.hugegraph.manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.base.HdfsDirectory;
import com.baidu.hugegraph.base.LocalDirectory;
import com.baidu.hugegraph.base.Printer;
import com.baidu.hugegraph.base.ToolClient;
import com.baidu.hugegraph.cmd.SubCommands;
import com.baidu.hugegraph.constant.AuthRestoreFlow;
import com.baidu.hugegraph.constant.AuthRestoreStrategy;
import com.baidu.hugegraph.exception.ToolsException;
import com.baidu.hugegraph.structure.auth.*;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AuthRestoreManager extends BackupRestoreBaseManager {

    private static final String AUTH_BACKUP_NAME = "auth-backup";
    private static final String AUTH_RESTORE_DIR = "auth-restore";

    private int conflict_status = 0;
    private AuthRestoreStrategy strategy;
    private String initPassword;
    /*
     * The collection of id relationships of users, groups and targets
     * is the basic data of belong and accesses。
     */
    private Map<String, String> idsMap;
    private Map<String, User> usersByName;
    private Map<String, Group> groupsByName;
    private Map<String, Target> targetsByName;
    private Map<String, Belong> belongsByName;
    private Map<String, Access> accessesByName;


    public AuthRestoreManager(ToolClient.ConnectionInfo info) {
        super(info, AUTH_RESTORE_DIR);
    }

    public void init(SubCommands.AuthRestore authRestore) {
        this.retry(authRestore.retry());
        this.directory(authRestore.directory(), authRestore.hdfsConf());
        this.ensureDirectoryExist(false);
        this.initStrategy(authRestore.strategy());
        this.initPassword(authRestore.types(), authRestore.initPassword());
        this.idsMap = Maps.newHashMap();
        this.usersByName = Maps.newHashMap();
        this.groupsByName = Maps.newHashMap();
        this.targetsByName = Maps.newHashMap();
        this.belongsByName = Maps.newHashMap();
        this.accessesByName = Maps.newHashMap();
    }

    public void authRestore(List<HugeType> types) {
        List<HugeType> sortedHugeTypes = this.sortListByCode(types);
        this.restore(sortedHugeTypes, AuthRestoreFlow.CHECK.code());
        this.restore(sortedHugeTypes, AuthRestoreFlow.RESTORE.code());
    }

    public void restore(List<HugeType> types, int status) {
        for (HugeType type : types) {
            switch (type) {
                case USER:
                    if (status == AuthRestoreFlow.CHECK.code()) {
                        this.checkUseConflict();
                    } else {
                        this.restoreUsers();
                    }
                    break;
                case GROUP:
                    if (status == AuthRestoreFlow.CHECK.code()) {
                        this.checkGroupsConflict();
                    } else {
                        this.restoreGroups();
                    }
                    break;
                case TARGET:
                    if (status == AuthRestoreFlow.CHECK.code()) {
                        this.checkTargetsConflict();
                    } else {
                        this.restoreTargets();
                    }
                    break;
                case BELONG:
                    if (status == AuthRestoreFlow.CHECK.code()) {
                        this.checkBelongsConflict();
                    } else {
                        this.restoreBelongs();
                    }
                    break;
                case ACCESS:
                    if (status == AuthRestoreFlow.CHECK.code()) {
                        this.checkAccessesConflict();
                    } else {
                        this.restoreAccesses();
                    }
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Bad restore type: %s", type));
            }
        }
        if (status == AuthRestoreFlow.RESTORE.code()) {
            this.shutdown(this.type());
        }
    }

    protected void checkUseConflict() {
        List<User> users = retry(this.client.authManager()::listUsers,
                                 "Querying users of authority");
        List<String> userJsons = this.read(HugeType.USER);
        Map<String, User> userMap = Maps.newHashMap();
        for (User user : users) {
             userMap.put(user.name(), user);
        }
        for (String userStr : userJsons) {
            int conflict = this.conflict_status;
            User restoreUser = JsonUtil.fromJson(userStr, User.class);
            if (userMap.containsKey(restoreUser.name())) {
                User resourceUser = userMap.get(restoreUser.name());
                if (resourceUser.phone() != null ?
                    !resourceUser.phone().equals(restoreUser.phone()) :
                    restoreUser.phone() != null) {
                    conflict++;
                }
                if (resourceUser.email() != null ?
                    !resourceUser.email().equals(restoreUser.email()) :
                    restoreUser.email() != null) {
                    conflict++;
                }
                if (resourceUser.avatar() != null ?
                    !resourceUser.avatar().equals(restoreUser.avatar()) :
                    restoreUser.avatar() != null) {
                    conflict++;
                }
                if (conflict > this.conflict_status) {
                    E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                    "Restore users conflict with stop strategy, " +
                                    "user name is s%", restoreUser.name());
                    E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                    this.strategy == AuthRestoreStrategy.IGNORE,
                                    "Restore users strategy is not fund");
                } else {
                    this.idsMap.put(restoreUser.id().toString(),
                                    resourceUser.id().toString());
                }
                continue;
            }
            this.prepareUserRestore(restoreUser);
        }
    }

    protected void checkGroupsConflict() {
        List<Group> groups = retry(this.client.authManager()::listGroups,
                                   "Querying users of authority");
        List<String> groupJsons = this.read(HugeType.GROUP);
        Map<String, Group> groupMap = Maps.newHashMap();
        for (Group group : groups) {
             groupMap.put(group.name(), group);
        }
        for (String groupStr : groupJsons) {
            int conflict = this.conflict_status;
            Group restoreGroup = JsonUtil.fromJson(groupStr, Group.class);
            if (groupMap.containsKey(restoreGroup.name())) {
                Group resourceGroup = groupMap.get(restoreGroup.name());
                if (resourceGroup.description() != null ?
                    !resourceGroup.description().equals(restoreGroup.description()) :
                    restoreGroup.description() != null) {
                    conflict++;
                }
                if (conflict > this.conflict_status) {
                    E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                    "Restore groups conflict with stop strategy, " +
                                    "group name is s%", restoreGroup.name());
                    E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                    this.strategy == AuthRestoreStrategy.IGNORE,
                                    "Restore groups strategy is not fund");
                } else {
                    this.idsMap.put(restoreGroup.id().toString(),
                                    resourceGroup.id().toString());
                }
                continue;
            }
            this.prepareGroupRestore(restoreGroup);
        }
    }

    protected void checkTargetsConflict() {
        List<Target> targets = retry(this.client.authManager()::listTargets,
                                     "Querying targets of authority");
        List<String> targetJsons = this.read(HugeType.TARGET);
        Map<String, Target> targetMap = Maps.newHashMap();
        for (Target target : targets) {
             targetMap.put(target.name(), target);
        }
        for (String targetStr : targetJsons) {
            int conflict = this.conflict_status;
            Target restoreTarget = JsonUtil.fromJson(targetStr, Target.class);
            if (targetMap.containsKey(restoreTarget.name())) {
                Target resourceTarget = targetMap.get(restoreTarget.name());
                if (resourceTarget.graph() != null ?
                    !resourceTarget.graph().equals(restoreTarget.graph()) :
                    restoreTarget.graph() != null) {
                    conflict++;
                }
                if (resourceTarget.url() != null ?
                    !resourceTarget.url().equals(restoreTarget.url()) :
                    restoreTarget.url() != null) {
                    conflict++;
                }
                if (conflict > this.conflict_status) {
                    E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                    "Restore targets conflict with stop strategy, " +
                                    "target name is s%", restoreTarget.name());
                    E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                    this.strategy == AuthRestoreStrategy.IGNORE,
                                    "Restore targets strategy is not fund");
                } else {
                    this.idsMap.put(restoreTarget.id().toString(),
                            resourceTarget.id().toString());
                }
                continue;
            }
            this.prepareTargetForRestore(restoreTarget);
        }
    }

    protected void checkBelongsConflict() {
        List<Belong> belongs = retry(this.client.authManager()::listBelongs,
                                     "Querying belongs of authority");
        List<String> belongJsons = this.read(HugeType.BELONG);
        Map<String, Belong>  belongMap = Maps.newHashMap();
        for (Belong belong : belongs) {
             belongMap.put(belong.user() + ":" + belong.group(),
                           belong);
        }
        for (String str : belongJsons) {
            Belong restoreBelong = JsonUtil.fromJson(str, Belong.class);
            if (checkIdMaps(restoreBelong.user().toString(),
                restoreBelong.group().toString())) {
                continue;
            }
            String ids = this.idsMap.get(restoreBelong.user()) + ":" +
                         this.idsMap.get(restoreBelong.group());
            if (belongMap.containsKey(ids)) {
                E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                "Restore belongs conflict with stop strategy, " +
                                "belong id is s%", restoreBelong.id());
                E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                this.strategy == AuthRestoreStrategy.IGNORE,
                                "Restore belongs strategy is not fund");
                continue;
            }
            this.belongsByName.put(restoreBelong.id().toString(), restoreBelong);
        }
    }

    protected void checkAccessesConflict() {
        List<Access> accesses = retry(this.client.authManager()::listAccesses,
                                      "Querying accesses of authority");
        List<String> accessJsons = this.read(HugeType.ACCESS);
        Map<String, Access>  accessMap = Maps.newHashMap();
        for (Access access : accesses) {
             accessMap.put(access.group() + ":" + access.target(),
                    access);
        }
        for (String str : accessJsons) {
            Access restoreAccess = JsonUtil.fromJson(str, Access.class);
            if (checkIdMaps(restoreAccess.group().toString(),
                restoreAccess.target().toString())) {
                continue;
            }
            String ids = this.idsMap.get(restoreAccess.group()) + ":" +
                         this.idsMap.get(restoreAccess.target());
            if (accessMap.containsKey(ids)) {
                E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                "Restore accesses conflict with stop strategy," +
                                "accesses id is s%", restoreAccess.id());
                E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                this.strategy == AuthRestoreStrategy.IGNORE,
                                "Restore accesses strategy is not fund");
                continue;
            }
            this.accessesByName.put(restoreAccess.id().toString(), restoreAccess);
        }
    }

    protected void restoreAccesses() {
        int counts = 0;
        for (Map.Entry<String, Access> entry : accessesByName.entrySet()) {
             Access restoreAccess = entry.getValue();
             restoreAccess.target(idsMap.get(restoreAccess.target().toString()));
             restoreAccess.group(idsMap.get(restoreAccess.group().toString()));
             retry(() -> {
                       return this.client.authManager().createAccess(restoreAccess);
                       }, "Restore access of authority");
             counts++;
            }
        Printer.print("Restore accesses finished, counts is %s !", counts);
    }

    protected void restoreBelongs() {
        int counts = 0;
        for (Map.Entry<String, Belong> entry : belongsByName.entrySet()) {
             Belong restoreBelong = entry.getValue();
             restoreBelong.user(idsMap.get(restoreBelong.user().toString()));
             restoreBelong.group(idsMap.get(restoreBelong.group().toString()));
             retry(() -> {
                       return this.client.authManager().createBelong(restoreBelong);
                       }, "Restore targets of authority");
             counts++;
            }
        Printer.print("Restore belongs finished, counts is %s !", counts);
    }

    protected void restoreTargets() {
        int counts = 0;
        for (Map.Entry<String, Target> entry: this.targetsByName.entrySet()) {
             Target restoreTarget = entry.getValue();
             Target target = retry(() -> {
                                       return this.client.authManager().createTarget(restoreTarget);
                                       }, "Restore targets of authority");
             idsMap.put(restoreTarget.id().toString(), target.id().toString());
             counts++;
           }
        Printer.print("Restore targets finished, counts is %s !", counts);
    }

    protected void restoreGroups() {
        int counts = 0;
        for (Map.Entry<String, Group> entry: this.groupsByName.entrySet()) {
             Group restoreGroup = entry.getValue();
             Group group = retry(() -> {
                                     return this.client.authManager().createGroup(restoreGroup);
                                     }, "Restore groups of authority");
             idsMap.put(restoreGroup.id().toString(), group.id().toString());
             counts++;
        }
        Printer.print("Restore groups finished, counts is %s !", counts);
    }

    protected void restoreUsers() {
        int counts = 0;
        for (Map.Entry<String, User> entry: this.usersByName.entrySet()) {
             User restoreUser = entry.getValue();
             restoreUser.password(this.initPassword);
             User user = retry(() -> {
                                   return this.client.authManager().createUser(restoreUser);
                                   }, "Restore users of authority");
             idsMap.put(restoreUser.id().toString(), user.id().toString());
             counts++;
            }
        Printer.print("Restore users finished, counts is %s !", counts);
    }

    protected void prepareTargetForRestore(Target restoreTarget) {
        this.idsMap.put(restoreTarget.id().toString(), restoreTarget.id().toString());
        this.targetsByName.put(restoreTarget.name(), restoreTarget);
    }

    protected void prepareGroupRestore(Group restoreGroup) {
        this.idsMap.put(restoreGroup.id().toString(), restoreGroup.id().toString());
        this.groupsByName.put(restoreGroup.name(), restoreGroup);
    }

    protected void prepareUserRestore(User restoreUser) {
        this.idsMap.put(restoreUser.id().toString(), restoreUser.id().toString());
        this.usersByName.put(restoreUser.name(), restoreUser);
    }

    private boolean checkIdMaps(String oneId, String otherId) {
        if (!idsMap.containsKey(oneId) ||
            !idsMap.containsKey(otherId)) {
            return true;
        }
        return false;
    }

    protected List<String> read(HugeType type) {
        List<String> resultList = Lists.newArrayList();
        InputStream is = this.inputStream(type.string());
        try (InputStreamReader isr = new InputStreamReader(is, API.CHARSET);
             BufferedReader reader = new BufferedReader(isr)) {
             String line;
             while ((line = reader.readLine()) != null) {
                 resultList.add(line);
             }
        } catch (IOException e) {
            throw new ToolsException("Failed to deserialize %s from %s",
                                     e, type, type.string());
        }
        return resultList;
    }

    protected void directory(String dir, Map<String, String> hdfsConf) {
        if (hdfsConf == null || hdfsConf.isEmpty()) {
            // Local FS directory
            super.directory = LocalDirectory.constructDir(dir, AUTH_BACKUP_NAME);
        } else {
            // HDFS directory
            super.directory = HdfsDirectory.constructDir(dir, AUTH_BACKUP_NAME,
                    hdfsConf);
        }
    }

    private void initStrategy(String strategy) {
        if (!StringUtils.isEmpty(strategy)) {
            this.strategy = AuthRestoreStrategy.getEnumByName(strategy);
        } else {
            throw new ParameterException(String.format(
                      "Bad restore strategy: %s", strategy));
        }
    }

    public List<HugeType> sortListByCode(List<HugeType> hugeTypes) {
        return hugeTypes.stream().
               sorted(Comparator.comparing(HugeType::code)).
               collect(Collectors.toList());
    }

    public void initPassword(List<HugeType> types, String password) {
        if (types.contains(HugeType.USER) && Strings.isEmpty(password)) {
            throw new ParameterException(String.format(
                      "The following option is required: [--init-password]"));
        } else {
            this.initPassword = password;
        }
    }
}
