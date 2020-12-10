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
import com.baidu.hugegraph.structure.auth.Access;
import com.baidu.hugegraph.structure.auth.Belong;
import com.baidu.hugegraph.structure.auth.Group;
import com.baidu.hugegraph.structure.auth.Target;
import com.baidu.hugegraph.structure.auth.User;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AuthRestoreManager extends BackupRestoreBaseManager {

    private static final String AUTH_BACKUP_NAME = "auth-backup";
    private static final String AUTH_RESTORE_DIR = "auth-restore";
    private static final int conflict_status = 0;

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
        this.strategy = AuthRestoreStrategy.fromName(authRestore.strategy());
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
        try {
            this.doAuthRestore(sortedHugeTypes, AuthRestoreFlow.CHECK);
            this.doAuthRestore(sortedHugeTypes, AuthRestoreFlow.RESTORE);
        } catch (Throwable e) {
            throw e;
        } finally {
            this.shutdown(this.type());
        }
    }

    public void doAuthRestore(List<HugeType> types,
                              AuthRestoreFlow authRestoreFlow) {
        for (HugeType type : types) {
            switch (type) {
                case USER:
                    if (authRestoreFlow == AuthRestoreFlow.CHECK) {
                        this.checkUseConflict();
                    } else {
                        this.restoreUsers();
                    }
                    break;
                case GROUP:
                    if (authRestoreFlow == AuthRestoreFlow.CHECK) {
                        this.checkGroupsConflict();
                    } else {
                        this.restoreGroups();
                    }
                    break;
                case TARGET:
                    if (authRestoreFlow == AuthRestoreFlow.CHECK) {
                        this.checkTargetsConflict();
                    } else {
                        this.restoreTargets();
                    }
                    break;
                case BELONG:
                    if (authRestoreFlow == AuthRestoreFlow.CHECK) {
                        this.checkBelongsConflict();
                    } else {
                        this.restoreBelongs();
                    }
                    break;
                case ACCESS:
                    if (authRestoreFlow == AuthRestoreFlow.CHECK) {
                        this.checkAccessesConflict();
                    } else {
                        this.restoreAccesses();
                    }
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Bad auth restore type: %s", type));
            }
        }
    }

    protected void checkUseConflict() {
        List<User> users = retry(this.client.authManager()::listUsers,
                                 "Querying users of authority");
        Map<String, User> userMap = Maps.newHashMap();
        for (User user : users) {
             userMap.put(user.name(), user);
        }
        List<String> userJsons = this.read(HugeType.USER);
        for (String user : userJsons) {
            int conflict = conflict_status;
            User restoreUser = JsonUtil.fromJson(user, User.class);
            if (!userMap.containsKey(restoreUser.name())) {
                this.prepareUserForRestore(restoreUser);
                continue;
            }
            User existUser = userMap.get(restoreUser.name());
            if (!StringUtils.equals(existUser.phone(),
                                    restoreUser.phone())) {
                conflict++;
            }
            if (!StringUtils.equals(existUser.email(),
                                    restoreUser.email())) {
                conflict++;
            }
            if (!StringUtils.equals(existUser.avatar(),
                                    restoreUser.avatar())) {
                conflict++;
            }
            if (conflict > conflict_status) {
                E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                "Restore users conflict with STOP strategy, " +
                                "user name is s%", restoreUser.name());
                E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                this.strategy == AuthRestoreStrategy.IGNORE,
                                "Restore users strategy is not found");
            } else {
                this.idsMap.put(restoreUser.id().toString(),
                                existUser.id().toString());
            }
        }
    }

    protected void checkGroupsConflict() {
        List<Group> groups = retry(this.client.authManager()::listGroups,
                                   "Querying users of authority");
        Map<String, Group> groupMap = Maps.newHashMap();
        for (Group group : groups) {
             groupMap.put(group.name(), group);
        }
        List<String> groupJsons = this.read(HugeType.GROUP);
        for (String group : groupJsons) {
            int conflict = conflict_status;
            Group restoreGroup = JsonUtil.fromJson(group, Group.class);
            if (!groupMap.containsKey(restoreGroup.name())) {
                this.prepareGroupForRestore(restoreGroup);
                continue;
            }
            Group existGroup = groupMap.get(restoreGroup.name());
            if (!StringUtils.equals(existGroup.description(),
                                    restoreGroup.description())) {
                conflict++;
            }
            if (conflict > conflict_status) {
                E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                "Restore groups conflict with STOP strategy, " +
                                "group name is s%", restoreGroup.name());
                E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                this.strategy == AuthRestoreStrategy.IGNORE,
                                "Restore groups strategy is not found");
            } else {
                this.idsMap.put(restoreGroup.id().toString(),
                                existGroup.id().toString());
            }
        }
    }

    protected void checkTargetsConflict() {
        List<Target> targets = retry(this.client.authManager()::listTargets,
                                     "Querying targets of authority");
        Map<String, Target> targetMap = Maps.newHashMap();
        for (Target target : targets) {
             targetMap.put(target.name(), target);
        }
        List<String> targetJsons = this.read(HugeType.TARGET);
        for (String target : targetJsons) {
            int conflict = conflict_status;
            Target restoreTarget = JsonUtil.fromJson(target, Target.class);
            if (!targetMap.containsKey(restoreTarget.name())) {
                this.prepareTargetForRestore(restoreTarget);
                continue;
            }
            Target existTarget = targetMap.get(restoreTarget.name());
            if (!StringUtils.equals(existTarget.graph(),
                                    restoreTarget.graph())) {
                conflict++;
            }
            if (!StringUtils.equals(existTarget.url(),
                                    restoreTarget.url())) {
                conflict++;
            }
            if (conflict > conflict_status) {
                E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                "Restore targets conflict with STOP strategy, " +
                                "target name is s%", restoreTarget.name());
                E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                this.strategy == AuthRestoreStrategy.IGNORE,
                                "Restore targets strategy is not found");
            } else {
                this.idsMap.put(restoreTarget.id().toString(),
                                existTarget.id().toString());
            }
        }
    }

    protected void checkBelongsConflict() {
        List<Belong> belongs = retry(this.client.authManager()::listBelongs,
                                     "Querying belongs of authority");
        Map<String, Belong>  belongMap = Maps.newHashMap();
        for (Belong belong : belongs) {
             belongMap.put(belong.user() + ":" + belong.group(),
                           belong);
        }
        List<String> belongJsons = this.read(HugeType.BELONG);
        for (String belong : belongJsons) {
            Belong restoreBelong = JsonUtil.fromJson(belong, Belong.class);
            if (!checkAllExistInIdMaps(restoreBelong.user().toString(),
                restoreBelong.group().toString())) {
                continue;
            }
            String ids = this.idsMap.get(restoreBelong.user()) + ":" +
                         this.idsMap.get(restoreBelong.group());
            if (belongMap.containsKey(ids)) {
                E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                "Restore belongs conflict with STOP strategy, " +
                                "belong id is s%", restoreBelong.id());
                E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                this.strategy == AuthRestoreStrategy.IGNORE,
                                "Restore belongs strategy is not found");
                continue;
            }
            this.belongsByName.put(restoreBelong.id().toString(), restoreBelong);
        }
    }

    protected void checkAccessesConflict() {
        List<Access> accesses = retry(this.client.authManager()::listAccesses,
                                      "Querying accesses of authority");
        Map<String, Access>  accessMap = Maps.newHashMap();
        for (Access access : accesses) {
             accessMap.put(access.group() + ":" + access.target(),
                           access);
        }
        List<String> accessJsons = this.read(HugeType.ACCESS);
        for (String access : accessJsons) {
            Access restoreAccess = JsonUtil.fromJson(access, Access.class);
            if (!checkAllExistInIdMaps(restoreAccess.group().toString(),
                restoreAccess.target().toString())) {
                continue;
            }
            String ids = this.idsMap.get(restoreAccess.group()) + ":" +
                         this.idsMap.get(restoreAccess.target());
            if (accessMap.containsKey(ids)) {
                E.checkArgument(this.strategy != AuthRestoreStrategy.STOP,
                                "Restore accesses conflict with STOP strategy," +
                                "accesses id is s%", restoreAccess.id());
                E.checkArgument(this.strategy == AuthRestoreStrategy.STOP ||
                                this.strategy == AuthRestoreStrategy.IGNORE,
                                "Restore accesses strategy is not found");
                continue;
            }
            this.accessesByName.put(restoreAccess.id().toString(), restoreAccess);
        }
    }

    protected void restoreAccesses() {
        int count = 0;
        for (Map.Entry<String, Access> entry : this.accessesByName.entrySet()) {
             Access restoreAccess = entry.getValue();
             restoreAccess.target(this.idsMap.get(restoreAccess.target().toString()));
             restoreAccess.group(this.idsMap.get(restoreAccess.group().toString()));
             retry(() -> {
                          return this.client.authManager().createAccess(restoreAccess);
                         }, "Restore access of authority");
             count++;
            }
        Printer.print("Restore accesses finished, count is %d !", count);
    }

    protected void restoreBelongs() {
        int count = 0;
        for (Map.Entry<String, Belong> entry : this.belongsByName.entrySet()) {
             Belong restoreBelong = entry.getValue();
             restoreBelong.user(this.idsMap.get(restoreBelong.user().toString()));
             restoreBelong.group(this.idsMap.get(restoreBelong.group().toString()));
             retry(() -> {
                          return this.client.authManager().createBelong(restoreBelong);
                         }, "Restore belongs of authority");
             count++;
            }
        Printer.print("Restore belongs finished, count is %d !", count);
    }

    protected void restoreTargets() {
        int count = 0;
        for (Map.Entry<String, Target> entry : this.targetsByName.entrySet()) {
             Target restoreTarget = entry.getValue();
             Target target = retry(() -> {
                                          return this.client.authManager().createTarget(restoreTarget);
                                         }, "Restore targets of authority");
             this.idsMap.put(restoreTarget.id().toString(), target.id().toString());
             count++;
           }
        Printer.print("Restore targets finished, count is %d !", count);
    }

    protected void restoreGroups() {
        int count = 0;
        for (Map.Entry<String, Group> entry : this.groupsByName.entrySet()) {
             Group restoreGroup = entry.getValue();
             Group group = retry(() -> {
                                        return this.client.authManager().createGroup(restoreGroup);
                                       }, "Restore groups of authority");
             this.idsMap.put(restoreGroup.id().toString(), group.id().toString());
             count++;
        }
        Printer.print("Restore groups finished, count is %d !", count);
    }

    protected void restoreUsers() {
        int count = 0;
        for (Map.Entry<String, User> entry : this.usersByName.entrySet()) {
             User restoreUser = entry.getValue();
             restoreUser.password(this.initPassword);
             User user = retry(() -> {
                                      return this.client.authManager().createUser(restoreUser);
                                     }, "Restore users of authority");
             this.idsMap.put(restoreUser.id().toString(), user.id().toString());
             count++;
            }
        Printer.print("Restore users finished, count is %d !", count);
    }

    protected void prepareTargetForRestore(Target restoreTarget) {
        this.idsMap.put(restoreTarget.id().toString(), restoreTarget.id().toString());
        this.targetsByName.put(restoreTarget.name(), restoreTarget);
    }

    protected void prepareGroupForRestore(Group restoreGroup) {
        this.idsMap.put(restoreGroup.id().toString(), restoreGroup.id().toString());
        this.groupsByName.put(restoreGroup.name(), restoreGroup);
    }

    protected void prepareUserForRestore(User restoreUser) {
        this.idsMap.put(restoreUser.id().toString(), restoreUser.id().toString());
        this.usersByName.put(restoreUser.name(), restoreUser);
    }

    private boolean checkAllExistInIdMaps(String oneId, String otherId) {
        if (this.idsMap.containsKey(oneId) &&
            this.idsMap.containsKey(otherId)) {
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
                                     e, resultList, type.string());
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
