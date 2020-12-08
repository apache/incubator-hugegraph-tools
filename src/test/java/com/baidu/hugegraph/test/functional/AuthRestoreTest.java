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

package com.baidu.hugegraph.test.functional;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.cmd.HugeGraphCommand;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.structure.auth.*;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AuthRestoreTest extends AuthTest {

    private HugeClient client;

    @Before
    public void init() {
        ClientUtil initUtil = new ClientUtil(URL, GRAPH, USER_NAME,
                                             USER_PASSWORD, TIME_OUT,
                                             TRUST_STORE_FILE, TRUST_STORE_PASSWORD);
        this.client = initUtil.hugeClient;
    }

    @Test
    public void testAuthRestoreByAll() {
        this.loadData("./auth-backup/" + HugeType.USER.string(),
                      "/auth/auth_users.txt");
        this.loadData("./auth-backup/" + HugeType.TARGET.string(),
                      "/auth/auth_targets.txt");
        this.loadData("./auth-backup/" + HugeType.GROUP.string(),
                      "/auth/auth_groups.txt");
        this.loadData("./auth-backup/" + HugeType.BELONG.string(),
                      "/auth/auth_belongs.txt");
        this.loadData("./auth-backup/" + HugeType.ACCESS.string(),
                      "/auth/auth_accesses.txt");

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--directory", DEFAULT_URL,
                "--init-password", "123456",
                "--strategy", "ignore"
        };

        HugeGraphCommand.main(args);

        List<String> idList = Lists.newArrayList();
        List<User> userList = this.client.auth().listUsers();
        Map<String, User> userMap = Maps.newHashMap();
        for (User user1 : userList) {
            userMap.put(user1.name(), user1);
        }
        Assert.assertTrue(userMap.containsKey("test_user1"));
        idList.add(userMap.get("test_user1").id().toString());

        List<Group> groups = this.client.auth().listGroups();
        Map<String, Group> groupMap = Maps.newHashMap();
        for (Group group : groups) {
             groupMap.put(group.name(), group);
        }
        Assert.assertTrue(groupMap.containsKey("test_group6"));
        idList.add(groupMap.get("test_group6").id().toString());

        List<Target> targets = this.client.auth().listTargets();
        Map<String, Target> targetMap = Maps.newHashMap();
        for (Target target : targets) {
             targetMap.put(target.name(), target);
        }
        Assert.assertTrue(targetMap.containsKey("test_target1"));
        idList.add(targetMap.get("test_target1").id().toString());

        List<Belong> belongs = this.client.auth().listBelongs();
        Assert.assertTrue(CollectionUtils.isNotEmpty(belongs));
        boolean checkUserAndGroup = false;
        for (Belong belong : belongs) {
            if (idList.contains(belong.user().toString()) &&
                idList.contains(belong.group().toString())) {
                checkUserAndGroup = true;
                break;
            }
        }
        Assert.assertTrue(checkUserAndGroup);

        List<Access> accesses = this.client.auth().listAccesses();
        Assert.assertTrue(CollectionUtils.isNotEmpty(accesses));
        boolean checkGroupAndTarget = false;
        for (Access access : accesses) {
            if (idList.contains(access.group().toString()) &&
                idList.contains(access.target().toString())) {
                checkGroupAndTarget = true;
                break;
            }
        }
        Assert.assertTrue(checkGroupAndTarget);
    }

    @Test
    public void testAuthRestoreByUser() {
        this.loadData("./auth-backup/" + HugeType.USER.string(),
                      "/auth/auth_users.txt");

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "user",
                "--directory", DEFAULT_URL,
                "--init-password", "123456"
        };

        HugeGraphCommand.main(args);

        List<User> userList = this.client.auth().listUsers();
        Map<String, User> userMap = Maps.newHashMap();
        for (User user1 : userList) {
             userMap.put(user1.name(), user1);
        }

        Assert.assertTrue(userMap.containsKey("test_user1"));
    }

    @Test
    public void testAuthRestoreWithException() {
        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "user",
                "--directory", DEFAULT_URL
        };

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphCommand.main(args);
        }, (e) -> {
            String msg = e.getMessage();
            Assert.assertTrue(msg.endsWith("The following option is required: [--init-password]"));
            Assert.assertContains("com.beust.jcommander.ParameterException: The " +
                                  "following option is required: [--init-password]",
                                  msg);
        });
    }

    @Test
    public void testAuthRestoreByStrategyConflict() {
        this.loadData("./auth-backup/" + HugeType.USER.string(),
                      "/auth/auth_users_conflict.txt");

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "user",
                "--strategy", "stop",
                "--init-password", "123456"
        };

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphCommand.main(args);
        }, (e) -> {
            String msg = e.getMessage();
            Assert.assertTrue(msg.startsWith("java.lang.IllegalArgumentException"));
            Assert.assertContains("java.lang.IllegalArgumentException: " +
                                  "Restore users conflict with stop strategy",
                                  msg);
        });
    }

    @Test
    public void testAuthRestoreByStrategyIgnore() {
        this.loadData("./auth-backup/" + HugeType.USER.string(),
                      "/auth/auth_users_conflict.txt");

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "user",
                "--strategy", "ignore",
                "--init-password", "123456"
        };

        HugeGraphCommand.main(args);

        List<User> userList = this.client.auth().listUsers();
        Map<String, User> userMap = Maps.newHashMap();
        for (User user1 : userList) {
             userMap.put(user1.name(), user1);
        }

        Assert.assertTrue(userMap.containsKey("test_user1"));
    }

    @Test
    public void testAuthRestoreByDirectoryException() {
        String filePath = "./auth-test-test";

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "user",
                "--strategy", "stop",
                "--init-password", "123456",
                "--directory", filePath
        };

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphCommand.main(args);
        }, (e) -> {
            String msg = e.getMessage();
            Assert.assertTrue(msg.startsWith("java.lang.IllegalStateException"));
            Assert.assertContains("The directory does not exist",
                                  msg);
        });
    }

    @Test
    public void testAuthRestoreByTypesWithException() {
        String filePath = "./auth-test-test";

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "user，test",
                "--strategy", "stop",
                "--init-password", "123456",
                "--directory", filePath
        };

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphCommand.main(args);
        }, (e) -> {
            String msg = e.getMessage();
            Assert.assertTrue(msg.startsWith("com.beust.jcommander.ParameterException:"));
            Assert.assertContains("valid value is 'all' or combination of " +
                                  "'user,group,target,belong,access'",
                                  msg);
        });
    }

    @Test
    public void testAuthRestoreByTypesWithBelongException() {
        String filePath = "./auth-test-test";

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "belong",
                "--strategy", "stop",
                "--init-password", "123456",
                "--directory", filePath
        };

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphCommand.main(args);
        }, (e) -> {
            String msg = e.getMessage();
            Assert.assertTrue(msg.startsWith("java.lang.IllegalArgumentException:"));
            Assert.assertContains("if type contains ‘belong’ then should " +
                                  "contains ’user’ and ‘group’.",
                                  msg);
        });
    }

    @Test
    public void testAuthRestoreByTypesWithAccessException() {
        String filePath = "./auth-test-test";

        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-restore",
                "--types", "access",
                "--strategy", "stop",
                "--init-password", "123456",
                "--directory", filePath
        };

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphCommand.main(args);
        }, (e) -> {
            String msg = e.getMessage();
            Assert.assertTrue(msg.startsWith("java.lang.IllegalArgumentException:"));
            Assert.assertContains("if type contains ‘access’ then should " +
                                  "contains ’group’ and ‘target’.",
                                  msg);
        });
    }

    private void loadData(String restoreFilePath, String dataFilePath) {
        List<String> list = FileUtil.read(FileUtil.configPath(dataFilePath));

        FileUtil.writeText(restoreFilePath, list);
    }
}
