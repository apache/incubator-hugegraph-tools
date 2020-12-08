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

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.cmd.HugeGraphCommand;
import com.baidu.hugegraph.testutil.Assert;

public class AuthBackupTest extends AuthTest {

    @Before
    public void init() {
        FileUtil.clearFile(DEFAULT_URL);
    }

    @Test
    public void testAuthBackup() {
        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-backup"
        };

        HugeGraphCommand.main(args);

        Assert.assertTrue(FileUtil.checkFileExists(DEFAULT_URL));
        List<String> fileNames = FileUtil.getFileDirectoryNames(DEFAULT_URL);
        Assert.assertTrue(fileNames.size() == 5);
    }

    @Test
    public void testAuthBackupByTypes() {
        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-backup",
                "--types", "user,group"
        };

        HugeGraphCommand.main(args);

        Assert.assertTrue(FileUtil.checkFileExists(DEFAULT_URL));
        List<String> fileNames = FileUtil.getFileDirectoryNames(DEFAULT_URL);
        Assert.assertTrue(fileNames.size() == 2);
    }

    @Test
    public void testAuthBackupByTypesWithException() {
        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-backup",
                "--types", "user,group,test"
        };

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphCommand.main(args);
        }, (e) -> {
            String msg = e.getMessage();
            Assert.assertTrue(msg.startsWith("com.beust.jcommander.ParameterException"));
            Assert.assertContains("valid value is 'all' or combination of " +
                                  "'user,group,target,belong,access'",
                                  msg);
        });
    }

    @Test
    public void testAuthBackupByDirectory() {
        String directory = "./backup";
        String[] args = new String[]{
                "--test-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "auth-backup",
                "--directory", directory
        };

        HugeGraphCommand.main(args);

        Assert.assertTrue(FileUtil.checkFileExists(directory));
        List<String> fileNames = FileUtil.getFileDirectoryNames(directory);
        Assert.assertTrue(fileNames.size() == 5);
    }
}
