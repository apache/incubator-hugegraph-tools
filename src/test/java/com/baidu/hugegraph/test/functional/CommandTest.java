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

import org.junit.Test;

import com.baidu.hugegraph.cmd.HugeGraphCommand;
import com.baidu.hugegraph.exception.ExitException;
import com.baidu.hugegraph.testutil.Assert;

public class CommandTest extends AuthTest {

    @Test
    public void testHelpCommand() {
        String[] args = new String[]{
                "--throw-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "help"
        };

        Assert.assertThrows(ExitException.class, () -> {
            HugeGraphCommand.main(args);
        }, e -> {
            Assert.assertContains("Command : hugegragh help",
                                  e.getMessage());
        });
    }

    @Test
    public void testHelpSubCommand() {
        String[] args = new String[]{
                "--throw-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "help", "auth-backup"
        };

        Assert.assertThrows(ExitException.class, () -> {
            HugeGraphCommand.main(args);
        }, e -> {
            Assert.assertContains("Command : hugegragh help auth-backup",
                                  e.getMessage());
        });
    }

    @Test
    public void testBadHelpSubCommandException() {
        String badCommand = "asd";
        String[] args = new String[]{
                "--throw-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD,
                "help", badCommand
        };

        Assert.assertThrows(ExitException.class, () -> {
            HugeGraphCommand.main(args);
        }, e -> {
            Assert.assertContains(String.format(
                                  "Unexpected help sub-command %s",
                                  badCommand), e.getMessage());
        });
    }

    @Test
    public void testEmptyCommandException() {
        String[] args = new String[]{
                "--throw-mode", "true",
                "--user", USER_NAME,
                "--password", USER_PASSWORD
        };

        Assert.assertThrows(ExitException.class, () -> {
            HugeGraphCommand.main(args);
        }, e -> {
            Assert.assertContains("No sub-command found",
                                  e.getMessage());
        });
    }
}
