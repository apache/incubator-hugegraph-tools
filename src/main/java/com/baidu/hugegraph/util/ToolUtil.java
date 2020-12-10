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

package com.baidu.hugegraph.util;

import java.util.Map;
import java.util.Scanner;

import com.baidu.hugegraph.base.Printer;
import com.baidu.hugegraph.constant.Constants;
import com.baidu.hugegraph.exception.ExitException;
import com.beust.jcommander.JCommander;

public final class ToolUtil {

    public static void printException(Throwable e,
                                      boolean testMode) {
        Printer.print("Failed to execute %s", e.getMessage());
        if (testMode) {
            if (e instanceof RuntimeException) {
                throw  (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
        printExceptionStackIfNeeded(e);
    }

    public static void printExceptionStackIfNeeded(Throwable e) {
        System.out.println("Type y(yes) to print exception stack[default n]?");
        Scanner scan = new Scanner(System.in);
        String inputInfomation = scan.nextLine();

        if (inputInfomation.equalsIgnoreCase(Constants.INPUT_YES) ||
            inputInfomation.equalsIgnoreCase(Constants.INPUT_Y)) {
            e.printStackTrace();
        }
    }

    public static void exitWithUsageOrThrow(ExitException e,
                                            int code,
                                            boolean testMode) {
        if (testMode) {
            throw e;
        } else {
            Printer.print(e.details());
            System.exit(code);
        }
    }

    public static String commandsCategoryDetails(JCommander jCommander) {
        StringBuffer sb = new StringBuffer();
        sb.append("================================================");
        sb.append("\n");
        sb.append("Warning : must provide one sub-command");
        sb.append("\n");
        sb.append("================================================");
        sb.append("\n");
        sb.append("Here are some sub-command :");
        sb.append("\n");
        Map<String, JCommander> subCommandeMap = jCommander.getCommands();
        for (String subCommand : subCommandeMap.keySet()) {
            sb.append("||");
            sb.append(subCommand);
            sb.append("\n");
        }
        sb.append("================================================");
        sb.append("\n");
        sb.append("Can use 'hugegraph help' to get detail help info " +
                  "of all sub-commands or 'hugegraph help sub-command' " +
                  "to get detail help info of one sub-command");
        sb.append("\n");
        sb.append("================================================");

        return sb.toString();
    }

    public static String commandUsage(JCommander jCommander) {
        StringBuilder sb = new StringBuilder();
        jCommander.usage(sb);

        return sb.toString();
    }
}
