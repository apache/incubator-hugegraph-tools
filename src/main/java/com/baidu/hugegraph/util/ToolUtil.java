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

import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.base.Printer;
import com.baidu.hugegraph.base.ToolManager;
import com.baidu.hugegraph.constant.Constants;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public final class ToolUtil {

    public static void shutdown(List<ToolManager> taskManagers) {
        if (CollectionUtils.isEmpty(taskManagers)) {
            return;
        }
        for (ToolManager toolManager : taskManagers) {
            toolManager.close();
        }
    }

    public static void printException(Throwable e,
                                      boolean testMode) {
        Printer.print("Failed to execute %s", e.getMessage());
        if (testMode) {
            throw new RuntimeException(e);
        }
        printExceptionStackByUserChoise(e);
    }

    public static void printExceptionStackByUserChoise(Throwable e) {
        System.out.println("Type y(yes) to print exception stack[default n]?");
        Scanner scan = new Scanner(System.in);
        String inputInfomation = scan.nextLine();

        if (inputInfomation.equalsIgnoreCase(Constants.INPUT_YES) ||
            inputInfomation.equalsIgnoreCase(Constants.INPUT_Y)) {
            e.printStackTrace();
        }
    }

    public static void exitWithUsageOrThrow(JCommander commander,
                                            int code,
                                            boolean testMode) {
        if (testMode) {
            throw new ParameterException("Failed to parse command");
        }
        commander.usage();
        System.exit(code);
    }

    public static void exitOrThrow(int code,
                                   boolean testMode) {
        if (testMode) {
            throw new ParameterException("Failed to parse command");
        }
        System.exit(code);
    }

    public static RuntimeException targetRuntimeException(Throwable t) {
        Throwable e;
        if (t instanceof UndeclaredThrowableException) {
            e = ((UndeclaredThrowableException) t).getUndeclaredThrowable()
                                                  .getCause();
        } else {
            e = t;
        }
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new RuntimeException(e);
    }

    public static void printCommandsCategory(JCommander jCommander) {
        Printer.print("================================================");
        Printer.print("Warning : must provide one sub-command");
        Printer.print("================================================");
        Printer.print("Here are some sub-command :");
        Map<String, JCommander> subCommandeMap = jCommander.getCommands();
        for (String subCommand : subCommandeMap.keySet()) {
             Printer.print("||" + subCommand);
        }
        Printer.print("=================================================");
        Printer.print("Can use 'hugegraph help' to get detail help info " +
                      "\nof all sub-commands or 'hugegraph help sub-command' " +
                      "\nto get detail help info of one sub-command");
        Printer.print("=================================================");
    }
}
