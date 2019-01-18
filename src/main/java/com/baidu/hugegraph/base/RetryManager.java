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

package com.baidu.hugegraph.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.baidu.hugegraph.exception.ToolsException;

public class RetryManager extends ToolManager {

    private static int threadsNum = Math.min(10,
            Math.max(4, Runtime.getRuntime().availableProcessors() / 2));
    private final ExecutorService pool =
            Executors.newFixedThreadPool(threadsNum);
    private final List<Future<?>> futures =
            Collections.synchronizedList(new ArrayList<>());
    private int retry = 0;

    public RetryManager(ToolClient.ConnectionInfo info, String type) {
        super(info, type);
    }

    public <R> R retry(Supplier<R> supplier, String description) {
        int retries = 0;
        R r = null;
        do {
            try {
                r = supplier.get();
            } catch (Exception e) {
                if (retries == this.retry) {
                    throw new ToolsException(
                              "Exception occurred while %s(after %s retries)",
                              e, description, this.retry);
                }
                // Ignore exception and retry
                continue;
            }
            break;
        } while (retries++ < this.retry);
        return r;
    }

    public void submit(Runnable task) {
        this.futures.add(this.pool.submit(task));
    }

    public void awaitTasks() {
        int size = 0;
        int offset;
        do {
            offset = size;
            size = this.futures.size();
            for (int i = offset; i < size; i++) {
                Future<?> future = this.futures.get(i);
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        } while (size < this.futures.size());
        this.futures.clear();
    }

    public void shutdown(String taskType) {
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(24, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new ToolsException(
                      "Exception appears in %s threads", e, taskType);
        }
    }

    public int retry() {
        return this.retry;
    }

    public void retry(int retry) {
        this.retry = retry;
    }

    public static int threadsNum() {
        return threadsNum;
    }
}
