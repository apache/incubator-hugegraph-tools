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

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.GremlinManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.driver.TaskManager;
import com.baidu.hugegraph.driver.TraverserManager;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ToolClient {

    private HugeClient client;
    private ObjectMapper mapper;

    public ToolClient(ConnectionInfo info) {
        if (info.username != null) {
            this.client = new HugeClient(info.url, info.graph, info.username,
                                         info.password, info.timeout);
        } else {
            this.client = new HugeClient(info.url, info.graph, info.timeout);
        }
        this.mapper = new ObjectMapper();
    }

    public TraverserManager traverser() {
        return this.client.traverser();
    }

    public GraphManager graph() {
        return this.client.graph();
    }

    public SchemaManager schema() {
        return this.client.schema();
    }

    public com.baidu.hugegraph.driver.GraphsManager graphs() {
        return this.client.graphs();
    }

    public TaskManager tasks() {
        return this.client.task();
    }

    public GremlinManager gremlin() {
        return this.client.gremlin();
    }

    public ObjectMapper mapper() {
        return this.mapper;
    }

    public static class ConnectionInfo {

        private String url;
        private String graph;
        private String username;
        private String password;
        private Integer timeout;

        public ConnectionInfo(String url, String graph,
                              String username, String password,
                              Integer timeout) {
            this.url = url;
            this.graph = graph;
            this.username = username;
            this.password = password;
            this.timeout = timeout;
        }
    }
}
