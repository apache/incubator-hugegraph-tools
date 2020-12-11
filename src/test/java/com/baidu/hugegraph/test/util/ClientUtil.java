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

package com.baidu.hugegraph.test.util;

import com.baidu.hugegraph.driver.HugeClient;

public class ClientUtil {

    private String url;
    private String graph;
    private String username;
    private String password;
    private Integer timeout;
    private String trustStoreFile;
    private String trustStorePassword;
    private HugeClient hugeClient;

    public ClientUtil(String url, String graph, String username,
                      String password, Integer timeout,
                      String trustStoreFile, String trustStorePassword) {
        this.url = url;
        this.graph = graph;
        this.username = username;
        this.password = password;
        this.timeout = timeout;
        this.trustStoreFile = trustStoreFile;
        this.trustStorePassword = trustStorePassword;
        this.hugeClient();
    }

    protected void hugeClient() {
        this.hugeClient = HugeClient.builder(this.url, this.graph)
                                    .configUser(this.username, this.password)
                                    .configTimeout(this.timeout)
                                    .configSSL(this.trustStoreFile, this.trustStorePassword)
                                    .build();
    }

    public HugeClient client() {
        return this.hugeClient;
    }
}
