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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.base.LocalDirectory;
import com.baidu.hugegraph.base.Printer;
import com.baidu.hugegraph.base.ToolClient;
import com.baidu.hugegraph.formatter.Formatter;
import com.baidu.hugegraph.structure.JsonGraph;
import com.baidu.hugegraph.structure.JsonGraph.JsonVertex;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;

public class DumpGraphManager extends BackupManager {

    private static final byte[] EOF = "\n".getBytes();

    private final JsonGraph graph;

    private Formatter dumpFormatter;

    public DumpGraphManager(ToolClient.ConnectionInfo info) {
        this(info, "JsonFormatter");
    }

    public DumpGraphManager(ToolClient.ConnectionInfo info, String formatter) {
        super(info);
        this.graph = new JsonGraph();
        this.dumpFormatter = Formatter.loadFormatter(formatter);
    }

    public void dumpFormatter(String formatter) {
        this.dumpFormatter = Formatter.loadFormatter(formatter);
    }

    public void dump(String outputDir) {
        LocalDirectory.ensureDirectoryExist(outputDir);
        this.startTimer();

        // Fetch data to JsonGraph
        this.backupVertices();
        this.backupEdges();

        // Dump to file
        for (String table : this.graph.tables()) {
            File file = Paths.get(outputDir, table).toFile();
            this.submit(() -> dump(file, this.graph.table(table).values()));
        }

        this.shutdown(this.type());
        this.printSummary("dump graph");
    }

    private void dump(File file, Collection<JsonVertex> vertices) {
        try (FileOutputStream fos = new FileOutputStream(file);
             BufferedOutputStream bos = new BufferedOutputStream(fos);) {
            for (JsonVertex vertex : vertices) {
                String content = this.dumpFormatter.dump(vertex);
                bos.write(content.getBytes(API.CHARSET));
                bos.write(EOF);
            }
        } catch (Throwable e) {
            Printer.print("Failed to write vertex: %s", e);
        }
    }

    @Override
    protected long write(String file, HugeType type, List<?> list) {
        switch (type) {
            case VERTEX:
                for (Object vertex : list) {
                    this.graph.put((Vertex) vertex);
                }
                break;
            case EDGE:
                for (Object edge : list) {
                    this.graph.put((Edge) edge);
                }
                break;
            default:
                throw new AssertionError("Invalid type " + type);
        }
        return list.size();
    }
}
