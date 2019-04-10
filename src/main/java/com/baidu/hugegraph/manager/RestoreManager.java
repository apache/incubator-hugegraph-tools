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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.baidu.hugegraph.base.Printer;
import com.baidu.hugegraph.base.ToolClient;
import com.baidu.hugegraph.cmd.SubCommands;
import com.baidu.hugegraph.structure.constant.GraphMode;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.structure.constant.IdStrategy;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.schema.EdgeLabel;
import com.baidu.hugegraph.structure.schema.IndexLabel;
import com.baidu.hugegraph.structure.schema.PropertyKey;
import com.baidu.hugegraph.structure.schema.VertexLabel;
import com.baidu.hugegraph.util.E;

public class RestoreManager extends BackupRestoreBaseManager {

    private static final int BATCH_SIZE = 500;
    private GraphMode mode = null;

    private Map<String, Long> primaryKeyVLs = null;

    public RestoreManager(ToolClient.ConnectionInfo info) {
        super(info, "restore");
    }

    public void init(SubCommands.Restore restore) {
        super.init(restore);
        this.ensureDirectoryExist(false);
    }

    public void mode(GraphMode mode) {
        this.mode = mode;
    }

    public void restore(List<HugeType> types) {
        E.checkNotNull(this.mode, "mode");
        this.startTimer();
        for (HugeType type : types) {
            switch (type) {
                case VERTEX:
                    this.restoreVertices(type);
                    break;
                case EDGE:
                    this.restoreEdges(type);
                    break;
                case PROPERTY_KEY:
                    this.restorePropertyKeys(type);
                    break;
                case VERTEX_LABEL:
                    this.restoreVertexLabels(type);
                    break;
                case EDGE_LABEL:
                    this.restoreEdgeLabels(type);
                    break;
                case INDEX_LABEL:
                    this.restoreIndexLabels(type);
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Bad restore type: %s", type));
            }
        }
        this.shutdown(this.type());
        this.printSummary();
    }

    private void restoreVertices(HugeType type) {
        this.initPrimaryKeyVLs();
        List<String> files = this.filesWithPrefix(HugeType.VERTEX);
        printRestoreFiles(type, files);
        BiConsumer<String, String> consumer = (t, l) -> {
            List<Vertex> vertices = this.readList(t, Vertex.class, l);
            int size = vertices.size();
            for (int i = 0; i < size; i += BATCH_SIZE) {
                int toIndex = Math.min(i + BATCH_SIZE, size);
                List<Vertex> subVertices = vertices.subList(i, toIndex);
                for (Vertex vertex : subVertices) {
                    if (this.primaryKeyVLs.containsKey(vertex.label())) {
                        vertex.id(null);
                    }
                }
                this.retry(() -> this.client.graph().addVertices(subVertices),
                           "restoring vertices");
                this.vertexCounter.getAndAdd(toIndex - i);
            }
        };
        for (String file : files) {
            this.submit(() -> {
                try {
                    this.restore(type, file, consumer);
                } catch (Throwable e) {
                    Printer.print("When restoring vertices in file '%s' " +
                                  "occurs exception '%s'", file, e);
                }
            });
        }
        this.awaitTasks();
    }

    private void restoreEdges(HugeType type) {
        this.initPrimaryKeyVLs();
        List<String> files = this.filesWithPrefix(HugeType.EDGE);
        printRestoreFiles(type, files);
        BiConsumer<String, String> consumer = (t, l) -> {
            List<Edge> edges = this.readList(t, Edge.class, l);
            int size = edges.size();
            for (int i = 0; i < size; i += BATCH_SIZE) {
                int toIndex = Math.min(i + BATCH_SIZE, size);
                List<Edge> subEdges = edges.subList(i, toIndex);
                /*
                 * Edge id is concat using source and target vertex id and
                 * vertices of primary key id strategy might have changed
                 * their id
                 */
                this.updateVertexIdInEdge(subEdges);
                this.retry(() -> this.client.graph().addEdges(subEdges, false),
                           "restoring edges");
                this.edgeCounter.getAndAdd(toIndex - i);
            }
        };
        for (String file : files) {
            this.submit(() -> {
                try {
                    this.restore(type, file, consumer);
                } catch (Throwable e) {
                    Printer.print("When restoring edges in file '%s' " +
                                  "occurs exception '%s'", file, e);
                }
            });
        }
        this.awaitTasks();
    }

    private void restorePropertyKeys(HugeType type) {
        BiConsumer<String, String> consumer = (t, l) -> {
            for (PropertyKey pk : this.readList(t, PropertyKey.class, l)) {
                if (this.mode == GraphMode.MERGING) {
                    pk.resetId();
                    pk.checkExist(false);
                }
                this.client.schema().addPropertyKey(pk);
                this.propertyKeyCounter.getAndIncrement();
            }
        };
        String path = this.filesWithPrefix(HugeType.PROPERTY_KEY).get(0);
        this.restore(type, path, consumer);
    }

    private void restoreVertexLabels(HugeType type) {
        BiConsumer<String, String> consumer = (t, l) -> {
            for (VertexLabel vl : this.readList(t, VertexLabel.class, l)) {
                if (this.mode == GraphMode.MERGING) {
                    vl.resetId();
                    vl.checkExist(false);
                }
                this.client.schema().addVertexLabel(vl);
                this.vertexLabelCounter.getAndIncrement();
            }
        };
        String path = this.filesWithPrefix(HugeType.VERTEX_LABEL).get(0);
        this.restore(type, path, consumer);
    }

    private void restoreEdgeLabels(HugeType type) {
        BiConsumer<String, String> consumer = (t, l) -> {
            for (EdgeLabel el : this.readList(t, EdgeLabel.class, l)) {
                if (this.mode == GraphMode.MERGING) {
                    el.resetId();
                    el.checkExist(false);
                }
                this.client.schema().addEdgeLabel(el);
                this.edgeLabelCounter.getAndIncrement();
            }
        };
        String path = this.filesWithPrefix(HugeType.EDGE_LABEL).get(0);
        this.restore(type, path, consumer);
    }

    private void restoreIndexLabels(HugeType type) {
        BiConsumer<String, String> consumer = (t, l) -> {
            for (IndexLabel il : this.readList(t, IndexLabel.class, l)) {
                if (this.mode == GraphMode.MERGING) {
                    il.resetId();
                    il.checkExist(false);
                }
                this.client.schema().addIndexLabel(il);
                this.indexLabelCounter.getAndIncrement();
            }
        };
        String path = this.filesWithPrefix(HugeType.INDEX_LABEL).get(0);
        this.restore(type, path, consumer);
    }

    private void restore(HugeType type, String file,
                         BiConsumer<String, String> consumer) {
        this.read(file, type, consumer);
    }

    private void initPrimaryKeyVLs() {
        if (this.primaryKeyVLs != null) {
            return;
        }
        this.primaryKeyVLs = new HashMap<>();
        List<VertexLabel> vertexLabels = this.client.schema().getVertexLabels();
        for (VertexLabel vl : vertexLabels) {
            if (vl.idStrategy() == IdStrategy.PRIMARY_KEY) {
                this.primaryKeyVLs.put(vl.name(), vl.id());
            }
        }
    }

    private void updateVertexIdInEdge(List<Edge> edges) {
        for (Edge edge : edges) {
            edge.sourceId(this.updateVid(edge.sourceLabel(), edge.sourceId()));
            edge.targetId(this.updateVid(edge.targetLabel(), edge.targetId()));
        }
    }

    private Object updateVid(String label, Object id) {
        if (this.primaryKeyVLs.containsKey(label)) {
            String sid = (String) id;
            return this.primaryKeyVLs.get(label) +
                   sid.substring(sid.indexOf(':'));
        }
        return id;
    }

    private void printRestoreFiles(HugeType type, List<String> files) {
        Printer.print("Restoring %s ...", type);
        Printer.printList("  files", files);
    }
}
