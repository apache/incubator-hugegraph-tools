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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.baidu.hugegraph.exception.ToolsException;
import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.util.E;

public class HdfsDirectory extends Directory {

    public static final String HDFS_PREFIX = "hdfs://";

    private Map<String, String> conf;

    public HdfsDirectory(String directory, Map<String, String> conf) {
        super(directory);
        this.conf = conf;
    }

    private FileSystem fileSystem() {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : this.conf.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new ClientException("Failed to access HDFS with " +
                                      "configuration %s", this.conf, e);
        }
        return fs;
    }

    @Override
    public List<String> files() {
        List<String> files = new ArrayList<>();
        FileSystem fs = this.fileSystem();
        FileStatus[] statuses;
        try {
            statuses = fs.listStatus(new Path(this.directory()));
        } catch (IOException e) {
            throw new ToolsException("Failed to get file list in directory " +
                                     "'%s'", e, this.directory());
        }
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                files.add(status.getPath().getName());
            }
        }
        return files;
    }

    @Override
    public void ensureDirectoryExist(boolean create) {
        FileSystem fs = this.fileSystem();
        Path path = new Path(this.directory());
        try {
            if (fs.exists(path)) {
                E.checkState(fs.getFileStatus(path).isDirectory(),
                             "Can't use directory '%s' because " +
                             "a file with same name exists.", this.directory());
            } else {
                if (create) {
                    E.checkState(fs.mkdirs(path),
                                 "Directory '%s' not exists and created " +
                                 "failed", path.toString());
                } else {
                    throw new ToolsException("Directory '%s' not exists",
                                             path.toString());
                }
            }
        } catch (IOException e) {
            throw new ToolsException("Invalid directory '%s'",
                                     e, this.directory());
        }
    }

    @Override
    public String suffix() {
        return ".zip";
    }

    @Override
    public InputStream inputStream(String file) {
        String path = this.path(file);
        FileSystem fs = this.fileSystem();
        FSDataInputStream is;
        ZipInputStream zis;
        Path source = new Path(path);
        try {
            is = fs.open(source);
            zis = new ZipInputStream(is);
            E.checkState(zis.getNextEntry() != null,
                         "Invalid zip file '%s'", file);
        } catch (IOException e) {
            throw new ClientException("Failed to read from %s", e, path);
        }
        return zis;
    }

    @Override
    public OutputStream outputStream(String file, boolean override) {
        String path = this.path(file);
        FileSystem fs = this.fileSystem();
        FSDataOutputStream os;
        ZipOutputStream zos;
        Path dest = new Path(path);
        try {
            if (override) {
                os = fs.create(dest, true);
            } else {
                os = fs.append(dest);
            }
            zos = new ZipOutputStream(os);
            ZipEntry entry = new ZipEntry(file);
            zos.putNextEntry(entry);
        } catch (IOException e) {
            throw new ClientException("Failed to write to %s", e, path);
        }
        return zos;
    }

    private String path(String file) {
        if (this.directory().endsWith(Path.SEPARATOR)) {
            return this.directory() + file;
        } else {
            return this.directory() + Path.SEPARATOR + file;
        }
    }
}
