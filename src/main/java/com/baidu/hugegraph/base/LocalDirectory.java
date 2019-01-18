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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.baidu.hugegraph.exception.ToolsException;
import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class LocalDirectory extends Directory {

    public LocalDirectory(String directory) {
        super(directory);
    }

    @Override
    public List<String> files() {
        List<String> fileList = new ArrayList<>(8);
        File dir = new File(this.directory());
        String[] files = dir.list();
        if (files == null) {
            return ImmutableList.of();
        }
        for (String f : files) {
            File file = Paths.get(dir.getAbsolutePath(), f).toFile();
            if (file.isFile()) {
                fileList.add(file.getName());
            }
        }
        return fileList;
    }

    @Override
    public void ensureDirectoryExist(boolean create) {
        File file = new File(this.directory());
        if (file.exists()) {
            E.checkState(file.isDirectory(),
                         "Can't use directory '%s' because a file with " +
                         "same name exists.", file.getAbsolutePath());
        } else {
            if (create) {
                E.checkState(file.mkdirs(),
                             "Directory '%s' not exists and created failed",
                             file.getAbsolutePath());
            } else {
                throw new ToolsException("Directory '%s' not exists",
                                         file.getAbsolutePath());
            }
        }
    }

    @Override
    public String suffix() {
        return ".zip";
    }

    @Override
    public InputStream inputStream(String file) {
        String path = Paths.get(this.directory(), file).toString();
        ZipInputStream zis;
        try {
            InputStream is = new FileInputStream(path);
            zis = new ZipInputStream(is);
            E.checkState(zis.getNextEntry() != null,
                         "Invalid zip file '%s'", file);
        } catch (IOException e) {
            throw new ClientException("Failed to read from local file: %s",
                                      e, path);
        }
        return zis;
    }

    @Override
    public OutputStream outputStream(String file, boolean override) {
        String path = Paths.get(this.directory(), file).toString();
        FileOutputStream fos;
        ZipOutputStream zos;
        try {
            fos = new FileOutputStream(path, !override);
            zos = new ZipOutputStream(fos);
            ZipEntry entry = new ZipEntry(file);
            zos.putNextEntry(entry);
        } catch (IOException e) {
            throw new ClientException("Failed to write to local file: %s",
                                      e, path);
        }
        return zos;
    }
}
