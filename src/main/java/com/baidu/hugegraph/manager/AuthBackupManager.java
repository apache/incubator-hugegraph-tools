/*
 * Copyright 2020 HugeGraph Authors
 * backup of authority
 * operation commands include:
 * auth-backup
 * --types : backup data type, the default is 'all',
 * include user,group,target,belong,access
 * --directory : backup directory, the default is './auth-backup'
 *
 */

package com.baidu.hugegraph.manager;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.base.HdfsDirectory;
import com.baidu.hugegraph.base.LocalDirectory;
import com.baidu.hugegraph.base.Printer;
import com.baidu.hugegraph.base.ToolClient;
import com.baidu.hugegraph.cmd.SubCommands;
import com.baidu.hugegraph.exception.ToolsException;
import com.baidu.hugegraph.structure.auth.*;
import com.baidu.hugegraph.structure.constant.HugeType;
import com.baidu.hugegraph.util.JsonUtil;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class AuthBackupManager extends BackupRestoreBaseManager {

    private static final String AUTH_BACKUP_NAME = "auth-backup";

    public AuthBackupManager(ToolClient.ConnectionInfo info) {
        super(info, AUTH_BACKUP_NAME);
    }

    public void init(SubCommands.AuthBackup authBackup) {
        this.retry(authBackup.retry());
        this.directory(authBackup.directory(), authBackup.hdfsConf());
        this.ensureDirectoryExist(true);
    }

    public void authBackup(List<HugeType> types) {
        for (HugeType type : types) {
            switch (type) {
                case USER:
                    this.backupUsers();
                    break;
                case GROUP:
                    this.backupGroups();
                    break;
                case TARGET:
                    this.backupTargets();
                    break;
                case BELONG:
                    this.backupBelongs();
                    break;
                case ACCESS:
                    this.backupAccesses();
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Bad backup type: %s", type));
            }
        }
        this.shutdown(this.type());
    }

    protected void backupUsers() {
        Printer.print("Users backup started");
        List<User> users = retry(this.client.authManager()::listUsers,
                                 "querying users of authority");
        long writeLines = this.writeText(HugeType.USER, users);
        Printer.print("Users backup finished, write lines: %d", writeLines);
    }

    protected void backupGroups() {
        Printer.print("Groups backup started");
        List<Group> groups = retry(this.client.authManager()::listGroups,
                                   "querying groups of authority");
        long writeLines = this.writeText(HugeType.GROUP, groups);
        Printer.print("Groups backup finished, write lines: %d", writeLines);
    }

    protected void backupTargets() {
        Printer.print("Targets backup started");
        List<Target> targets = retry(this.client.authManager()::listTargets,
                                     "querying targets of authority");
        long writeLines = this.writeText(HugeType.TARGET, targets);
        Printer.print("Targets backup finished, write lines: %d", writeLines);
    }

    protected void backupBelongs() {
        Printer.print("Belongs backup started");
        List<Belong> belongs = retry(this.client.authManager()::listBelongs,
                                     "querying belongs of authority");
        long writeLines = this.writeText(HugeType.BELONG, belongs);
        Printer.print("Belongs backup finished, write lines: %d", writeLines);
    }

    protected void backupAccesses() {
        Printer.print("Accesses backup started");
        List<Access> accesses = retry(this.client.authManager()::listAccesses,
                                      "querying accesses of authority");
        long writeLines = this.writeText(HugeType.ACCESS, accesses);
        Printer.print("Accesses backup finished, write lines: %d", writeLines);
    }

    protected long writeText(HugeType type, List<?> list) {
        long count = 0L;
        try {
            OutputStream os = this.outputStream(type.string(), false);
            ByteArrayOutputStream baos = new ByteArrayOutputStream(LBUF_SIZE);
            StringBuilder builder = new StringBuilder(LBUF_SIZE);

            for (Object e : list) {
                 count++;
                 builder.append(JsonUtil.toJson(e)).append("\n");
            }
            baos.write(builder.toString().getBytes(API.CHARSET));
            os.write(baos.toByteArray());
        } catch (Throwable e) {
            throw new ToolsException("Failed to serialize %s to %s",
                                     e, type, type.string());
        }
        return count;
    }

    protected void directory(String dir, Map<String, String> hdfsConf) {
        if (hdfsConf == null || hdfsConf.isEmpty()) {
            // Local FS directory
            super.directory = LocalDirectory.constructDir(dir, AUTH_BACKUP_NAME);
        } else {
            // HDFS directory
            super.directory = HdfsDirectory.constructDir(dir, AUTH_BACKUP_NAME,
                                                         hdfsConf);
        }
    }

}
