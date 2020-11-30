/*
 * Copyright 2020 HugeGraph Authors
 *
 */

package com.baidu.hugegraph.constant;

public enum AuthRestoreFlow {

    CHECK(1, "check"),
    RESTORE(2, "restore")
    ;

    private int code;
    private String name = null;

    AuthRestoreFlow(int code, String name) {
        assert code < 256;
        this.code = code;
        this.name = name;
    }

    public int code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
