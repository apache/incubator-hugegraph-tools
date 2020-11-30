/*
 * Copyright 2020 HugeGraph Authors
 *
 */

package com.baidu.hugegraph.constant;

public enum AuthRestoreStrategy {

    STOP(1, "stop"),
    IGNORE(2, "ignore")
    ;

    private int code;
    private String name = null;

    AuthRestoreStrategy(int code, String name) {
        assert code < 256;
        this.code = code;
        this.name = name;
    }

    public int code() {
        return this.code;
    }

    public static AuthRestoreStrategy getEnumByName(String name) {
        AuthRestoreStrategy[] restoreStrategys = AuthRestoreStrategy.values();
        for (AuthRestoreStrategy strategy : restoreStrategys) {
            if (strategy.string().equals(name)) {
                return strategy;
            }
        }
        return null;
    }

    public String string() {
        return this.name;
    }


}
