package com.lettuce.demo;

import lombok.Data;
import lombok.NonNull;
import org.apache.commons.codec.digest.DigestUtils;

@Data
public class LuaScript {
    private final String script;
    private final String sha1;

    public LuaScript(@NonNull String script) {
        this.script = script;
        this.sha1 = DigestUtils.sha1Hex(script);
    }

}
