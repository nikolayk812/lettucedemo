package com.lettuce.demo;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SomeKey {
    private final int a;
    private final String b;

    public String toDataKey() {
        return "data:" + a + "," + b;
    }

    public String toExpirationKey() {
        return "exp:" + a + "," + b;
    }

    public static boolean isExpirationKeyCandidate(String key) {
        return key.startsWith("exp");
    }

    public static SomeKey fromExpirationCandidate(String key) {
        int index = key.indexOf(':');
        String[] parts = key.substring(index + 1).split(",");
        return new SomeKey(Integer.valueOf(parts[0]), parts[1]);
    }

}
