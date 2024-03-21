package com.ebay.sojourner.cjs.util;

import lombok.experimental.UtilityClass;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

@UtilityClass
public class UUIDUtils {

    public String fromString(String name) {
        if (Objects.isNull(name)) {
            return null;
        }
        return fromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    public String fromAny(Object name) {
        if (Objects.isNull(name)) {
            return null;
        }
        return fromBytes(name.toString().getBytes(StandardCharsets.UTF_8));
    }

    public String fromBytes(byte[] name) {
        if (Objects.isNull(name) || name.length == 0) {
            return null;
        }
        return UUID.nameUUIDFromBytes(name).toString();
    }
}