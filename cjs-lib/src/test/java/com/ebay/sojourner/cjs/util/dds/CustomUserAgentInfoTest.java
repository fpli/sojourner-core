package com.ebay.sojourner.cjs.util.dds;

import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class CustomUserAgentInfoTest {

    @Test
    void testEquals() {
        Objects.equals(
                new CustomUserAgentInfo("ebayUserAgent/eBayAndroid;6.151.1;Android;13;Google;coral;Xfinity Mobile;1440x2872;3.5;"),
                new CustomUserAgentInfo("ebayUserAgent/eBayAndroid;6.151.1;Android;13;Google;coral;Xfinity Mobile;1440x2872;3.5;")
        );
    }

    @Test
    void testHashCode() {
        Objects.equals(
                new CustomUserAgentInfo("ebayUserAgent/eBayAndroid;6.151.1;Android;13;Google;coral;Xfinity Mobile;1440x2872;3.5;").hashCode(),
                new CustomUserAgentInfo("ebayUserAgent/eBayAndroid;6.151.1;Android;13;Google;coral;Xfinity Mobile;1440x2872;3.5;").hashCode()
        );
    }

    @Test
    void testToString() {
        Objects.equals(
                new CustomUserAgentInfo("ebayUserAgent/eBayAndroid;6.151.1;Android;13;Google;coral;Xfinity Mobile;1440x2872;3.5;").toString(),
                new CustomUserAgentInfo("ebayUserAgent/eBayAndroid;6.151.1;Android;13;Google;coral;Xfinity Mobile;1440x2872;3.5;").toString()
        );
    }
}